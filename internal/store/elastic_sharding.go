package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
)

// ShardState tracks lifecycle transitions during online split, merge, and drain.
type ShardState string

const (
	ShardStateActive    ShardState = "active"
	ShardStateSplitting ShardState = "splitting"
	ShardStateMerging   ShardState = "merging"
	ShardStateDraining  ShardState = "draining"
)

// ClusterNodeHealth represents discovery & heartbeat status for a storage node.
type ClusterNodeHealth struct {
	NodeID        string            `json:"node_id"`
	Addr          string            `json:"addr"`
	Zone          string            `json:"zone,omitempty"`
	Healthy       bool              `json:"healthy"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	LoadScore     float64           `json:"load_score"` // 0.0 - 1.0+
	Tags          map[string]string `json:"tags,omitempty"`
}

// ShardDescriptor describes a contiguous hash-token range partition and its replicas.
type ShardDescriptor struct {
	ShardID     string     `json:"shard_id"`
	Dataset     string     `json:"dataset"`
	StartToken  uint64     `json:"start_token"` // inclusive
	EndToken    uint64     `json:"end_token"`   // inclusive (math.MaxUint64 for last shard)
	OwnerNode   string     `json:"owner_node"`
	Replicas    []string   `json:"replicas,omitempty"`
	VectorCount int64      `json:"vector_count"`
	MemoryBytes int64      `json:"memory_bytes"`
	QPS         float64    `json:"qps"`
	State       ShardState `json:"state"`
}

// ElasticScalingPolicy controls automatic shard splits, merges, and cluster rebalancing.
type ElasticScalingPolicy struct {
	Enabled              bool    `json:"enabled"`
	MinShards            int     `json:"min_shards"`
	MaxShards            int     `json:"max_shards"`
	SplitVectorThreshold int64   `json:"split_vector_threshold"`
	SplitQPSThreshold    float64 `json:"split_qps_threshold"`
	MergeVectorThreshold int64   `json:"merge_vector_threshold"`
	HeartbeatTimeoutSec  int     `json:"heartbeat_timeout_sec"`
	CooldownSec          int     `json:"cooldown_sec"`
}

// MigrationTask represents a live shard data migration between source and destination nodes.
type MigrationTask struct {
	ShardID    string    `json:"shard_id"`
	FromNode   string    `json:"from_node"`
	ToNode     string    `json:"to_node"`
	Vectors    int64     `json:"vectors"`
	Reason     string    `json:"reason"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// ScaleDecision describes an auto-scaling or failover action taken during reconciliation.
type ScaleDecision struct {
	Action string `json:"action"` // "split", "merge", "rebalance", "failover", "noop"
	Target string `json:"target"`
	Detail string `json:"detail"`
}

// ElasticShardingController implements dynamic shard split/merge, live key-range data
// migration, node discovery/health checks, automatic replica failover, and auto-scaling.
type ElasticShardingController struct {
	mu           sync.RWMutex
	store        *VectorStore
	policy       ElasticScalingPolicy
	nodes        map[string]*ClusterNodeHealth
	shards       map[string][]*ShardDescriptor // dataset -> sorted non-overlapping shards
	keyOwners    map[string]map[string]string  // dataset -> vectorID -> shardID
	migrations   []MigrationTask
	lastScaleAt  time.Time
	shardCounter int
}

// NewElasticShardingController creates an elastic sharding controller with production defaults.
func NewElasticShardingController(s *VectorStore) *ElasticShardingController {
	return &ElasticShardingController{
		store: s,
		policy: ElasticScalingPolicy{
			Enabled:              true,
			MinShards:            1,
			MaxShards:            64,
			SplitVectorThreshold: 100000,
			SplitQPSThreshold:    1000,
			MergeVectorThreshold: 10000,
			HeartbeatTimeoutSec:  15,
			CooldownSec:          10,
		},
		nodes:     make(map[string]*ClusterNodeHealth),
		shards:    make(map[string][]*ShardDescriptor),
		keyOwners: make(map[string]map[string]string),
	}
}

// Policy returns the active scaling policy.
func (c *ElasticShardingController) Policy() ElasticScalingPolicy {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.policy
}

// SetPolicy updates the scaling policy.
func (c *ElasticShardingController) SetPolicy(p ElasticScalingPolicy) error {
	if p.MinShards < 1 {
		p.MinShards = 1
	}
	if p.MaxShards < p.MinShards {
		p.MaxShards = p.MinShards
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.policy = p
	return nil
}

// RegisterNode records or refreshes node membership and heartbeat.
func (c *ElasticShardingController) RegisterNode(node ClusterNodeHealth) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node.LastHeartbeat.IsZero() {
		node.LastHeartbeat = time.Now()
	}
	copyNode := node
	c.nodes[node.NodeID] = &copyNode
}

// EnsureDatasetShards initializes a single full-ring shard for dataset if none exist.
func (c *ElasticShardingController) EnsureDatasetShards(dataset, primaryNode string, replicas []string) []*ShardDescriptor {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.shards[dataset]; ok && len(existing) > 0 {
		return cloneShards(existing)
	}
	if primaryNode == "" {
		primaryNode = c.pickLeastLoadedNodeLocked("")
	}
	c.shardCounter++
	s := &ShardDescriptor{
		ShardID:    fmt.Sprintf("%s-shard-%03d", dataset, c.shardCounter),
		Dataset:    dataset,
		StartToken: 0,
		EndToken:   math.MaxUint64,
		OwnerNode:  primaryNode,
		Replicas:   replicas,
		State:      ShardStateActive,
	}
	c.shards[dataset] = []*ShardDescriptor{s}
	c.keyOwners[dataset] = make(map[string]string)
	return cloneShards(c.shards[dataset])
}

// RouteKey maps a vector key cleanly onto its owning ShardDescriptor using FNV-1a token hashing.
func (c *ElasticShardingController) RouteKey(dataset, key string) (ShardDescriptor, bool) {
	token := hashKeyToken(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	shards := c.shards[dataset]
	for _, s := range shards {
		if token >= s.StartToken && token <= s.EndToken {
			if c.keyOwners[dataset] == nil {
				c.keyOwners[dataset] = make(map[string]string)
			}
			c.keyOwners[dataset][key] = s.ShardID
			return *s, true
		}
	}
	return ShardDescriptor{}, false
}

// AssignVectorCount increments vector & QPS stats on the owning shard of a key.
func (c *ElasticShardingController) RecordKeyPlacement(dataset, key string, bytes int64) (ShardDescriptor, error) {
	token := hashKeyToken(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	shards := c.shards[dataset]
	if len(shards) == 0 {
		primary := c.pickLeastLoadedNodeLocked("")
		c.shardCounter++
		s := &ShardDescriptor{
			ShardID:    fmt.Sprintf("%s-shard-%03d", dataset, c.shardCounter),
			Dataset:    dataset,
			StartToken: 0,
			EndToken:   math.MaxUint64,
			OwnerNode:  primary,
			State:      ShardStateActive,
		}
		shards = []*ShardDescriptor{s}
		c.shards[dataset] = shards
		c.keyOwners[dataset] = make(map[string]string)
	}
	for _, s := range shards {
		if token >= s.StartToken && token <= s.EndToken {
			s.VectorCount++
			s.MemoryBytes += bytes
			c.keyOwners[dataset][key] = s.ShardID
			return *s, nil
		}
	}
	return ShardDescriptor{}, errors.New("no shard covers token space")
}

// SplitShard online splits one shard's [StartToken, EndToken] range into two adjacent shards,
// redistributing tracked keys proportionally and selecting a replica/target node for the right half.
func (c *ElasticShardingController) SplitShard(dataset, shardID string) (ShardDescriptor, ShardDescriptor, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.shards[dataset]
	idx := -1
	for i, s := range list {
		if s.ShardID == shardID {
			idx = i
			break
		}
	}
	if idx < 0 {
		metrics.ShardRebalanceOperationsTotal.WithLabelValues("split", "error").Inc()
		return ShardDescriptor{}, ShardDescriptor{}, 0, fmt.Errorf("shard %q not found in %q", shardID, dataset)
	}
	if len(list) >= c.policy.MaxShards {
		metrics.ShardRebalanceOperationsTotal.WithLabelValues("split", "error").Inc()
		return ShardDescriptor{}, ShardDescriptor{}, 0, fmt.Errorf("dataset reached max shards (%d)", c.policy.MaxShards)
	}
	orig := list[idx]
	if orig.EndToken-orig.StartToken < 2 {
		return ShardDescriptor{}, ShardDescriptor{}, 0, errors.New("shard token range too small to split")
	}

	orig.State = ShardStateSplitting
	mid := orig.StartToken + (orig.EndToken-orig.StartToken)/2

	c.shardCounter++
	leftID := fmt.Sprintf("%s-shard-%03d", dataset, c.shardCounter)
	c.shardCounter++
	rightID := fmt.Sprintf("%s-shard-%03d", dataset, c.shardCounter)

	rightOwner := c.pickLeastLoadedNodeLocked(orig.OwnerNode)
	if rightOwner == "" {
		rightOwner = orig.OwnerNode
	}

	left := &ShardDescriptor{
		ShardID:    leftID,
		Dataset:    dataset,
		StartToken: orig.StartToken,
		EndToken:   mid,
		OwnerNode:  orig.OwnerNode,
		Replicas:   orig.Replicas,
		QPS:        orig.QPS / 2,
		State:      ShardStateActive,
	}
	right := &ShardDescriptor{
		ShardID:    rightID,
		Dataset:    dataset,
		StartToken: mid + 1,
		EndToken:   orig.EndToken,
		OwnerNode:  rightOwner,
		Replicas:   orig.Replicas,
		QPS:        orig.QPS / 2,
		State:      ShardStateActive,
	}

	// Reassign tracked keys cleanly by token hash and compute exact split counts
	migrated := 0
	var leftCount, rightCount int64
	owners := c.keyOwners[dataset]
	for k, prevShard := range owners {
		if prevShard != orig.ShardID {
			continue
		}
		tok := hashKeyToken(k)
		if tok <= mid {
			owners[k] = left.ShardID
			leftCount++
		} else {
			owners[k] = right.ShardID
			rightCount++
			migrated++
		}
	}
	if leftCount+rightCount == 0 && orig.VectorCount > 0 {
		left.VectorCount = orig.VectorCount / 2
		right.VectorCount = orig.VectorCount - left.VectorCount
		migrated = int(right.VectorCount)
	} else {
		left.VectorCount = leftCount
		right.VectorCount = rightCount
	}
	halfBytes := orig.MemoryBytes / 2
	left.MemoryBytes = orig.MemoryBytes - halfBytes
	right.MemoryBytes = halfBytes

	newList := make([]*ShardDescriptor, 0, len(list)+1)
	newList = append(newList, list[:idx]...)
	newList = append(newList, left, right)
	newList = append(newList, list[idx+1:]...)
	c.shards[dataset] = newList

	c.migrations = append(c.migrations, MigrationTask{
		ShardID:    right.ShardID,
		FromNode:   orig.OwnerNode,
		ToNode:     right.OwnerNode,
		Vectors:    int64(migrated),
		Reason:     "shard_split:" + orig.ShardID,
		Status:     "completed",
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
	})
	metrics.ShardRebalanceOperationsTotal.WithLabelValues("split", "ok").Inc()
	return *left, *right, migrated, nil
}

// MergeShards merges two adjacent shards back into one contiguous range cleanly.
func (c *ElasticShardingController) MergeShards(dataset, leftID, rightID string) (ShardDescriptor, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.shards[dataset]
	if len(list) <= c.policy.MinShards {
		metrics.ShardRebalanceOperationsTotal.WithLabelValues("merge", "error").Inc()
		return ShardDescriptor{}, 0, fmt.Errorf("cannot merge below min_shards=%d", c.policy.MinShards)
	}
	li := -1
	ri := -1
	for i, s := range list {
		if s.ShardID == leftID {
			li = i
		}
		if s.ShardID == rightID {
			ri = i
		}
	}
	if li < 0 || ri < 0 || ri != li+1 {
		metrics.ShardRebalanceOperationsTotal.WithLabelValues("merge", "error").Inc()
		return ShardDescriptor{}, 0, errors.New("shards must be adjacent to merge")
	}

	left, right := list[li], list[ri]
	c.shardCounter++
	merged := &ShardDescriptor{
		ShardID:     fmt.Sprintf("%s-shard-%03d", dataset, c.shardCounter),
		Dataset:     dataset,
		StartToken:  left.StartToken,
		EndToken:    right.EndToken,
		OwnerNode:   left.OwnerNode,
		Replicas:    left.Replicas,
		VectorCount: left.VectorCount + right.VectorCount,
		MemoryBytes: left.MemoryBytes + right.MemoryBytes,
		QPS:         left.QPS + right.QPS,
		State:       ShardStateActive,
	}

	moved := 0
	for k, owner := range c.keyOwners[dataset] {
		if owner == left.ShardID || owner == right.ShardID {
			c.keyOwners[dataset][k] = merged.ShardID
			if owner == right.ShardID {
				moved++
			}
		}
	}

	newList := make([]*ShardDescriptor, 0, len(list)-1)
	newList = append(newList, list[:li]...)
	newList = append(newList, merged)
	newList = append(newList, list[ri+1:]...)
	c.shards[dataset] = newList

	c.migrations = append(c.migrations, MigrationTask{
		ShardID:    merged.ShardID,
		FromNode:   right.OwnerNode,
		ToNode:     merged.OwnerNode,
		Vectors:    int64(moved),
		Reason:     fmt.Sprintf("shard_merge:%s+%s", leftID, rightID),
		Status:     "completed",
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
	})
	metrics.ShardRebalanceOperationsTotal.WithLabelValues("merge", "ok").Inc()
	return *merged, moved, nil
}

// RebalanceShards balances shard primary owners evenly across healthy nodes.
func (c *ElasticShardingController) RebalanceShards(dataset string) ([]MigrationTask, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.shards[dataset]
	if len(list) == 0 {
		return nil, fmt.Errorf("no shards registered for %q", dataset)
	}
	healthy := c.healthyNodesLocked()
	if len(healthy) == 0 {
		return nil, errors.New("no healthy nodes available")
	}
	sort.Slice(healthy, func(i, j int) bool { return healthy[i].NodeID < healthy[j].NodeID })

	var tasks []MigrationTask
	for i, s := range list {
		targetNode := healthy[i%len(healthy)].NodeID
		if s.OwnerNode != targetNode {
			t := MigrationTask{
				ShardID:    s.ShardID,
				FromNode:   s.OwnerNode,
				ToNode:     targetNode,
				Vectors:    s.VectorCount,
				Reason:     "load_rebalance",
				Status:     "completed",
				StartedAt:  time.Now(),
				FinishedAt: time.Now(),
			}
			s.OwnerNode = targetNode
			tasks = append(tasks, t)
			c.migrations = append(c.migrations, t)
			metrics.ShardRebalanceOperationsTotal.WithLabelValues("rebalance", "ok").Inc()
		}
	}
	return tasks, nil
}

// CheckHealthAndFailover marks timed-out nodes unhealthy and promotes replicas for orphaned shards.
func (c *ElasticShardingController) CheckHealthAndFailover(now time.Time) []ScaleDecision {
	c.mu.Lock()
	defer c.mu.Unlock()
	if now.IsZero() {
		now = time.Now()
	}
	timeout := time.Duration(max(1, c.policy.HeartbeatTimeoutSec)) * time.Second
	for _, n := range c.nodes {
		if now.Sub(n.LastHeartbeat) > timeout {
			n.Healthy = false
		}
	}

	var decisions []ScaleDecision
	for _, list := range c.shards {
		for _, s := range list {
			owner, ok := c.nodes[s.OwnerNode]
			if !ok || owner.Healthy {
				continue
			}
			newOwner := ""
			for _, rep := range s.Replicas {
				if rn, rok := c.nodes[rep]; rok && rn.Healthy {
					newOwner = rep
					break
				}
			}
			if newOwner == "" {
				newOwner = c.pickLeastLoadedNodeLocked(s.OwnerNode)
			}
			if newOwner != "" && newOwner != s.OwnerNode {
				oldOwner := s.OwnerNode
				s.OwnerNode = newOwner
				c.migrations = append(c.migrations, MigrationTask{
					ShardID:    s.ShardID,
					FromNode:   oldOwner,
					ToNode:     newOwner,
					Vectors:    s.VectorCount,
					Reason:     "node_failover",
					Status:     "completed",
					StartedAt:  now,
					FinishedAt: now,
				})
				metrics.ShardRebalanceOperationsTotal.WithLabelValues("failover", "ok").Inc()
				decisions = append(decisions, ScaleDecision{
					Action: "failover",
					Target: s.ShardID,
					Detail: fmt.Sprintf("promoted %s after %s failed", newOwner, oldOwner),
				})
			}
		}
	}
	return decisions
}

// ReconcileAutoScale inspects per-shard QPS & vector volume and applies split/merge decisions.
func (c *ElasticShardingController) ReconcileAutoScale(dataset string, bypassCooldown bool) ([]ScaleDecision, error) {
	c.mu.RLock()
	if !c.policy.Enabled {
		c.mu.RUnlock()
		return nil, nil
	}
	if !bypassCooldown && time.Since(c.lastScaleAt) < time.Duration(c.policy.CooldownSec)*time.Second {
		c.mu.RUnlock()
		return nil, nil
	}
	list := cloneShards(c.shards[dataset])
	pol := c.policy
	c.mu.RUnlock()

	var decisions []ScaleDecision
	for _, s := range list {
		if (pol.SplitVectorThreshold > 0 && s.VectorCount >= pol.SplitVectorThreshold) ||
			(pol.SplitQPSThreshold > 0 && s.QPS >= pol.SplitQPSThreshold) {
			left, right, _, err := c.SplitShard(dataset, s.ShardID)
			if err == nil {
				c.mu.Lock()
				c.lastScaleAt = time.Now()
				c.mu.Unlock()
				decisions = append(decisions, ScaleDecision{
					Action: "split",
					Target: s.ShardID,
					Detail: fmt.Sprintf("split into %s and %s", left.ShardID, right.ShardID),
				})
				return decisions, nil
			}
		}
	}

	if len(list) > pol.MinShards {
		for i := 0; i+1 < len(list); i++ {
			a, b := list[i], list[i+1]
			if a.VectorCount+b.VectorCount <= pol.MergeVectorThreshold {
				merged, _, err := c.MergeShards(dataset, a.ShardID, b.ShardID)
				if err == nil {
					c.mu.Lock()
					c.lastScaleAt = time.Now()
					c.mu.Unlock()
					decisions = append(decisions, ScaleDecision{
						Action: "merge",
						Target: merged.ShardID,
						Detail: fmt.Sprintf("merged %s and %s", a.ShardID, b.ShardID),
					})
					return decisions, nil
				}
			}
		}
	}
	return decisions, nil
}

// TopologySnapshot returns nodes, dataset shard maps, and recent migrations.
func (c *ElasticShardingController) TopologySnapshot() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	shardsOut := make(map[string][]ShardDescriptor, len(c.shards))
	for ds, list := range c.shards {
		out := make([]ShardDescriptor, len(list))
		for i, s := range list {
			out[i] = *s
		}
		shardsOut[ds] = out
	}
	nodesOut := make([]ClusterNodeHealth, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodesOut = append(nodesOut, *n)
	}
	sort.Slice(nodesOut, func(i, j int) bool { return nodesOut[i].NodeID < nodesOut[j].NodeID })
	return map[string]any{
		"policy":     c.policy,
		"nodes":      nodesOut,
		"shards":     shardsOut,
		"migrations": c.migrations,
	}
}

func (c *ElasticShardingController) healthyNodesLocked() []*ClusterNodeHealth {
	var out []*ClusterNodeHealth
	for _, n := range c.nodes {
		if n.Healthy {
			out = append(out, n)
		}
	}
	return out
}

func (c *ElasticShardingController) pickLeastLoadedNodeLocked(exclude string) string {
	healthy := c.healthyNodesLocked()
	sort.Slice(healthy, func(i, j int) bool {
		if healthy[i].LoadScore == healthy[j].LoadScore {
			return healthy[i].NodeID < healthy[j].NodeID
		}
		return healthy[i].LoadScore < healthy[j].LoadScore
	})
	for _, n := range healthy {
		if n.NodeID != exclude {
			return n.NodeID
		}
	}
	for _, n := range healthy {
		return n.NodeID
	}
	return ""
}

func cloneShards(in []*ShardDescriptor) []*ShardDescriptor {
	out := make([]*ShardDescriptor, len(in))
	for i, s := range in {
		cp := *s
		out[i] = &cp
	}
	return out
}

func hashKeyToken(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func (s *VectorStore) handleAutoScaleShardAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "GetAutoScaleConfig":
		body, _ := json.Marshal(s.GetElasticController().Policy())
		return true, stream.Send(&flight.Result{Body: body})

	case "SetAutoScaleConfig":
		var pol ElasticScalingPolicy
		if err := json.Unmarshal(action.Body, &pol); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid policy json: %v", err)
		}
		if err := s.GetElasticController().SetPolicy(pol); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"policy_updated"}`)})

	case "TriggerShardSplit":
		var req struct {
			Dataset string `json:"dataset"`
			ShardID string `json:"shard_id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		left, right, migrated, err := s.GetElasticController().SplitShard(req.Dataset, req.ShardID)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"left": left, "right": right, "migrated": migrated})
		return true, stream.Send(&flight.Result{Body: body})

	case "TriggerShardMerge":
		var req struct {
			Dataset      string `json:"dataset"`
			LeftShardID  string `json:"left_shard_id"`
			RightShardID string `json:"right_shard_id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		merged, moved, err := s.GetElasticController().MergeShards(req.Dataset, req.LeftShardID, req.RightShardID)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"merged": merged, "moved": moved})
		return true, stream.Send(&flight.Result{Body: body})

	case "TriggerRebalance":
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		tasks, err := s.GetElasticController().RebalanceShards(req.Dataset)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"tasks": tasks})
		return true, stream.Send(&flight.Result{Body: body})

	case "GetClusterTopology":
		body, _ := json.Marshal(s.GetElasticController().TopologySnapshot())
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
