package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
)

// CloudTier identifies the managed service tier and SLA guarantees for a cloud cluster.
type CloudTier string

const (
	CloudTierServerless CloudTier = "serverless"
	CloudTierDedicated  CloudTier = "dedicated"
	CloudTierEnterprise CloudTier = "enterprise"
)

// CloudClusterStatus represents the lifecycle state of a managed cloud cluster.
type CloudClusterStatus string

const (
	ClusterStatusProvisioning CloudClusterStatus = "provisioning"
	ClusterStatusActive       CloudClusterStatus = "active"
	ClusterStatusScaling      CloudClusterStatus = "scaling"
	ClusterStatusDegraded     CloudClusterStatus = "degraded"
	ClusterStatusFailover     CloudClusterStatus = "failover"
	ClusterStatusDeleting     CloudClusterStatus = "deleting"
)

// ReplicationMode configures cross-region write acknowledgment policy.
type ReplicationMode string

const (
	ReplicationAsync    ReplicationMode = "async"
	ReplicationSemiSync ReplicationMode = "semi_sync"
	ReplicationSync     ReplicationMode = "sync"
)

// SLAPolicy defines uptime, replication RPO/lag, failover RTO, and latency targets.
type SLAPolicy struct {
	AvailabilityTarget  float64 `json:"availability_target"`
	MaxReplicationLagMs int64   `json:"max_replication_lag_ms"`
	RPOSeconds          int64   `json:"rpo_seconds"`
	RTOSeconds          int64   `json:"rto_seconds"`
	MaxP99LatencyMs     float64 `json:"max_p99_latency_ms"`
}

// DefaultSLAPolicy returns tier-appropriate SLA targets for a managed cloud cluster.
func DefaultSLAPolicy(tier CloudTier) SLAPolicy {
	switch tier {
	case CloudTierEnterprise:
		return SLAPolicy{
			AvailabilityTarget:  99.99,
			MaxReplicationLagMs: 500,
			RPOSeconds:          5,
			RTOSeconds:          30,
			MaxP99LatencyMs:     25,
		}
	case CloudTierDedicated:
		return SLAPolicy{
			AvailabilityTarget:  99.95,
			MaxReplicationLagMs: 2000,
			RPOSeconds:          15,
			RTOSeconds:          60,
			MaxP99LatencyMs:     50,
		}
	default:
		return SLAPolicy{
			AvailabilityTarget:  99.9,
			MaxReplicationLagMs: 5000,
			RPOSeconds:          60,
			RTOSeconds:          180,
			MaxP99LatencyMs:     100,
		}
	}
}

// RegionReplica tracks a regional deployment participating in multi-region replication.
type RegionReplica struct {
	Region             string    `json:"region"`
	Endpoint           string    `json:"endpoint"`
	Role               string    `json:"role"` // "primary" or "replica"
	Healthy            bool      `json:"healthy"`
	LastAppliedLSN     uint64    `json:"last_applied_lsn"`
	LagMs              int64     `json:"lag_ms"`
	LastHeartbeat      time.Time `json:"last_heartbeat"`
	AppliedMutations   int64     `json:"applied_mutations"`
	FailedReplications int64     `json:"failed_replications"`
	LastError          string    `json:"last_error,omitempty"`
}

// ReplicationEvent captures a single replicated dataset mutation across regions.
type ReplicationEvent struct {
	ClusterID     string         `json:"cluster_id"`
	SourceRegion  string         `json:"source_region"`
	LSN           uint64         `json:"lsn,omitempty"`
	Dataset       string         `json:"dataset"`
	Op            string         `json:"op"` // "upsert" or "delete"
	ID            string         `json:"id"`
	Vector        []float32      `json:"vector,omitempty"`
	Text          string         `json:"text,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	TimestampUnix int64          `json:"timestamp_unix,omitempty"`
	Checksum      string         `json:"checksum,omitempty"`
}

// CreateCloudClusterRequest configures provisioning a managed multi-region cluster.
type CreateCloudClusterRequest struct {
	ID              string            `json:"id,omitempty"`
	Name            string            `json:"name"`
	TenantID        string            `json:"tenant_id,omitempty"`
	Tier            CloudTier         `json:"tier,omitempty"`
	PrimaryRegion   string            `json:"primary_region"`
	ReplicaRegions  map[string]string `json:"replica_regions,omitempty"` // region -> endpoint
	PrimaryEndpoint string            `json:"primary_endpoint,omitempty"`
	Nodes           int               `json:"nodes,omitempty"`
	ReplicationMode ReplicationMode   `json:"replication_mode,omitempty"`
	SLAPolicy       *SLAPolicy        `json:"sla_policy,omitempty"`
}

// CloudCluster describes a provisioned managed cloud cluster and its regional replicas.
type CloudCluster struct {
	ID               string                    `json:"id"`
	Name             string                    `json:"name"`
	TenantID         string                    `json:"tenant_id"`
	Tier             CloudTier                 `json:"tier"`
	Status           CloudClusterStatus        `json:"status"`
	PrimaryRegion    string                    `json:"primary_region"`
	Nodes            int                       `json:"nodes"`
	ReplicationMode  ReplicationMode           `json:"replication_mode"`
	ReplicationLSN   uint64                    `json:"replication_lsn"`
	SLAPolicy        SLAPolicy                 `json:"sla_policy"`
	Regions          map[string]*RegionReplica `json:"regions"`
	History          []ReplicationEvent        `json:"history,omitempty"`
	UptimeSamples    int64                     `json:"uptime_samples"`
	CompliantSamples int64                     `json:"compliant_samples"`
	LastFailoverAt   *time.Time                `json:"last_failover_at,omitempty"`
	CreatedAt        time.Time                 `json:"created_at"`
	UpdatedAt        time.Time                 `json:"updated_at"`
}

// SLAReport summarizes live compliance against the cluster's SLA policy.
type SLAReport struct {
	ClusterID            string    `json:"cluster_id"`
	Tier                 CloudTier `json:"tier"`
	TargetAvailability   float64   `json:"target_availability"`
	ObservedAvailability float64   `json:"observed_availability"`
	Compliant            bool      `json:"compliant"`
	MaxObservedLagMs     int64     `json:"max_observed_lag_ms"`
	ObservedP99Ms        float64   `json:"observed_p99_ms"`
	RPOMet               bool      `json:"rpo_met"`
	RTOMet               bool      `json:"rto_met"`
	UnhealthyRegions     []string  `json:"unhealthy_regions,omitempty"`
	BreachReasons        []string  `json:"breach_reasons,omitempty"`
	EvaluatedAt          time.Time `json:"evaluated_at"`
}

// ReplicationTransport delivers a ReplicationEvent to a target regional replica.
type ReplicationTransport interface {
	Replicate(ctx context.Context, replica *RegionReplica, ev ReplicationEvent) error
}

type httpReplicationTransport struct {
	client *http.Client
}

func (t *httpReplicationTransport) Replicate(ctx context.Context, replica *RegionReplica, ev ReplicationEvent) error {
	if replica == nil {
		return errors.New("nil replica")
	}
	ep := strings.TrimSpace(replica.Endpoint)
	if ep == "" || ep == "local" || ep == "loopback" {
		return nil
	}
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		return fmt.Errorf("unsupported replica endpoint scheme: %q", ep)
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	targetURL := strings.TrimRight(ep, "/") + "/api/v1/cloud/internal/replicate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req) // #nosec G107,G704 -- targetURL is scheme-validated admin replica endpoint
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("replica %s returned HTTP %d", replica.Region, resp.StatusCode)
	}
	return nil
}

// CloudControlPlane manages provisioned SaaS clusters, multi-region replication,
// regional failover promotion, and continuous SLA monitoring.
type CloudControlPlane struct {
	mu        sync.RWMutex
	store     *VectorStore
	clusters  map[string]*CloudCluster
	transport ReplicationTransport
}

// NewCloudControlPlane creates a managed cloud control plane attached to store s.
func NewCloudControlPlane(s *VectorStore) *CloudControlPlane {
	return &CloudControlPlane{
		store:     s,
		clusters:  make(map[string]*CloudCluster),
		transport: &httpReplicationTransport{client: &http.Client{Timeout: 5 * time.Second}},
	}
}

// SetTransport overrides the replication transport (used for custom RPCs or tests).
func (c *CloudControlPlane) SetTransport(t ReplicationTransport) {
	if t == nil {
		return
	}
	c.mu.Lock()
	c.transport = t
	c.mu.Unlock()
}

func validateCloudTier(t CloudTier) CloudTier {
	switch t {
	case CloudTierDedicated, CloudTierEnterprise, CloudTierServerless:
		return t
	default:
		return CloudTierServerless
	}
}

func validateReplicationMode(m ReplicationMode) ReplicationMode {
	switch m {
	case ReplicationSync, ReplicationSemiSync, ReplicationAsync:
		return m
	default:
		return ReplicationSemiSync
	}
}

func computeReplicationChecksum(ev ReplicationEvent) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%s|%d|%s|%s|%s|%d",
		ev.ClusterID, ev.SourceRegion, ev.LSN, ev.Dataset, ev.Op, ev.ID, len(ev.Vector))
	for _, v := range ev.Vector {
		_, _ = fmt.Fprintf(h, "|%.6g", v)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func verifyReplicationChecksum(ev ReplicationEvent) bool {
	if ev.Checksum == "" {
		return true
	}
	return ev.Checksum == computeReplicationChecksum(ev)
}

func cloneCloudCluster(cl *CloudCluster) *CloudCluster {
	if cl == nil {
		return nil
	}
	cp := *cl
	cp.Regions = make(map[string]*RegionReplica, len(cl.Regions))
	for k, v := range cl.Regions {
		rcp := *v
		cp.Regions[k] = &rcp
	}
	if len(cl.History) > 0 {
		cp.History = append([]ReplicationEvent(nil), cl.History...)
	}
	return &cp
}

func (c *CloudControlPlane) updateClusterMetricsLocked() {
	counts := make(map[[2]string]float64)
	for _, cl := range c.clusters {
		counts[[2]string{string(cl.Tier), string(cl.Status)}]++
		for region, rep := range cl.Regions {
			metrics.CloudReplicationLagSeconds.WithLabelValues(cl.ID, region).Set(float64(rep.LagMs) / 1000.0)
		}
	}
	metrics.CloudClustersActive.Reset()
	for key, val := range counts {
		metrics.CloudClustersActive.WithLabelValues(key[0], key[1]).Set(val)
	}
}

// CreateCluster provisions a managed multi-region cloud cluster with tier SLA defaults.
func (c *CloudControlPlane) CreateCluster(req CreateCloudClusterRequest) (*CloudCluster, error) {
	name := strings.TrimSpace(req.Name)
	primary := strings.TrimSpace(req.PrimaryRegion)
	if !utf8.ValidString(req.ID) || !utf8.ValidString(name) || !utf8.ValidString(primary) || !utf8.ValidString(req.TenantID) {
		return nil, errors.New("cluster fields must be valid UTF-8")
	}
	if name == "" {
		return nil, errors.New("cluster name is required")
	}
	if primary == "" {
		return nil, errors.New("primary_region is required")
	}
	tier := validateCloudTier(req.Tier)
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}
	id := strings.TrimSpace(req.ID)
	if id == "" {
		id = fmt.Sprintf("cls-%s-%d", strings.ToLower(strings.ReplaceAll(name, " ", "-")), time.Now().UnixNano()%1000000)
	}
	nodes := req.Nodes
	if nodes <= 0 {
		nodes = 1
	}
	policy := DefaultSLAPolicy(tier)
	if req.SLAPolicy != nil {
		if req.SLAPolicy.AvailabilityTarget > 0 {
			policy.AvailabilityTarget = req.SLAPolicy.AvailabilityTarget
		}
		if req.SLAPolicy.MaxReplicationLagMs > 0 {
			policy.MaxReplicationLagMs = req.SLAPolicy.MaxReplicationLagMs
		}
		if req.SLAPolicy.RPOSeconds > 0 {
			policy.RPOSeconds = req.SLAPolicy.RPOSeconds
		}
		if req.SLAPolicy.RTOSeconds > 0 {
			policy.RTOSeconds = req.SLAPolicy.RTOSeconds
		}
		if req.SLAPolicy.MaxP99LatencyMs > 0 {
			policy.MaxP99LatencyMs = req.SLAPolicy.MaxP99LatencyMs
		}
	}

	now := time.Now().UTC()
	regions := make(map[string]*RegionReplica, 1+len(req.ReplicaRegions))
	primaryEP := strings.TrimSpace(req.PrimaryEndpoint)
	if primaryEP == "" {
		primaryEP = "local"
	}
	regions[primary] = &RegionReplica{
		Region:        primary,
		Endpoint:      primaryEP,
		Role:          "primary",
		Healthy:       true,
		LastHeartbeat: now,
	}
	for reg, ep := range req.ReplicaRegions {
		reg = strings.TrimSpace(reg)
		if reg == "" || reg == primary {
			continue
		}
		if strings.TrimSpace(ep) == "" {
			ep = "local"
		}
		regions[reg] = &RegionReplica{
			Region:        reg,
			Endpoint:      strings.TrimSpace(ep),
			Role:          "replica",
			Healthy:       true,
			LastHeartbeat: now,
		}
	}

	cl := &CloudCluster{
		ID:              id,
		Name:            name,
		TenantID:        tenant,
		Tier:            tier,
		Status:          ClusterStatusActive,
		PrimaryRegion:   primary,
		Nodes:           nodes,
		ReplicationMode: validateReplicationMode(req.ReplicationMode),
		SLAPolicy:       policy,
		Regions:         regions,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.clusters[id]; exists {
		return nil, fmt.Errorf("cluster %s already exists", id)
	}
	c.clusters[id] = cl
	c.updateClusterMetricsLocked()
	return cloneCloudCluster(cl), nil
}

// GetCluster returns a deep copy of a cluster by ID.
func (c *CloudControlPlane) GetCluster(id string) (*CloudCluster, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cl, ok := c.clusters[id]
	if !ok {
		return nil, false
	}
	return cloneCloudCluster(cl), true
}

// ListClusters returns all managed clusters, optionally filtered by tenantID.
func (c *CloudControlPlane) ListClusters(tenantID string) []*CloudCluster {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*CloudCluster, 0, len(c.clusters))
	for _, cl := range c.clusters {
		if tenantID != "" && cl.TenantID != tenantID {
			continue
		}
		out = append(out, cloneCloudCluster(cl))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ScaleCluster adjusts node count and/or tier on an existing managed cluster.
func (c *CloudControlPlane) ScaleCluster(id string, nodes int, tier CloudTier) (*CloudCluster, error) {
	if nodes <= 0 && tier == "" {
		return nil, errors.New("positive nodes or tier change required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.clusters[id]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", id)
	}
	if nodes > 0 {
		cl.Nodes = nodes
	}
	if tier != "" {
		cl.Tier = validateCloudTier(tier)
		cl.SLAPolicy = DefaultSLAPolicy(cl.Tier)
	}
	cl.Status = ClusterStatusActive
	cl.UpdatedAt = time.Now().UTC()
	c.updateClusterMetricsLocked()
	return cloneCloudCluster(cl), nil
}

// AddReplicaRegion attaches a new geographic replica region to a managed cluster.
func (c *CloudControlPlane) AddReplicaRegion(id, region, endpoint string) (*CloudCluster, error) {
	region = strings.TrimSpace(region)
	if !utf8.ValidString(region) || !utf8.ValidString(endpoint) {
		return nil, errors.New("region and endpoint must be valid UTF-8")
	}
	if region == "" {
		return nil, errors.New("region is required")
	}
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "local"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.clusters[id]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", id)
	}
	if _, exists := cl.Regions[region]; exists {
		return nil, fmt.Errorf("region %s already attached to cluster %s", region, id)
	}
	cl.Regions[region] = &RegionReplica{
		Region:        region,
		Endpoint:      strings.TrimSpace(endpoint),
		Role:          "replica",
		Healthy:       true,
		LastHeartbeat: time.Now().UTC(),
	}
	cl.UpdatedAt = time.Now().UTC()
	c.updateClusterMetricsLocked()
	return cloneCloudCluster(cl), nil
}

// MarkRegionHealth updates regional health and observed replication lag.
func (c *CloudControlPlane) MarkRegionHealth(id, region string, healthy bool, lagMs int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.clusters[id]
	if !ok {
		return fmt.Errorf("cluster %s not found", id)
	}
	rep, ok := cl.Regions[region]
	if !ok {
		return fmt.Errorf("region %s not found in cluster %s", region, id)
	}
	rep.Healthy = healthy
	if lagMs >= 0 {
		rep.LagMs = lagMs
	}
	rep.LastHeartbeat = time.Now().UTC()
	c.updateClusterStatusFromRegionsLocked(cl)
	c.updateClusterMetricsLocked()
	return nil
}

func (c *CloudControlPlane) updateClusterStatusFromRegionsLocked(cl *CloudCluster) {
	unhealthy := 0
	for _, rep := range cl.Regions {
		if !rep.Healthy {
			unhealthy++
		}
	}
	if unhealthy > 0 {
		cl.Status = ClusterStatusDegraded
	} else {
		cl.Status = ClusterStatusActive
	}
}

// ReplicateMutation assigns a monotonic LSN, checksums the mutation event, applies it locally
// if appropriate, and dispatches replication across replica regions respecting sync policy.
func (c *CloudControlPlane) ReplicateMutation(ctx context.Context, clusterID string, ev ReplicationEvent) (int, error) {
	if strings.TrimSpace(ev.Dataset) == "" || strings.TrimSpace(ev.ID) == "" {
		return 0, errors.New("dataset and id are required for replication")
	}
	c.mu.Lock()
	cl, ok := c.clusters[clusterID]
	if !ok {
		c.mu.Unlock()
		return 0, fmt.Errorf("cluster %s not found", clusterID)
	}
	cl.ReplicationLSN++
	ev.ClusterID = clusterID
	if ev.SourceRegion == "" {
		ev.SourceRegion = cl.PrimaryRegion
	}
	ev.LSN = cl.ReplicationLSN
	if ev.TimestampUnix == 0 {
		ev.TimestampUnix = time.Now().Unix()
	}
	if ev.Op == "" {
		ev.Op = "upsert"
	}
	ev.Checksum = computeReplicationChecksum(ev)
	if primaryRep, exists := cl.Regions[cl.PrimaryRegion]; exists {
		primaryRep.LastAppliedLSN = ev.LSN
		primaryRep.AppliedMutations++
		primaryRep.LagMs = 0
		primaryRep.LastHeartbeat = time.Now().UTC()
	}
	cl.History = append(cl.History, ev)
	if len(cl.History) > 256 {
		cl.History = cl.History[len(cl.History)-256:]
	}
	replicas := make([]*RegionReplica, 0, len(cl.Regions))
	for _, rep := range cl.Regions {
		if rep.Region == cl.PrimaryRegion {
			continue
		}
		rcp := *rep
		replicas = append(replicas, &rcp)
	}
	mode := cl.ReplicationMode
	transport := c.transport
	c.mu.Unlock()

	if c.store != nil {
		if err := c.ApplyIncomingReplication(ctx, ev); err != nil {
			return 0, err
		}
	}
	if len(replicas) == 0 {
		return 0, nil
	}

	type repResult struct {
		region  string
		latency time.Duration
		err     error
	}
	resCh := make(chan repResult, len(replicas))
	var wg sync.WaitGroup
	for _, rep := range replicas {
		wg.Add(1)
		go func(r *RegionReplica) {
			defer wg.Done()
			start := time.Now()
			err := transport.Replicate(ctx, r, ev)
			resCh <- repResult{region: r.Region, latency: time.Since(start), err: err}
		}(rep)
	}
	wg.Wait()
	close(resCh)

	succeeded := 0
	var repErrs []string
	c.mu.Lock()
	defer c.mu.Unlock()
	cl = c.clusters[clusterID]
	for res := range resCh {
		rep := cl.Regions[res.region]
		if res.err != nil {
			if rep != nil {
				rep.FailedReplications++
				rep.LastError = res.err.Error()
				rep.Healthy = false
			}
			metrics.CloudReplicationsTotal.WithLabelValues(res.region, "error").Inc()
			repErrs = append(repErrs, fmt.Sprintf("%s: %v", res.region, res.err))
			continue
		}
		succeeded++
		if rep != nil {
			rep.Healthy = true
			rep.LastAppliedLSN = ev.LSN
			rep.AppliedMutations++
			rep.LagMs = res.latency.Milliseconds()
			rep.LastError = ""
			rep.LastHeartbeat = time.Now().UTC()
		}
		metrics.CloudReplicationsTotal.WithLabelValues(res.region, "success").Inc()
	}
	c.updateClusterStatusFromRegionsLocked(cl)
	cl.UpdatedAt = time.Now().UTC()
	c.updateClusterMetricsLocked()

	if mode == ReplicationSync && len(repErrs) > 0 {
		return succeeded, fmt.Errorf("sync replication failed on regions: %s", strings.Join(repErrs, "; "))
	}
	if mode == ReplicationSemiSync && succeeded == 0 && len(replicas) > 0 {
		return succeeded, fmt.Errorf("semi_sync quorum not met: %s", strings.Join(repErrs, "; "))
	}
	return succeeded, nil
}

// ApplyIncomingReplication verifies the SHA-256 event checksum and applies the mutation to the local VectorStore.
func (c *CloudControlPlane) ApplyIncomingReplication(ctx context.Context, ev ReplicationEvent) error {
	if !verifyReplicationChecksum(ev) {
		return errors.New("replication checksum verification failed")
	}
	if c.store == nil {
		return nil
	}
	switch strings.ToLower(ev.Op) {
	case "delete":
		return c.store.DeleteVectorByID(ctx, ev.Dataset, ev.ID)
	case "", "upsert":
		if len(ev.Vector) == 0 {
			return errors.New("upsert replication event requires vector data")
		}
		_, err := c.store.UpsertStreamEvents(ctx, ev.Dataset, []StreamVectorEvent{{
			Op:            "upsert",
			ID:            ev.ID,
			Vector:        ev.Vector,
			Text:          ev.Text,
			Metadata:      ev.Metadata,
			TimestampUnix: ev.TimestampUnix,
		}})
		return err
	default:
		return fmt.Errorf("unsupported replication op %q", ev.Op)
	}
}

// SelectReadRegion routes read requests to preferredRegion if healthy, otherwise lowest-lag healthy region.
func (c *CloudControlPlane) SelectReadRegion(clusterID, preferredRegion string) (string, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cl, ok := c.clusters[clusterID]
	if !ok {
		return "", "", fmt.Errorf("cluster %s not found", clusterID)
	}
	if preferredRegion != "" {
		if rep, exists := cl.Regions[preferredRegion]; exists && rep.Healthy {
			return rep.Region, rep.Endpoint, nil
		}
	}
	var best *RegionReplica
	for _, rep := range cl.Regions {
		if !rep.Healthy {
			continue
		}
		if best == nil || rep.LagMs < best.LagMs || (rep.LagMs == best.LagMs && rep.Role == "primary") {
			best = rep
		}
	}
	if best == nil {
		return "", "", fmt.Errorf("no healthy regions available for cluster %s", clusterID)
	}
	return best.Region, best.Endpoint, nil
}

// PromoteRegion executes a regional failover promotion to targetRegion (or auto-selects healthiest replica).
func (c *CloudControlPlane) PromoteRegion(_ context.Context, clusterID, targetRegion, _ string) (*CloudCluster, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", clusterID)
	}
	if targetRegion == "" {
		var best *RegionReplica
		for _, rep := range cl.Regions {
			if rep.Region == cl.PrimaryRegion || !rep.Healthy {
				continue
			}
			if best == nil || rep.LastAppliedLSN > best.LastAppliedLSN {
				best = rep
			}
		}
		if best == nil {
			return nil, fmt.Errorf("no healthy replica available for failover in cluster %s", clusterID)
		}
		targetRegion = best.Region
	}
	targetRep, exists := cl.Regions[targetRegion]
	if !exists {
		return nil, fmt.Errorf("target region %s not found in cluster %s", targetRegion, clusterID)
	}
	if !targetRep.Healthy {
		return nil, fmt.Errorf("cannot promote unhealthy region %s", targetRegion)
	}
	if oldPrimary, ok := cl.Regions[cl.PrimaryRegion]; ok {
		oldPrimary.Role = "replica"
	}
	targetRep.Role = "primary"
	targetRep.LagMs = 0
	now := time.Now().UTC()
	cl.PrimaryRegion = targetRegion
	cl.Status = ClusterStatusActive
	cl.LastFailoverAt = &now
	cl.UpdatedAt = now
	metrics.CloudFailoversTotal.WithLabelValues(clusterID, targetRegion).Inc()
	c.updateClusterMetricsLocked()
	return cloneCloudCluster(cl), nil
}

// EvaluateSLA evaluates regional health, replication RPO lag, failover RTO, and latency against SLAPolicy.
func (c *CloudControlPlane) EvaluateSLA(clusterID string, observedP99Ms float64) (*SLAReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cl, ok := c.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", clusterID)
	}

	var unhealthy []string
	var breaches []string
	var maxLag int64
	primaryHealthy := false

	for region, rep := range cl.Regions {
		if rep.LagMs > maxLag {
			maxLag = rep.LagMs
		}
		if !rep.Healthy {
			unhealthy = append(unhealthy, region)
		} else if region == cl.PrimaryRegion {
			primaryHealthy = true
		}
	}
	sort.Strings(unhealthy)

	rpoMet := maxLag <= cl.SLAPolicy.MaxReplicationLagMs
	rtoMet := primaryHealthy
	if !primaryHealthy {
		breaches = append(breaches, fmt.Sprintf("primary region %s unhealthy", cl.PrimaryRegion))
	}
	if !rpoMet {
		breaches = append(breaches, fmt.Sprintf("replication lag %dms exceeds SLA %dms", maxLag, cl.SLAPolicy.MaxReplicationLagMs))
	}
	if observedP99Ms > 0 && cl.SLAPolicy.MaxP99LatencyMs > 0 && observedP99Ms > cl.SLAPolicy.MaxP99LatencyMs {
		breaches = append(breaches, fmt.Sprintf("p99 latency %.2fms exceeds SLA %.2fms", observedP99Ms, cl.SLAPolicy.MaxP99LatencyMs))
	}

	cl.UptimeSamples++
	sampleCompliant := len(breaches) == 0
	if sampleCompliant {
		cl.CompliantSamples++
	} else {
		metrics.CloudSLABreachesTotal.WithLabelValues(clusterID, string(cl.Tier)).Inc()
	}
	observedAvail := (float64(cl.CompliantSamples) / float64(cl.UptimeSamples)) * 100.0
	if observedAvail < cl.SLAPolicy.AvailabilityTarget && sampleCompliant {
		breaches = append(breaches, fmt.Sprintf("rolling availability %.3f%% below target %.3f%%", observedAvail, cl.SLAPolicy.AvailabilityTarget))
	}

	return &SLAReport{
		ClusterID:            clusterID,
		Tier:                 cl.Tier,
		TargetAvailability:   cl.SLAPolicy.AvailabilityTarget,
		ObservedAvailability: observedAvail,
		Compliant:            len(breaches) == 0,
		MaxObservedLagMs:     maxLag,
		ObservedP99Ms:        observedP99Ms,
		RPOMet:               rpoMet,
		RTOMet:               rtoMet,
		UnhealthyRegions:     unhealthy,
		BreachReasons:        breaches,
		EvaluatedAt:          time.Now().UTC(),
	}, nil
}

// DeleteCluster decommissions a managed cloud cluster by ID.
func (c *CloudControlPlane) DeleteCluster(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.clusters[id]; !ok {
		return fmt.Errorf("cluster %s not found", id)
	}
	delete(c.clusters, id)
	c.updateClusterMetricsLocked()
	return nil
}

func (s *VectorStore) handleCloudControlAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	ctrl := s.GetCloudControlPlane()
	switch action.Type {
	case "CreateCloudCluster":
		var req CreateCloudClusterRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.CreateCluster(req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "GetCloudCluster":
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, ok := ctrl.GetCluster(req.ID)
		if !ok {
			return true, status.Errorf(codes.NotFound, "cluster %s not found", req.ID)
		}
		return true, sendJSONActionResult(stream, cl)

	case "ListCloudClusters":
		var req struct {
			TenantID string `json:"tenant_id"`
		}
		if len(action.Body) > 0 {
			_ = json.Unmarshal(action.Body, &req)
		}
		return true, sendJSONActionResult(stream, map[string]any{"clusters": ctrl.ListClusters(req.TenantID)})

	case "ScaleCloudCluster":
		var req struct {
			ID    string    `json:"id"`
			Nodes int       `json:"nodes"`
			Tier  CloudTier `json:"tier"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.ScaleCluster(req.ID, req.Nodes, req.Tier)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "AddReplicaRegion":
		var req struct {
			ID       string `json:"id"`
			Region   string `json:"region"`
			Endpoint string `json:"endpoint"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.AddReplicaRegion(req.ID, req.Region, req.Endpoint)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "ReplicateClusterMutation":
		var req struct {
			ClusterID string           `json:"cluster_id"`
			Event     ReplicationEvent `json:"event"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		n, err := ctrl.ReplicateMutation(stream.Context(), req.ClusterID, req.Event)
		if err != nil {
			return true, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return true, sendJSONActionResult(stream, map[string]any{"replicated_regions": n})

	case "PromoteClusterRegion":
		var req struct {
			ClusterID    string `json:"cluster_id"`
			TargetRegion string `json:"target_region"`
			Reason       string `json:"reason"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.PromoteRegion(stream.Context(), req.ClusterID, req.TargetRegion, req.Reason)
		if err != nil {
			return true, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "GetCloudSLAReport":
		var req struct {
			ClusterID     string  `json:"cluster_id"`
			ObservedP99Ms float64 `json:"observed_p99_ms"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		rep, err := ctrl.EvaluateSLA(req.ClusterID, req.ObservedP99Ms)
		if err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, sendJSONActionResult(stream, rep)

	case "DeleteCloudCluster":
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := ctrl.DeleteCluster(req.ID); err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, sendJSONActionResult(stream, map[string]any{"deleted": req.ID})

	default:
		return false, nil
	}
}

func sendJSONActionResult(stream flight.FlightService_DoActionServer, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal response: %v", err)
	}
	return stream.Send(&flight.Result{Body: raw})
}
