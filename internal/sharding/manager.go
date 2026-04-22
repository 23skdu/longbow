package sharding

import (
	"sync"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/rs/zerolog"
)

// RingManager coordinates the consistent hash ring with the cluster membership state
type RingManager struct {
	mu          sync.RWMutex
	ring        *ConsistentHash
	localNodeID string
	logger      zerolog.Logger
	nodeAddrs   map[string]string // ID -> Data Addr
	metaAddrs   map[string]string // ID -> Meta Addr
	nodeLoads   map[string]float64 // ID -> load ratio (0.0-1.0)
	baseVNodes  int
}

// NewRingManager creates a new RingManager
//
//nolint:gocritic // Logger passed by value for constructor simplicity
func NewRingManager(localNodeID string, logger zerolog.Logger) *RingManager {
	return &RingManager{
		ring:        NewConsistentHash(20), // 20 vnodes default
		localNodeID: localNodeID,
		logger:      logger,
		nodeAddrs:   make(map[string]string),
		metaAddrs:   make(map[string]string),
		nodeLoads:   make(map[string]float64),
		baseVNodes:  20,
	}
}

// NotifyJoin is invoked when a node joins the cluster
func (rm *RingManager) NotifyJoin(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info().
		Str("node", member.ID).
		Str("grpc_addr", member.GRPCAddr).
		Str("meta_addr", member.MetaAddr).
		Msg("Node joined ring")
	rm.ring.AddNode(member.ID)
	rm.nodeAddrs[member.ID] = member.GRPCAddr
	rm.metaAddrs[member.ID] = member.MetaAddr
}

// NotifyLeave is invoked when a node leaves the cluster
func (rm *RingManager) NotifyLeave(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info().Str("node", member.ID).Msg("Node left ring")
	rm.ring.RemoveNode(member.ID)
	delete(rm.nodeAddrs, member.ID)
	delete(rm.metaAddrs, member.ID)
}

// NotifyUpdate is invoked when a node is updated
func (rm *RingManager) NotifyUpdate(member *mesh.Member) {
	// Update addresses if they changed
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.nodeAddrs[member.ID] = member.GRPCAddr
	rm.metaAddrs[member.ID] = member.MetaAddr
}

// UpdateNodeLoad updates the reported load for a node and triggers rebalancing if necessary
func (rm *RingManager) UpdateNodeLoad(nodeID string, load float64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.nodeLoads[nodeID] = load
	rm.logger.Debug().Str("node", nodeID).Float64("load", load).Msg("Node load updated")

	// Trigger rebalancing logic (could be debounced or on a ticker, but we do it here for now)
	rm.rebalance()
}

// rebalance adjusts vnode counts based on node loads to distribute traffic more fairly
func (rm *RingManager) rebalance() {
	// Rebalance policy:
	// - If load > 0.8, decrease vnodes (reduce incoming traffic)
	// - If load < 0.4, increase vnodes (accept more traffic)
	// - Otherwise, keep at baseVNodes

	for nodeID, load := range rm.nodeLoads {
		targetVNodes := rm.baseVNodes
		if load > 0.8 {
			targetVNodes = rm.baseVNodes / 2
			if targetVNodes < 1 {
				targetVNodes = 1
			}
		} else if load < 0.4 {
			targetVNodes = rm.baseVNodes * 2
		}

		rm.ring.UpdateVNodeCount(nodeID, targetVNodes)
	}
}

// GetNode returns the owner of a key
func (rm *RingManager) GetNode(key string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.ring.GetNode(key)
}

// IsLocalKey returns true if the key belongs to the local node
func (rm *RingManager) IsLocalKey(key string) bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	owner := rm.ring.GetNode(key)
	return owner == rm.localNodeID
}

// GetPreferenceList returns the replication nodes for a key
func (rm *RingManager) GetPreferenceList(key string, n int) []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.ring.GetPreferenceList(key, n)
}

// GetNodeAddr returns the data service network address for a given node ID
func (rm *RingManager) GetNodeAddr(nodeID string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.nodeAddrs[nodeID]
}

// GetMetaAddr returns the metadata/search service network address for a given node ID
func (rm *RingManager) GetMetaAddr(nodeID string) string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.metaAddrs[nodeID]
}

// GetMembers returns a list of all known node IDs in the ring
func (rm *RingManager) GetMembers() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	members := make([]string, 0, len(rm.nodeAddrs))
	for id := range rm.nodeAddrs {
		members = append(members, id)
	}
	return members
}
