package sharding

import (
	"sync"

	"github.com/23skdu/longbow/internal/mesh"
	"go.uber.org/zap"
)

// RingManager coordinates the consistent hash ring with the cluster membership state
type RingManager struct {
	mu          sync.RWMutex
	ring        *ConsistentHash
	localNodeID string
	logger      *zap.Logger
	nodeAddrs   map[string]string // ID -> Data Addr
	metaAddrs   map[string]string // ID -> Meta Addr
}

// NewRingManager creates a new RingManager
func NewRingManager(localNodeID string, logger *zap.Logger) *RingManager {
	return &RingManager{
		ring:        NewConsistentHash(20), // 20 vnodes default
		localNodeID: localNodeID,
		logger:      logger,
		nodeAddrs:   make(map[string]string),
		metaAddrs:   make(map[string]string),
	}
}

// NotifyJoin is invoked when a node joins the cluster
func (rm *RingManager) NotifyJoin(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info("Node joined ring", zap.String("node", member.ID), zap.String("grpc_addr", member.GRPCAddr), zap.String("meta_addr", member.MetaAddr))
	rm.ring.AddNode(member.ID)
	rm.nodeAddrs[member.ID] = member.GRPCAddr
	rm.metaAddrs[member.ID] = member.MetaAddr
}

// NotifyLeave is invoked when a node leaves the cluster
func (rm *RingManager) NotifyLeave(member *mesh.Member) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info("Node left ring", zap.String("node", member.ID))
	rm.ring.RemoveNode(member.ID)
	delete(rm.nodeAddrs, member.ID)
	delete(rm.metaAddrs, member.ID)
}

// NotifyUpdate is invoked when a node is updated
func (rm *RingManager) NotifyUpdate(member *mesh.Member) {
	// No-op for ring structure
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
