package sharding

import (
	"sync"

	"github.com/hashicorp/memberlist"
	"go.uber.org/zap"
)

// RingManager coordinates the consistent hash ring with the cluster membership state
type RingManager struct {
	mu          sync.RWMutex
	ring        *ConsistentHash
	localNodeID string
	logger      *zap.Logger
}

// NewRingManager creates a new RingManager
func NewRingManager(localNodeID string, logger *zap.Logger) *RingManager {
	return &RingManager{
		ring:        NewConsistentHash(20), // 20 vnodes default
		localNodeID: localNodeID,
		logger:      logger,
	}
}

// NotifyJoin is invoked when a node joins the memberlist cluster
func (rm *RingManager) NotifyJoin(node *memberlist.Node) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info("Node joined ring", zap.String("node", node.Name), zap.String("addr", node.Address()))
	rm.ring.AddNode(node.Name)
}

// NotifyLeave is invoked when a node leaves the memberlist cluster
func (rm *RingManager) NotifyLeave(node *memberlist.Node) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.logger.Info("Node left ring", zap.String("node", node.Name))
	rm.ring.RemoveNode(node.Name)
}

// NotifyUpdate is invoked when a node is updated (we ignore this for the ring usually)
func (rm *RingManager) NotifyUpdate(node *memberlist.Node) {
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
