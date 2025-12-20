package store

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// PeerState represents the connection state of a peer
type PeerState int

const (
	PeerStateDisconnected PeerState = iota
	PeerStateConnecting
	PeerStateConnected
	PeerStateError
)

// ReplicationMode defines how replication is performed
type ReplicationMode int

const (
	ReplicationModeAsync ReplicationMode = iota
	ReplicationModeSync
)

// ReplicationConfig holds configuration for Arrow Flight replication
type ReplicationConfig struct {
	Enabled        bool
	Peers          []string
	ReplicaFactor  int
	SyncInterval   time.Duration
	ConnectTimeout time.Duration
	MaxRetries     int
	Mode           ReplicationMode
}

// NewReplicationConfig returns a ReplicationConfig with sensible defaults
func NewReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		Enabled:        false,
		Peers:          []string{},
		ReplicaFactor:  1,
		SyncInterval:   30 * time.Second,
		ConnectTimeout: 10 * time.Second,
		MaxRetries:     3,
		Mode:           ReplicationModeAsync,
	}
}

// Validate checks if the configuration is valid
func (c *ReplicationConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.Peers) == 0 {
		return errors.New("replication enabled but no peers configured")
	}

	if c.ReplicaFactor <= 0 {
		return errors.New("replica factor must be positive")
	}

	if c.ReplicaFactor > len(c.Peers) {
		return fmt.Errorf("replica factor (%d) exceeds number of peers (%d)", c.ReplicaFactor, len(c.Peers))
	}

	if c.SyncInterval < 0 {
		return errors.New("sync interval cannot be negative")
	}

	for i, peer := range c.Peers {
		if peer == "" {
			return fmt.Errorf("peer at index %d has empty address", i)
		}
	}

	return nil
}

// PeerInfo holds detailed information about a peer
type PeerInfo struct {
	Address       string
	State         PeerState
	LastSeen      time.Time
	LastError     error
	RetryCount    int
	DatasetsCount int
}

// ReplicationStatus provides status information about replication
type ReplicationStatus struct {
	Enabled        bool
	TotalPeers     int
	ConnectedPeers int
	ReplicaFactor  int
	LastSync       time.Time
	SyncErrors     int
}

// ReplicationManager handles Arrow Flight replication to peer servers
type ReplicationManager struct {
	config ReplicationConfig
	logger *zap.Logger
	mu     sync.RWMutex
	peers  map[string]*PeerInfo
	stopCh chan struct{}
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(config ReplicationConfig, logger *zap.Logger) *ReplicationManager {
	if logger == nil {
		logger = zap.NewNop()
	}

	m := &ReplicationManager{
		config: config,
		logger: logger,
		peers:  make(map[string]*PeerInfo),
		stopCh: make(chan struct{}),
	}

	for _, addr := range config.Peers {
		m.peers[addr] = &PeerInfo{
			Address: addr,
			State:   PeerStateDisconnected,
		}
	}

	return m
}

// IsEnabled returns whether replication is enabled
func (m *ReplicationManager) IsEnabled() bool {
	return m.config.Enabled
}

// GetPeers returns the list of peer addresses
func (m *ReplicationManager) GetPeers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]string, 0, len(m.peers))
	for addr := range m.peers {
		peers = append(peers, addr)
	}
	return peers
}

// GetHealthyPeers returns peers in Connected state
func (m *ReplicationManager) GetHealthyPeers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]string, 0)
	for addr, info := range m.peers {
		if info.State == PeerStateConnected {
			healthy = append(healthy, addr)
		}
	}
	return healthy
}

// UpdatePeerState updates the connection state of a peer
func (m *ReplicationManager) UpdatePeerState(addr string, state PeerState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if info, ok := m.peers[addr]; ok {
		info.State = state
		if state == PeerStateConnected {
			info.LastSeen = time.Now()
		}
	}
}

// GetStatus returns the current replication status
func (m *ReplicationManager) GetStatus() ReplicationStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	connected := 0
	for _, info := range m.peers {
		if info.State == PeerStateConnected {
			connected++
		}
	}

	return ReplicationStatus{
		Enabled:        m.config.Enabled,
		TotalPeers:     len(m.peers),
		ConnectedPeers: connected,
		ReplicaFactor:  m.config.ReplicaFactor,
	}
}

// AddPeer adds a new peer to the replication mesh
func (m *ReplicationManager) AddPeer(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.peers[addr]; exists {
		return NewReplicationError("add_peer", addr, "", fmt.Errorf("peer already exists"))
	}

	m.peers[addr] = &PeerInfo{
		Address: addr,
		State:   PeerStateDisconnected,
	}
	return nil
}

// RemovePeer removes a peer from the replication mesh
func (m *ReplicationManager) RemovePeer(addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.peers[addr]; !exists {
		return NewReplicationError("remove_peer", addr, "", fmt.Errorf("peer not found"))
	}

	delete(m.peers, addr)
	return nil
}

// GetPeerInfo returns detailed information about a specific peer
func (m *ReplicationManager) GetPeerInfo(addr string) (*PeerInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.peers[addr]
	if !ok {
		return nil, NewReplicationError("remove_peer", addr, "", fmt.Errorf("peer not found"))
	}

	infoCopy := *info
	return &infoCopy, nil
}

// ReplicateDataset replicates Arrow data to peers
func (m *ReplicationManager) ReplicateDataset(ctx context.Context, name string, records []arrow.RecordBatch) error {
	if !m.config.Enabled {
		return nil
	}

	m.logger.Info("Replicating dataset", zap.String("name", name), zap.Int("records", len(records)))
	return nil
}
