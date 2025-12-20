package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// =============================================================================
// PeerConfig
// =============================================================================

// PeerConfig configures peer-to-peer streaming behavior
type PeerConfig struct {
	// ReplicationMode determines how data is replicated (uses ReplicationMode from replication.go)
	Mode ReplicationMode
	// ReplicationTimeout is the maximum time to wait for replication
	ReplicationTimeout time.Duration
	// MaxReplicationRetries is the number of retry attempts for failed replication
	MaxReplicationRetries int
	// PeerFallbackEnabled enables falling back to peers when local data is unavailable
	PeerFallbackEnabled bool
	// PeerFallbackTimeout is the maximum time to wait for peer fallback
	PeerFallbackTimeout time.Duration
	// HealthCheckInterval is how often to check peer health
	HealthCheckInterval time.Duration
	// UnhealthyThreshold is number of failures before marking peer unhealthy
	UnhealthyThreshold int
}

// DefaultPeerConfig returns a PeerConfig with sensible defaults
func DefaultPeerConfig() PeerConfig {
	return PeerConfig{
		Mode:                  ReplicationModeAsync,
		ReplicationTimeout:    5 * time.Second,
		MaxReplicationRetries: 3,
		PeerFallbackEnabled:   true,
		PeerFallbackTimeout:   2 * time.Second,
		HealthCheckInterval:   30 * time.Second,
		UnhealthyThreshold:    3,
	}
}

// Validate checks if the PeerConfig is valid
func (c *PeerConfig) Validate() error {
	if c.ReplicationTimeout <= 0 {
		return errors.New("ReplicationTimeout must be positive")
	}
	if c.MaxReplicationRetries < 0 {
		return errors.New("MaxReplicationRetries cannot be negative")
	}
	if c.PeerFallbackTimeout <= 0 {
		return errors.New("PeerFallbackTimeout must be positive")
	}
	return nil
}

// =============================================================================
// peerInfo
// =============================================================================

// peerInfo tracks information about a peer
type peerInfo struct {
	Address       string
	Healthy       bool
	FailureCount  int
	LastSeen      time.Time
	LastError     error
	LastErrorTime time.Time
}

// =============================================================================
// PeerManager
// =============================================================================

// PeerManager manages peer connections and health
type PeerManager struct {
	pool   *FlightClientPool
	config PeerConfig
	mu     sync.RWMutex
	peers  map[string]*peerInfo
}

// NewPeerManager creates a new PeerManager
func NewPeerManager(pool *FlightClientPool, config PeerConfig) *PeerManager {
	return &PeerManager{
		pool:   pool,
		config: config,
		peers:  make(map[string]*peerInfo),
	}
}

// AddPeer adds a peer to the manager
func (pm *PeerManager) AddPeer(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.peers[address]; !exists {
		pm.peers[address] = &peerInfo{
			Address:  address,
			Healthy:  true,
			LastSeen: time.Now(),
		}
	}
}

// RemovePeer removes a peer from the manager
func (pm *PeerManager) RemovePeer(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, address)
}

// GetPeers returns all peer addresses
func (pm *PeerManager) GetPeers() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]string, 0, len(pm.peers))
	for addr := range pm.peers {
		peers = append(peers, addr)
	}
	return peers
}

// GetHealthyPeers returns only healthy peer addresses
func (pm *PeerManager) GetHealthyPeers() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	peers := make([]string, 0, len(pm.peers))
	for addr, info := range pm.peers {
		if info.Healthy {
			peers = append(peers, addr)
		}
	}
	return peers
}

// IsPeerHealthy checks if a peer is healthy
func (pm *PeerManager) IsPeerHealthy(address string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if info, exists := pm.peers[address]; exists {
		return info.Healthy
	}
	return false
}

// MarkPeerHealthy marks a peer as healthy
func (pm *PeerManager) MarkPeerHealthy(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if info, exists := pm.peers[address]; exists {
		info.Healthy = true
		info.FailureCount = 0
		info.LastSeen = time.Now()
	}
}

// MarkPeerUnhealthy marks a peer as unhealthy
func (pm *PeerManager) MarkPeerUnhealthy(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if info, exists := pm.peers[address]; exists {
		info.Healthy = false
	}
}

// RecordFailure records a failure for a peer
func (pm *PeerManager) RecordFailure(address string, err error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if info, exists := pm.peers[address]; exists {
		info.FailureCount++
		info.LastError = err
		info.LastErrorTime = time.Now()
		if info.FailureCount >= pm.config.UnhealthyThreshold {
			info.Healthy = false
		}
	}
}

// RecordSuccess records a successful operation for a peer
func (pm *PeerManager) RecordSuccess(address string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if info, exists := pm.peers[address]; exists {
		info.FailureCount = 0
		info.Healthy = true
		info.LastSeen = time.Now()
	}
}

// =============================================================================
// ReplicationResult
// =============================================================================

// ReplicationResult contains the result of a replication operation
type ReplicationResult struct {
	TotalPeers      int
	SuccessfulPeers int
	FailedPeers     int
	Errors          map[string]error
	Duration        time.Duration
}

// AllSucceeded returns true if all peers succeeded
func (r *ReplicationResult) AllSucceeded() bool {
	return r.FailedPeers == 0 && r.TotalPeers > 0
}

// AnyFailed returns true if any peer failed
func (r *ReplicationResult) AnyFailed() bool {
	return r.FailedPeers > 0
}

// SuccessRate returns the ratio of successful peers
func (r *ReplicationResult) SuccessRate() float64 {
	if r.TotalPeers == 0 {
		return 0
	}
	return float64(r.SuccessfulPeers) / float64(r.TotalPeers)
}

// =============================================================================
// PeerStreamingStats
// =============================================================================

// PeerStreamingStats contains statistics for peer streaming operations
type PeerStreamingStats struct {
	TotalGetRequests     int64
	TotalPutRequests     int64
	SuccessfulGets       int64
	SuccessfulPuts       int64
	FailedGets           int64
	FailedPuts           int64
	TotalRecordsSent     int64
	TotalRecordsReceived int64
}

// =============================================================================
// PeerStreamingService
// =============================================================================

// PeerStreamingService provides high-level peer-to-peer streaming operations
type PeerStreamingService struct {
	peerManager *PeerManager
	pool        *FlightClientPool
	config      PeerConfig

	// Statistics
	totalGetRequests     int64
	totalPutRequests     int64
	successfulGets       int64
	successfulPuts       int64
	failedGets           int64
	failedPuts           int64
	totalRecordsSent     int64
	totalRecordsReceived int64
}

// NewPeerStreamingService creates a new PeerStreamingService
func NewPeerStreamingService(pm *PeerManager, pool *FlightClientPool, config PeerConfig) *PeerStreamingService {
	return &PeerStreamingService{
		peerManager: pm,
		pool:        pool,
		config:      config,
	}
}

// DoGetFromPeers attempts to retrieve data from healthy peers
func (s *PeerStreamingService) DoGetFromPeers(ctx context.Context, dataset string, filters []Filter) ([]arrow.RecordBatch, error) {
	atomic.AddInt64(&s.totalGetRequests, 1)

	peers := s.peerManager.GetHealthyPeers()
	if len(peers) == 0 {
		atomic.AddInt64(&s.failedGets, 1)
		return nil, errors.New("no healthy peers available")
	}

	// Try each peer until one succeeds
	var lastErr error
	for _, peer := range peers {
		peerCtx, cancel := context.WithTimeout(ctx, s.config.PeerFallbackTimeout)
		records, err := s.pool.DoGetFromPeer(peerCtx, peer, dataset, filters)
		cancel()

		if err == nil {
			s.peerManager.RecordSuccess(peer)
			atomic.AddInt64(&s.successfulGets, 1)
			atomic.AddInt64(&s.totalRecordsReceived, int64(len(records)))
			return records, nil
		}

		s.peerManager.RecordFailure(peer, err)
		lastErr = err
	}

	atomic.AddInt64(&s.failedGets, 1)
	return nil, fmt.Errorf("all peers failed: %w", lastErr)
}

// ReplicateToPeers replicates data to all healthy peers
func (s *PeerStreamingService) ReplicateToPeers(ctx context.Context, dataset string, records []arrow.RecordBatch) *ReplicationResult {
	atomic.AddInt64(&s.totalPutRequests, 1)
	start := time.Now()

	peers := s.peerManager.GetHealthyPeers()
	result := &ReplicationResult{
		TotalPeers: len(peers),
		Errors:     make(map[string]error),
	}

	if len(peers) == 0 {
		result.Duration = time.Since(start)
		return result
	}

	// Use the existing ReplicateToPeers from FlightClientPool
	replicationCtx, cancel := context.WithTimeout(ctx, s.config.ReplicationTimeout)
	defer cancel()

	peerErrors := s.pool.ReplicateToPeers(replicationCtx, peers, dataset, records)

	for peer, err := range peerErrors {
		if err != nil {
			result.FailedPeers++
			result.Errors[peer] = err
			s.peerManager.RecordFailure(peer, err)
		} else {
			result.SuccessfulPeers++
			s.peerManager.RecordSuccess(peer)
		}
	}

	atomic.AddInt64(&s.totalRecordsSent, int64(len(records)*result.SuccessfulPeers))
	if result.FailedPeers > 0 {
		atomic.AddInt64(&s.failedPuts, 1)
	} else {
		atomic.AddInt64(&s.successfulPuts, 1)
	}

	result.Duration = time.Since(start)
	return result
}

// DoPutToPeer sends data to a specific peer
func (s *PeerStreamingService) DoPutToPeer(ctx context.Context, peer, dataset string, records []arrow.RecordBatch) error {
	peerCtx, cancel := context.WithTimeout(ctx, s.config.ReplicationTimeout)
	defer cancel()

	err := s.pool.DoPutToPeer(peerCtx, peer, dataset, records)
	if err != nil {
		s.peerManager.RecordFailure(peer, err)
		return err
	}

	s.peerManager.RecordSuccess(peer)
	return nil
}

// Stats returns current statistics
func (s *PeerStreamingService) Stats() PeerStreamingStats {
	return PeerStreamingStats{
		TotalGetRequests:     atomic.LoadInt64(&s.totalGetRequests),
		TotalPutRequests:     atomic.LoadInt64(&s.totalPutRequests),
		SuccessfulGets:       atomic.LoadInt64(&s.successfulGets),
		SuccessfulPuts:       atomic.LoadInt64(&s.successfulPuts),
		FailedGets:           atomic.LoadInt64(&s.failedGets),
		FailedPuts:           atomic.LoadInt64(&s.failedPuts),
		TotalRecordsSent:     atomic.LoadInt64(&s.totalRecordsSent),
		TotalRecordsReceived: atomic.LoadInt64(&s.totalRecordsReceived),
	}
}
