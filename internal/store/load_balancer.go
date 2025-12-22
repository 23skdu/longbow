package store

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// Load Balancer Strategy Types
// =============================================================================

type StrategyType string

const (
	StrategyRoundRobin       StrategyType = "round_robin"
	StrategyLeastConnections StrategyType = "least_connections"
	StrategyWeighted         StrategyType = "weighted"
)

// LoadBalancerStrategy defines the interface for selection strategies
type LoadBalancerStrategy interface {
	Select(replicas []string) string
}

// =============================================================================
// Round Robin Strategy
// =============================================================================

// RoundRobinStrategy cycles through replicas in order
type RoundRobinStrategy struct {
	counter uint64
}

// NewRoundRobinStrategy creates a new round robin strategy
func NewRoundRobinStrategy() *RoundRobinStrategy {
	return &RoundRobinStrategy{}
}

// Select picks the next replica in rotation
func (s *RoundRobinStrategy) Select(replicas []string) string {
	if len(replicas) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&s.counter, 1) - 1
	return replicas[idx%uint64(len(replicas))]
}

// =============================================================================
// Least Connections Strategy
// =============================================================================

// LeastConnectionsStrategy picks the replica with fewest active connections
type LeastConnectionsStrategy struct {
	mu          sync.RWMutex
	connections map[string]int64
}

// NewLeastConnectionsStrategy creates a new least connections strategy
func NewLeastConnectionsStrategy() *LeastConnectionsStrategy {
	return &LeastConnectionsStrategy{
		connections: make(map[string]int64),
	}
}

// Select picks the replica with lowest connection count
func (s *LeastConnectionsStrategy) Select(replicas []string) string {
	if len(replicas) == 0 {
		return ""
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	minConn := int64(-1)
	selected := replicas[0]

	for _, r := range replicas {
		conn := s.connections[r]
		if minConn == -1 || conn < minConn {
			minConn = conn
			selected = r
		}
	}

	return selected
}

// IncrementConnections adds a connection for a replica
func (s *LeastConnectionsStrategy) IncrementConnections(replica string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[replica]++
}

// DecrementConnections removes a connection for a replica
func (s *LeastConnectionsStrategy) DecrementConnections(replica string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connections[replica] > 0 {
		s.connections[replica]--
	}
}

// GetConnectionCount returns current connection count for a replica
func (s *LeastConnectionsStrategy) GetConnectionCount(replica string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connections[replica]
}

// =============================================================================
// Weighted Strategy
// =============================================================================

// WeightedStrategy picks replicas based on assigned weights
type WeightedStrategy struct {
	mu      sync.RWMutex
	weights map[string]int
	// rng     *rand.Rand // Removed for G404 compliance
}

// NewWeightedStrategy creates a new weighted strategy
func NewWeightedStrategy() *WeightedStrategy {
	return &WeightedStrategy{
		weights: make(map[string]int),
	}
}

// SetWeight sets the weight for a replica
func (s *WeightedStrategy) SetWeight(replica string, weight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.weights[replica] = weight
}

// Select picks a replica based on weighted probability
func (s *WeightedStrategy) Select(replicas []string) string {
	if len(replicas) == 0 {
		return ""
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Calculate total weight
	totalWeight := 0
	for _, r := range replicas {
		w := s.weights[r]
		if w <= 0 {
			w = 1 // default weight
		}
		totalWeight += w
	}

	// Pick random weighted value securely
	var pick int
	if totalWeight > 0 {
		bigPick, err := rand.Int(rand.Reader, big.NewInt(int64(totalWeight)))
		if err == nil {
			pick = int(bigPick.Int64())
		}
	}

	cumulative := 0

	for _, r := range replicas {
		w := s.weights[r]
		if w <= 0 {
			w = 1
		}
		cumulative += w
		if pick < cumulative {
			return r
		}
	}

	return replicas[0]
}

// =============================================================================
// Load Balancer Configuration
// =============================================================================

// LoadBalancerConfig holds configuration for the load balancer
type LoadBalancerConfig struct {
	Strategy            StrategyType
	HealthCheckInterval time.Duration
}

// LoadBalancerStats holds statistics about load balancer operations
type LoadBalancerStats struct {
	TotalSelections  int64
	FailedSelections int64
	HealthyReplicas  int
	TotalReplicas    int
}

// =============================================================================
// Replica Info
// =============================================================================

// ReplicaInfo holds information about a replica
type ReplicaInfo struct {
	ID      string
	Address string
	Healthy bool
}

// =============================================================================
// Replica Load Balancer
// =============================================================================

var (
	ErrNoHealthyReplicas = errors.New("no healthy replicas available")
)

// ReplicaLoadBalancer distributes read requests across replicas
type ReplicaLoadBalancer struct {
	mu       sync.RWMutex
	config   LoadBalancerConfig
	replicas map[string]*ReplicaInfo
	strategy LoadBalancerStrategy

	// Stats
	totalSelections  int64
	failedSelections int64
}

// NewReplicaLoadBalancer creates a new load balancer
func NewReplicaLoadBalancer(config LoadBalancerConfig) *ReplicaLoadBalancer {
	lb := &ReplicaLoadBalancer{
		config:   config,
		replicas: make(map[string]*ReplicaInfo),
	}

	// Initialize strategy based on config
	switch config.Strategy {
	case StrategyLeastConnections:
		lb.strategy = NewLeastConnectionsStrategy()
	case StrategyWeighted:
		lb.strategy = NewWeightedStrategy()
	default:
		lb.strategy = NewRoundRobinStrategy()
	}

	return lb
}

// AddReplica adds a replica to the load balancer
func (lb *ReplicaLoadBalancer) AddReplica(id, address string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.replicas[id] = &ReplicaInfo{
		ID:      id,
		Address: address,
		Healthy: true,
	}

	metrics.LoadBalancerReplicasTotal.Inc()
}

// RemoveReplica removes a replica from the load balancer
func (lb *ReplicaLoadBalancer) RemoveReplica(id string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	delete(lb.replicas, id)
	metrics.LoadBalancerReplicasTotal.Dec()
}

// GetReplicas returns all replica IDs
func (lb *ReplicaLoadBalancer) GetReplicas() []string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	result := make([]string, 0, len(lb.replicas))
	for id := range lb.replicas {
		result = append(result, id)
	}
	return result
}

// GetHealthyReplicas returns all healthy replica IDs
func (lb *ReplicaLoadBalancer) GetHealthyReplicas() []string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	result := make([]string, 0, len(lb.replicas))
	for id, r := range lb.replicas {
		if r.Healthy {
			result = append(result, id)
		}
	}
	return result
}

// MarkUnhealthy marks a replica as unhealthy
func (lb *ReplicaLoadBalancer) MarkUnhealthy(id string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if r, ok := lb.replicas[id]; ok {
		r.Healthy = false
		metrics.LoadBalancerUnhealthyTotal.Inc()
	}
}

// MarkHealthy marks a replica as healthy
func (lb *ReplicaLoadBalancer) MarkHealthy(id string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if r, ok := lb.replicas[id]; ok {
		r.Healthy = true
	}
}

// SelectReplica selects a healthy replica using the configured strategy
func (lb *ReplicaLoadBalancer) SelectReplica(ctx context.Context) (string, error) {
	_ = ctx // context for future timeout support

	healthy := lb.GetHealthyReplicas()
	if len(healthy) == 0 {
		atomic.AddInt64(&lb.failedSelections, 1)
		metrics.LoadBalancerSelectionsTotal.WithLabelValues("failed").Inc()
		return "", ErrNoHealthyReplicas
	}

	selected := lb.strategy.Select(healthy)
	atomic.AddInt64(&lb.totalSelections, 1)
	metrics.LoadBalancerSelectionsTotal.WithLabelValues("success").Inc()

	return selected, nil
}

// GetStats returns load balancer statistics
func (lb *ReplicaLoadBalancer) GetStats() LoadBalancerStats {
	lb.mu.RLock()
	healthyCount := 0
	for _, r := range lb.replicas {
		if r.Healthy {
			healthyCount++
		}
	}
	totalCount := len(lb.replicas)
	lb.mu.RUnlock()

	return LoadBalancerStats{
		TotalSelections:  atomic.LoadInt64(&lb.totalSelections),
		FailedSelections: atomic.LoadInt64(&lb.failedSelections),
		HealthyReplicas:  healthyCount,
		TotalReplicas:    totalCount,
	}
}
