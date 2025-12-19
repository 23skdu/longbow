package store

import (
"context"
"sync"
"sync/atomic"
"testing"
"time"
)

// =============================================================================
// TDD Red Phase: Read Replica Load Balancer Tests
// =============================================================================

func TestLoadBalancerStrategy_RoundRobin(t *testing.T) {
replicas := []string{"replica-1", "replica-2", "replica-3"}
strategy := NewRoundRobinStrategy()

// Should cycle through replicas
for i := 0; i < 9; i++ {
expected := replicas[i%3]
got := strategy.Select(replicas)
if got != expected {
t.Errorf("round %d: expected %s, got %s", i, expected, got)
}
}
}

func TestLoadBalancerStrategy_LeastConnections(t *testing.T) {
strategy := NewLeastConnectionsStrategy()

// Add connections to replicas
strategy.IncrementConnections("replica-1")
strategy.IncrementConnections("replica-1")
strategy.IncrementConnections("replica-2")
// replica-3 has 0 connections

replicas := []string{"replica-1", "replica-2", "replica-3"}
got := strategy.Select(replicas)

if got != "replica-3" {
t.Errorf("expected replica-3 (0 connections), got %s", got)
}
}

func TestLoadBalancerStrategy_LeastConnections_Release(t *testing.T) {
strategy := NewLeastConnectionsStrategy()

strategy.IncrementConnections("replica-1")
strategy.IncrementConnections("replica-1")
strategy.DecrementConnections("replica-1")

count := strategy.GetConnectionCount("replica-1")
if count != 1 {
t.Errorf("expected 1 connection, got %d", count)
}
}

func TestReplicaLoadBalancer_New(t *testing.T) {
config := LoadBalancerConfig{
Strategy:        StrategyRoundRobin,
HealthCheckInterval: 5 * time.Second,
}
lb := NewReplicaLoadBalancer(config)

if lb == nil {
t.Fatal("NewReplicaLoadBalancer returned nil")
}
}

func TestReplicaLoadBalancer_AddRemoveReplica(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

lb.AddReplica("replica-1", "localhost:8001")
lb.AddReplica("replica-2", "localhost:8002")

replicas := lb.GetReplicas()
if len(replicas) != 2 {
t.Errorf("expected 2 replicas, got %d", len(replicas))
}

lb.RemoveReplica("replica-1")
replicas = lb.GetReplicas()
if len(replicas) != 1 {
t.Errorf("expected 1 replica after removal, got %d", len(replicas))
}
}

func TestReplicaLoadBalancer_SelectHealthy(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

lb.AddReplica("replica-1", "localhost:8001")
lb.AddReplica("replica-2", "localhost:8002")
lb.AddReplica("replica-3", "localhost:8003")

// Mark replica-2 as unhealthy
lb.MarkUnhealthy("replica-2")

// Should only select from healthy replicas
ctx := context.Background()
for i := 0; i < 6; i++ {
replica, err := lb.SelectReplica(ctx)
if err != nil {
t.Fatalf("SelectReplica failed: %v", err)
}
if replica == "replica-2" {
t.Error("should not select unhealthy replica-2")
}
}
}

func TestReplicaLoadBalancer_MarkHealthy(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

lb.AddReplica("replica-1", "localhost:8001")
lb.MarkUnhealthy("replica-1")

healthy := lb.GetHealthyReplicas()
if len(healthy) != 0 {
t.Errorf("expected 0 healthy replicas, got %d", len(healthy))
}

lb.MarkHealthy("replica-1")
healthy = lb.GetHealthyReplicas()
if len(healthy) != 1 {
t.Errorf("expected 1 healthy replica after recovery, got %d", len(healthy))
}
}

func TestReplicaLoadBalancer_NoHealthyReplicas(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

lb.AddReplica("replica-1", "localhost:8001")
lb.MarkUnhealthy("replica-1")

ctx := context.Background()
_, err := lb.SelectReplica(ctx)

if err == nil {
t.Error("expected error when no healthy replicas")
}
}

func TestReplicaLoadBalancer_ConcurrentAccess(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

// Add replicas
for i := 0; i < 5; i++ {
lb.AddReplica(string(rune('a'+i)), "localhost:800"+string(rune('0'+i)))
}

var wg sync.WaitGroup
var successCount int64

// Concurrent selections
for i := 0; i < 100; i++ {
wg.Add(1)
go func() {
defer wg.Done()
ctx := context.Background()
_, err := lb.SelectReplica(ctx)
if err == nil {
atomic.AddInt64(&successCount, 1)
}
}()
}

wg.Wait()

if atomic.LoadInt64(&successCount) != 100 {
t.Errorf("expected 100 successful selections, got %d", successCount)
}
}

func TestReplicaLoadBalancer_Metrics(t *testing.T) {
lb := NewReplicaLoadBalancer(LoadBalancerConfig{
Strategy: StrategyRoundRobin,
})

lb.AddReplica("replica-1", "localhost:8001")

ctx := context.Background()
for i := 0; i < 10; i++ {
_, _ = lb.SelectReplica(ctx)
}

stats := lb.GetStats()
if stats.TotalSelections < 10 {
t.Errorf("expected at least 10 selections, got %d", stats.TotalSelections)
}
}

func TestLoadBalancerStrategy_Weighted(t *testing.T) {
strategy := NewWeightedStrategy()

// Set weights
strategy.SetWeight("replica-1", 3) // 3x more likely
strategy.SetWeight("replica-2", 1)
strategy.SetWeight("replica-3", 1)

// With deterministic weights, should work
replicas := []string{"replica-1", "replica-2", "replica-3"}
selected := strategy.Select(replicas)

if selected == "" {
t.Error("Select returned empty string")
}
}
