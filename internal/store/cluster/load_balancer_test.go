package cluster

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

// TestLoadBalancerStrategy_RoundRobin runs the test load balancer strategy_round robin operation.
func TestLoadBalancerStrategy_RoundRobin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestLoadBalancerStrategy_LeastConnections runs the test load balancer strategy_least connections operation.
func TestLoadBalancerStrategy_LeastConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestLoadBalancerStrategy_LeastConnections_Release runs the test load balancer strategy_least connections_release operation.
func TestLoadBalancerStrategy_LeastConnections_Release(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	strategy := NewLeastConnectionsStrategy()

	strategy.IncrementConnections("replica-1")
	strategy.IncrementConnections("replica-1")
	strategy.DecrementConnections("replica-1")

	count := strategy.GetConnectionCount("replica-1")
	if count != 1 {
		t.Errorf("expected 1 connection, got %d", count)
	}
}

// TestReplicaLoadBalancer_New runs the test replica load balancer_new operation.
func TestReplicaLoadBalancer_New(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := LoadBalancerConfig{
		Strategy:            StrategyRoundRobin,
		HealthCheckInterval: 5 * time.Second,
	}
	lb := NewReplicaLoadBalancer(config)

	if lb == nil {
		t.Fatal("NewReplicaLoadBalancer returned nil")
	}
}

// TestReplicaLoadBalancer_AddRemoveReplica runs the test replica load balancer_add remove replica operation.
func TestReplicaLoadBalancer_AddRemoveReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestReplicaLoadBalancer_SelectHealthy runs the test replica load balancer_select healthy operation.
func TestReplicaLoadBalancer_SelectHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestReplicaLoadBalancer_MarkHealthy runs the test replica load balancer_mark healthy operation.
func TestReplicaLoadBalancer_MarkHealthy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestReplicaLoadBalancer_NoHealthyReplicas runs the test replica load balancer_no healthy replicas operation.
func TestReplicaLoadBalancer_NoHealthyReplicas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestReplicaLoadBalancer_ConcurrentAccess runs the test replica load balancer_concurrent access operation.
func TestReplicaLoadBalancer_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestReplicaLoadBalancer_Metrics runs the test replica load balancer_metrics operation.
func TestReplicaLoadBalancer_Metrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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

// TestLoadBalancerStrategy_Weighted runs the test load balancer strategy_weighted operation.
func TestLoadBalancerStrategy_Weighted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
