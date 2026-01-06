package store


import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func createTestRecord(t *testing.T) arrow.Record { //nolint:staticcheck
	t.Helper()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).Append(1)
	return builder.NewRecord() //nolint:staticcheck //nolint:staticcheck
}

func TestPeerReplicator_NewPeerReplicator(t *testing.T) {
	cfg := DefaultReplicatorConfig()
	r := NewPeerReplicator(cfg)

	if r == nil {
		t.Fatal("expected non-nil peer replicator")
	}
	if r.config.Timeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", r.config.Timeout)
	}
	if r.config.MaxRetries != 3 {
		t.Errorf("expected 3 retries, got %d", r.config.MaxRetries)
	}
}

func TestPeerReplicator_AddPeer(t *testing.T) {
	r := NewPeerReplicator(DefaultReplicatorConfig())

	err := r.AddPeer("peer1", "localhost:9090")
	if err != nil {
		t.Fatalf("AddPeer failed: %v", err)
	}

	peers := r.GetPeers()
	if len(peers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].ID != "peer1" {
		t.Errorf("expected peer1, got %s", peers[0].ID)
	}
}

func TestPeerReplicator_RemovePeer(t *testing.T) {
	r := NewPeerReplicator(DefaultReplicatorConfig())
	_ = r.AddPeer("peer1", "localhost:9090")

	r.RemovePeer("peer1")

	peers := r.GetPeers()
	if len(peers) != 0 {
		t.Errorf("expected 0 peers after removal, got %d", len(peers))
	}
}

func TestPeerReplicator_ReplicateRecord(t *testing.T) {
	// Use very short timeout to fail fast
	cfg := ReplicatorConfig{
		Timeout:      100 * time.Millisecond,
		MaxRetries:   0,
		RetryBackoff: 10 * time.Millisecond,
		QuorumSize:   1,
		CircuitBreakerConfig: CircuitBreakerConfig{
			FailureThreshold: 1,
			Timeout:          100 * time.Millisecond,
		},
	}
	r := NewPeerReplicator(cfg)
	_ = r.AddPeer("peer1", "invalid:9090")

	record := createTestRecord(t)
	defer record.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results := r.ReplicateRecord(ctx, "test", record)

	// Should get results for the peer (will fail due to invalid address)
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if results[0].Success {
		t.Error("expected failure for invalid peer")
	}
}

func TestPeerReplicator_ReplicateWithCircuitBreaker(t *testing.T) {
	cfg := ReplicatorConfig{
		Timeout:      100 * time.Millisecond,
		MaxRetries:   0,
		RetryBackoff: 10 * time.Millisecond,
		QuorumSize:   1,
		CircuitBreakerConfig: CircuitBreakerConfig{
			FailureThreshold: 2,
			Timeout:          1 * time.Second,
		},
	}
	r := NewPeerReplicator(cfg)
	_ = r.AddPeer("peer1", "invalid:9090")

	record := createTestRecord(t)
	defer record.Release()

	ctx := context.Background()

	// First call - should fail and increment failure count
	r.ReplicateRecord(ctx, "test", record)

	// Second call - should fail and trip circuit
	r.ReplicateRecord(ctx, "test", record)

	// Third call - circuit should be open
	results := r.ReplicateRecord(ctx, "test", record)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Error != ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen, got %v", results[0].Error)
	}
}

func TestPeerReplicator_AsyncReplication(t *testing.T) {
	cfg := ReplicatorConfig{
		Timeout:          100 * time.Millisecond,
		MaxRetries:       0,
		RetryBackoff:     10 * time.Millisecond,
		AsyncReplication: true,
		AsyncBufferSize:  10,
		QuorumSize:       1,
		CircuitBreakerConfig: CircuitBreakerConfig{
			FailureThreshold: 5,
			Timeout:          100 * time.Millisecond,
		},
	}
	r := NewPeerReplicator(cfg)
	_ = r.AddPeer("peer1", "invalid:9090")

	err := r.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	record := createTestRecord(t)
	defer record.Release()

	// Queue async replication
	queued := r.ReplicateAsync(context.Background(), "test", record)
	if !queued {
		t.Error("expected task to be queued")
	}

	// Wait briefly for processing
	time.Sleep(200 * time.Millisecond)

	r.Stop()

	stats := r.Stats()
	if stats.QueuedTotal != 1 {
		t.Errorf("expected 1 queued, got %d", stats.QueuedTotal)
	}
}

func TestPeerReplicator_QuorumReplication(t *testing.T) {
	// Test quorum with short timeouts
	cfg := ReplicatorConfig{
		Timeout:      50 * time.Millisecond,
		MaxRetries:   0,
		RetryBackoff: 10 * time.Millisecond,
		QuorumSize:   1, // Quorum of 1
		CircuitBreakerConfig: CircuitBreakerConfig{
			FailureThreshold: 1,
			Timeout:          100 * time.Millisecond,
		},
	}
	r := NewPeerReplicator(cfg)
	// Add invalid peer - will fail
	_ = r.AddPeer("peer1", "invalid:9090")

	record := createTestRecord(t)
	defer record.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	results := r.ReplicateWithQuorum(ctx, "test", record)

	// Should get results even if failed
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
}

func TestPeerReplicator_Stats(t *testing.T) {
	r := NewPeerReplicator(DefaultReplicatorConfig())

	stats := r.Stats()
	if stats.SuccessTotal != 0 {
		t.Errorf("expected 0 success, got %d", stats.SuccessTotal)
	}
	if stats.FailureTotal != 0 {
		t.Errorf("expected 0 failure, got %d", stats.FailureTotal)
	}
}

func TestPeerReplicator_GetCircuitBreaker(t *testing.T) {
	r := NewPeerReplicator(DefaultReplicatorConfig())
	_ = r.AddPeer("peer1", "localhost:9090")

	cb := r.GetCircuitBreaker("peer1")
	if cb == nil {
		t.Error("expected non-nil circuit breaker")
	}
	if !cb.Allow() {
		t.Error("new circuit breaker should allow requests")
	}
}
