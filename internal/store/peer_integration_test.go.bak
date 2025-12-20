package store

import (
	"context"
	"sync"

	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// PeerConfig Tests
// =============================================================================

func TestPeerConfig_Defaults(t *testing.T) {
	cfg := DefaultPeerConfig()

	if cfg.Mode != ReplicationModeAsync {
		t.Errorf("expected ReplicationModeAsync, got %v", cfg.Mode)
	}
	if cfg.ReplicationTimeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", cfg.ReplicationTimeout)
	}
	if cfg.MaxReplicationRetries != 3 {
		t.Errorf("expected 3 retries, got %d", cfg.MaxReplicationRetries)
	}
	if cfg.PeerFallbackEnabled != true {
		t.Errorf("expected peer fallback enabled")
	}
	if cfg.PeerFallbackTimeout != 2*time.Second {
		t.Errorf("expected 2s fallback timeout, got %v", cfg.PeerFallbackTimeout)
	}
}

func TestPeerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*PeerConfig)
		wantErr bool
	}{
		{"valid_defaults", func(c *PeerConfig) {}, false},
		{"zero_timeout", func(c *PeerConfig) { c.ReplicationTimeout = 0 }, true},
		{"negative_retries", func(c *PeerConfig) { c.MaxReplicationRetries = -1 }, true},
		{"zero_fallback_timeout", func(c *PeerConfig) { c.PeerFallbackTimeout = 0 }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultPeerConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// PeerManager Tests
// =============================================================================

func TestPeerManager_AddRemovePeers(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())

	// Add peers
	pm.AddPeer("localhost:8081")
	pm.AddPeer("localhost:8082")
	pm.AddPeer("localhost:8083")

	peers := pm.GetPeers()
	if len(peers) != 3 {
		t.Errorf("expected 3 peers, got %d", len(peers))
	}

	// Remove peer
	pm.RemovePeer("localhost:8082")
	peers = pm.GetPeers()
	if len(peers) != 2 {
		t.Errorf("expected 2 peers after removal, got %d", len(peers))
	}

	// Verify correct peer removed
	for _, p := range peers {
		if p == "localhost:8082" {
			t.Error("removed peer still present")
		}
	}
}

func TestPeerManager_DuplicatePeer(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())

	pm.AddPeer("localhost:8081")
	pm.AddPeer("localhost:8081") // duplicate

	peers := pm.GetPeers()
	if len(peers) != 1 {
		t.Errorf("expected 1 peer (no duplicates), got %d", len(peers))
	}
}

func TestPeerManager_PeerHealth(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())
	pm.AddPeer("localhost:8081")

	// Mark unhealthy
	pm.MarkPeerUnhealthy("localhost:8081")
	if pm.IsPeerHealthy("localhost:8081") {
		t.Error("expected peer to be unhealthy")
	}

	// Mark healthy again
	pm.MarkPeerHealthy("localhost:8081")
	if !pm.IsPeerHealthy("localhost:8081") {
		t.Error("expected peer to be healthy")
	}
}

func TestPeerManager_GetHealthyPeers(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())
	pm.AddPeer("localhost:8081")
	pm.AddPeer("localhost:8082")
	pm.AddPeer("localhost:8083")

	pm.MarkPeerUnhealthy("localhost:8082")

	healthy := pm.GetHealthyPeers()
	if len(healthy) != 2 {
		t.Errorf("expected 2 healthy peers, got %d", len(healthy))
	}
}

// =============================================================================
// PeerStreamingService Tests
// =============================================================================

func TestPeerStreamingService_Creation(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	pm := NewPeerManager(pool, cfg)

	svc := NewPeerStreamingService(pm, pool, cfg)
	if svc == nil {
		t.Fatal("expected non-nil service")
	}

	stats := svc.Stats()
	if stats.TotalGetRequests != 0 {
		t.Errorf("expected 0 get requests, got %d", stats.TotalGetRequests)
	}
}

func TestPeerStreamingService_DoGetFromPeers_NoPeers(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	pm := NewPeerManager(pool, cfg)
	svc := NewPeerStreamingService(pm, pool, cfg)

	ctx := context.Background()
	records, err := svc.DoGetFromPeers(ctx, "test-dataset", nil)

	if err == nil {
		t.Error("expected error when no peers available")
	}
	if records != nil {
		t.Error("expected nil records when no peers")
	}
}

func TestPeerStreamingService_DoGetFromPeers_AllUnhealthy(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	pm := NewPeerManager(pool, cfg)
	pm.AddPeer("localhost:8081")
	pm.AddPeer("localhost:8082")
	pm.MarkPeerUnhealthy("localhost:8081")
	pm.MarkPeerUnhealthy("localhost:8082")

	svc := NewPeerStreamingService(pm, pool, cfg)

	ctx := context.Background()
	records, err := svc.DoGetFromPeers(ctx, "test-dataset", nil)

	if err == nil {
		t.Error("expected error when all peers unhealthy")
	}
	if records != nil {
		t.Error("expected nil records")
	}
}

func TestPeerStreamingService_ReplicateToPeers_NoPeers(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	pm := NewPeerManager(pool, cfg)
	svc := NewPeerStreamingService(pm, pool, cfg)

	ctx := context.Background()
	result := svc.ReplicateToPeers(ctx, "test-dataset", nil)

	// No peers = no errors (nothing to replicate to)
	if result.TotalPeers != 0 {
		t.Errorf("expected 0 total peers, got %d", result.TotalPeers)
	}
}

func TestPeerStreamingService_ReplicateToPeers_NoServer(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	cfg.ReplicationTimeout = 100 * time.Millisecond
	pm := NewPeerManager(pool, cfg)
	pm.AddPeer("localhost:19999") // non-existent server

	svc := NewPeerStreamingService(pm, pool, cfg)

	// Create test record
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ctx := context.Background()
	result := svc.ReplicateToPeers(ctx, "test-dataset", []arrow.RecordBatch{rec})

	if result.TotalPeers != 1 {
		t.Errorf("expected 1 total peer, got %d", result.TotalPeers)
	}
	if result.FailedPeers != 1 {
		t.Errorf("expected 1 failed peer, got %d", result.FailedPeers)
	}
}

// =============================================================================
// ReplicationResult Tests
// =============================================================================

func TestReplicationResult_Success(t *testing.T) {
	result := &ReplicationResult{
		TotalPeers:      3,
		SuccessfulPeers: 3,
		FailedPeers:     0,
		Errors:          make(map[string]error),
	}

	if !result.AllSucceeded() {
		t.Error("expected AllSucceeded() = true")
	}
	if result.AnyFailed() {
		t.Error("expected AnyFailed() = false")
	}
}

func TestReplicationResult_PartialFailure(t *testing.T) {
	result := &ReplicationResult{
		TotalPeers:      3,
		SuccessfulPeers: 2,
		FailedPeers:     1,
		Errors: map[string]error{
			"localhost:8083": context.DeadlineExceeded,
		},
	}

	if result.AllSucceeded() {
		t.Error("expected AllSucceeded() = false")
	}
	if !result.AnyFailed() {
		t.Error("expected AnyFailed() = true")
	}
	if result.SuccessRate() != 2.0/3.0 {
		t.Errorf("expected success rate 0.666, got %f", result.SuccessRate())
	}
}

// =============================================================================
// Statistics Tests
// =============================================================================

func TestPeerStreamingService_Statistics(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	cfg := DefaultPeerConfig()
	pm := NewPeerManager(pool, cfg)
	svc := NewPeerStreamingService(pm, pool, cfg)

	// Trigger some operations to update stats
	ctx := context.Background()
	_, _ = svc.DoGetFromPeers(ctx, "test", nil) // will fail, no peers
	_ = svc.ReplicateToPeers(ctx, "test", nil)  // will succeed, no peers

	stats := svc.Stats()
	if stats.TotalGetRequests != 1 {
		t.Errorf("expected 1 get request, got %d", stats.TotalGetRequests)
	}
	if stats.TotalPutRequests != 1 {
		t.Errorf("expected 1 put request, got %d", stats.TotalPutRequests)
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestPeerManager_ConcurrentAccess(t *testing.T) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			peer := "localhost:" + string(rune('8'+(n%10))) + "08" + string(rune('0'+(n%10)))
			pm.AddPeer(peer)
			_ = pm.GetPeers()
			_ = pm.GetHealthyPeers()
			if n%3 == 0 {
				pm.MarkPeerUnhealthy(peer)
			}
			if n%5 == 0 {
				pm.RemovePeer(peer)
			}
		}(i)
	}
	wg.Wait()

	// Should not panic or race
	peers := pm.GetPeers()
	t.Logf("Final peer count: %d", len(peers))
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkPeerManager_GetPeers(b *testing.B) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())
	for i := 0; i < 10; i++ {
		pm.AddPeer("localhost:" + string(rune('0'+i)) + "000")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pm.GetPeers()
	}
}

func BenchmarkPeerManager_GetHealthyPeers(b *testing.B) {
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	defer func() { _ = pool.Close() }()

	pm := NewPeerManager(pool, DefaultPeerConfig())
	for i := 0; i < 10; i++ {
		pm.AddPeer("localhost:" + string(rune('0'+i)) + "000")
	}
	// Mark half unhealthy
	for i := 0; i < 5; i++ {
		pm.MarkPeerUnhealthy("localhost:" + string(rune('0'+i)) + "000")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pm.GetHealthyPeers()
	}
}
