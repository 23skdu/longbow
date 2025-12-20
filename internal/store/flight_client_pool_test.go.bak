package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// FlightClientPoolConfig Tests
// =============================================================================

func TestFlightClientPoolConfig_Defaults(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()

	if cfg.MaxConnsPerHost <= 0 {
		t.Error("MaxConnsPerHost should be positive")
	}
	if cfg.MinConnsPerHost < 0 {
		t.Error("MinConnsPerHost should not be negative")
	}
	if cfg.ConnTimeout <= 0 {
		t.Error("ConnTimeout should be positive")
	}
	if cfg.IdleTimeout <= 0 {
		t.Error("IdleTimeout should be positive")
	}
	if cfg.MaxConnLifetime <= 0 {
		t.Error("MaxConnLifetime should be positive")
	}
	if cfg.HealthCheckInterval <= 0 {
		t.Error("HealthCheckInterval should be positive")
	}
}

func TestFlightClientPoolConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*FlightClientPoolConfig)
		wantErr bool
	}{
		{
			name:    "valid_defaults",
			modify:  func(c *FlightClientPoolConfig) {},
			wantErr: false,
		},
		{
			name:    "zero_max_conns_invalid",
			modify:  func(c *FlightClientPoolConfig) { c.MaxConnsPerHost = 0 },
			wantErr: true,
		},
		{
			name:    "negative_max_conns_invalid",
			modify:  func(c *FlightClientPoolConfig) { c.MaxConnsPerHost = -1 },
			wantErr: true,
		},
		{
			name: "min_exceeds_max_invalid",
			modify: func(c *FlightClientPoolConfig) {
				c.MinConnsPerHost = 10
				c.MaxConnsPerHost = 5
			},
			wantErr: true,
		},
		{
			name:    "zero_conn_timeout_invalid",
			modify:  func(c *FlightClientPoolConfig) { c.ConnTimeout = 0 },
			wantErr: true,
		},
		{
			name:    "negative_idle_timeout_invalid",
			modify:  func(c *FlightClientPoolConfig) { c.IdleTimeout = -1 },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultFlightClientPoolConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// FlightClientPool Tests
// =============================================================================

func TestFlightClientPool_Creation(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	if pool == nil {
		t.Fatal("NewFlightClientPool returned nil")
	}

	stats := pool.Stats()
	if stats.TotalHosts != 0 {
		t.Errorf("Expected 0 hosts, got %d", stats.TotalHosts)
	}
	if stats.TotalConnections != 0 {
		t.Errorf("Expected 0 connections, got %d", stats.TotalConnections)
	}
}

func TestFlightClientPool_AddRemoveHost(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	// Add host
	err := pool.AddHost("localhost:8815")
	if err != nil {
		t.Fatalf("AddHost failed: %v", err)
	}

	stats := pool.Stats()
	if stats.TotalHosts != 1 {
		t.Errorf("Expected 1 host, got %d", stats.TotalHosts)
	}

	// Add duplicate should error
	err = pool.AddHost("localhost:8815")
	if err == nil {
		t.Error("Expected error adding duplicate host")
	}

	// Remove host
	err = pool.RemoveHost("localhost:8815")
	if err != nil {
		t.Fatalf("RemoveHost failed: %v", err)
	}

	stats = pool.Stats()
	if stats.TotalHosts != 0 {
		t.Errorf("Expected 0 hosts after removal, got %d", stats.TotalHosts)
	}

	// Remove non-existent should error
	err = pool.RemoveHost("localhost:8815")
	if err == nil {
		t.Error("Expected error removing non-existent host")
	}
}

func TestFlightClientPool_GetConnection(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	cfg.ConnTimeout = 100 * time.Millisecond // Fast timeout for test
	pool := NewFlightClientPool(cfg)

	// Get with auto-add creates host entry
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// This will fail to connect (no server) but should create host entry
	_, err := pool.Get(ctx, "localhost:9999")
	// Connection error expected - no server running
	if err == nil {
		t.Log("Note: Get succeeded - server may be running")
	}

	// Host should be tracked even after failed connection
	stats := pool.Stats()
	if stats.TotalHosts != 1 {
		t.Errorf("Expected 1 host tracked, got %d", stats.TotalHosts)
	}
}

func TestFlightClientPool_ReturnConnection(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	// Return nil should not panic
	pool.Put(nil)

	// Return with unknown host should not panic
	mockConn := &PooledFlightClient{
		host: "unknown:8815",
	}
	pool.Put(mockConn)
}

func TestFlightClientPool_Statistics(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	_ = pool.AddHost("host1:8815")
	_ = pool.AddHost("host2:8815")
	_ = pool.AddHost("host3:8815")

	stats := pool.Stats()
	if stats.TotalHosts != 3 {
		t.Errorf("Expected 3 hosts, got %d", stats.TotalHosts)
	}
}

func TestFlightClientPool_Close(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	_ = pool.AddHost("host1:8815")
	_ = pool.AddHost("host2:8815")

	err := pool.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should error
	err = pool.AddHost("host3:8815")
	if err == nil {
		t.Error("Expected error after pool closed")
	}
}

func TestFlightClientPool_ConcurrentAccess(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)

	var wg sync.WaitGroup
	var errors int64

	// Concurrent adds
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			host := "host" + string(rune('a'+id)) + ":8815"
			if err := pool.AddHost(host); err != nil {
				atomic.AddInt64(&errors, 1)
			}
		}(i)
	}

	wg.Wait()

	stats := pool.Stats()
	if stats.TotalHosts != 10 {
		t.Errorf("Expected 10 hosts, got %d (errors: %d)", stats.TotalHosts, errors)
	}

	_ = pool.Close()
}

// =============================================================================
// PooledFlightClient Tests
// =============================================================================

func TestPooledFlightClient_Metadata(t *testing.T) {
	client := &PooledFlightClient{
		host:      "test:8815",
		createdAt: time.Now(),
		lastUsed:  time.Now(),
	}

	if client.Host() != "test:8815" {
		t.Errorf("Expected host test:8815, got %s", client.Host())
	}

	if client.CreatedAt().IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	if client.LastUsed().IsZero() {
		t.Error("LastUsed should not be zero")
	}

	if client.IsExpired(time.Hour) {
		t.Error("Fresh client should not be expired")
	}

	// Manually age the client
	client.createdAt = time.Now().Add(-2 * time.Hour)
	if !client.IsExpired(time.Hour) {
		t.Error("Old client should be expired")
	}
}

// =============================================================================
// HostPool Tests
// =============================================================================

func TestHostPool_ConnectionLimit(t *testing.T) {
	hp := newHostPool("test:8815", 2, 0) // max 2 connections

	// Track created connections
	conns := make([]*PooledFlightClient, 0)

	// Create up to max
	for i := 0; i < 2; i++ {
		conn := hp.tryCreate()
		if conn == nil {
			t.Errorf("Should be able to create connection %d", i)
		}
		conns = append(conns, conn)
	}

	// Third should fail
	conn := hp.tryCreate()
	if conn != nil {
		t.Error("Should not exceed max connections")
	}

	// Return one
	hp.returnConn(conns[0])

	// Now can create again
	conn = hp.tryCreate()
	if conn == nil {
		t.Error("Should be able to create after return")
	}
}

// =============================================================================
// Integration Tests - DoGetFromPeer / DoPutToPeer
// =============================================================================

func TestFlightClientPool_DoGetFromPeer_NoServer(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	cfg.ConnTimeout = 100 * time.Millisecond
	pool := NewFlightClientPool(cfg)
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Should fail gracefully when no server
	_, err := pool.DoGetFromPeer(ctx, "localhost:9999", "test-dataset", nil)
	if err == nil {
		t.Error("Expected error with no server running")
	}
}

func TestFlightClientPool_DoPutToPeer_NoServer(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	cfg.ConnTimeout = 100 * time.Millisecond
	pool := NewFlightClientPool(cfg)
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Create test record batch
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Should fail gracefully when no server
	err := pool.DoPutToPeer(ctx, "localhost:9999", "test-dataset", []arrow.RecordBatch{rec})
	if err == nil {
		t.Error("Expected error with no server running")
	}
}

func TestFlightClientPool_ReplicateToPeers_NoServers(t *testing.T) {
	cfg := DefaultFlightClientPoolConfig()
	cfg.ConnTimeout = 100 * time.Millisecond
	pool := NewFlightClientPool(cfg)
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Create test record batch
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Float64Builder).AppendValues([]float64{1.0, 2.0}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	peers := []string{"peer1:8815", "peer2:8815", "peer3:8815"}

	// Should return errors for all failed peers
	errors := pool.ReplicateToPeers(ctx, peers, "test-dataset", []arrow.RecordBatch{rec})
	if len(errors) != 3 {
		t.Errorf("Expected 3 errors, got %d", len(errors))
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkFlightClientPool_AddHost(b *testing.B) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)
	defer func() { _ = pool.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		host := "host" + string(rune(i%26+'a')) + ":8815"
		_ = pool.AddHost(host)
		_ = pool.RemoveHost(host)
	}
}

func BenchmarkFlightClientPool_Stats(b *testing.B) {
	cfg := DefaultFlightClientPoolConfig()
	pool := NewFlightClientPool(cfg)
	for i := 0; i < 100; i++ {
		_ = pool.AddHost("host" + string(rune(i)) + ":8815")
	}
	defer func() { _ = pool.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pool.Stats()
	}
}
