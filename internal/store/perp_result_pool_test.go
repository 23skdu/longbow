package store

import (
	"runtime"
	"sync"
	"testing"
)

// =============================================================================
// Subtask 2: TDD Tests for PerPResultPool
// Per-Processor local pools to eliminate sync.Pool contention at high core counts
// =============================================================================

// -----------------------------------------------------------------------------
// Test: PerPResultPoolConfig defaults and validation
// -----------------------------------------------------------------------------

func TestPerPResultPoolConfigDefaults(t *testing.T) {
	cfg := DefaultPerPResultPoolConfig()

	if cfg.NumShards <= 0 {
		t.Errorf("NumShards should be > 0 by default, got %d", cfg.NumShards)
	}
	// Default should be GOMAXPROCS
	if cfg.NumShards != runtime.GOMAXPROCS(0) {
		t.Errorf("NumShards should default to GOMAXPROCS(%d), got %d",
			runtime.GOMAXPROCS(0), cfg.NumShards)
	}
	if !cfg.EnableStats {
		t.Error("EnableStats should be true by default")
	}
}

func TestPerPResultPoolConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  PerPResultPoolConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  PerPResultPoolConfig{NumShards: 4, EnableStats: true},
			wantErr: false,
		},
		{
			name:    "zero shards invalid",
			config:  PerPResultPoolConfig{NumShards: 0},
			wantErr: true,
		},
		{
			name:    "negative shards invalid",
			config:  PerPResultPoolConfig{NumShards: -1},
			wantErr: true,
		},
		{
			name:    "single shard valid",
			config:  PerPResultPoolConfig{NumShards: 1},
			wantErr: false,
		},
		{
			name:    "high shard count valid",
			config:  PerPResultPoolConfig{NumShards: 128},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Test: PerPResultPool creation
// -----------------------------------------------------------------------------

func TestNewPerPResultPool(t *testing.T) {
	pool := NewPerPResultPool(nil) // nil uses defaults
	if pool == nil {
		t.Fatal("NewPerPResultPool returned nil")
	}

	// Should have GOMAXPROCS shards
	expected := runtime.GOMAXPROCS(0)
	if pool.NumShards() != expected {
		t.Errorf("Expected %d shards, got %d", expected, pool.NumShards())
	}
}

func TestNewPerPResultPoolWithConfig(t *testing.T) {
	cfg := &PerPResultPoolConfig{
		NumShards:   8,
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)
	if pool == nil {
		t.Fatal("NewPerPResultPool returned nil")
	}
	if pool.NumShards() != 8 {
		t.Errorf("Expected 8 shards, got %d", pool.NumShards())
	}
}

// -----------------------------------------------------------------------------
// Test: Get/Put operations for standard k values
// -----------------------------------------------------------------------------

func TestPerPResultPoolGetPut(t *testing.T) {
	pool := NewPerPResultPool(nil)

	kValues := []int{10, 20, 50, 100, 256, 1000}
	for _, k := range kValues {
		t.Run("k="+string(rune(k)), func(t *testing.T) {
			slice := pool.Get(k)
			if slice == nil {
				t.Fatalf("Get(%d) returned nil", k)
			}
			if len(slice) != k {
				t.Errorf("Get(%d) returned slice of length %d", k, len(slice))
			}
			// Verify zeroed
			for i, v := range slice {
				if v != 0 {
					t.Errorf("Slice[%d] not zeroed: %d", i, v)
				}
			}
			// Put it back
			pool.Put(slice)
		})
	}
}

func TestPerPResultPoolReslicing(t *testing.T) {
	pool := NewPerPResultPool(nil)

	// Request k=7, should get from k=10 pool and reslice
	slice := pool.Get(7)
	if len(slice) != 7 {
		t.Errorf("Expected length 7, got %d", len(slice))
	}
	if cap(slice) < 10 {
		t.Errorf("Expected capacity >= 10, got %d", cap(slice))
	}
	pool.Put(slice)

	// Request k=75, should get from k=100 pool
	slice = pool.Get(75)
	if len(slice) != 75 {
		t.Errorf("Expected length 75, got %d", len(slice))
	}
	pool.Put(slice)
}

func TestPerPResultPoolOversized(t *testing.T) {
	pool := NewPerPResultPool(nil)

	// Request k > 1000, should allocate new
	slice := pool.Get(2000)
	if len(slice) != 2000 {
		t.Errorf("Expected length 2000, got %d", len(slice))
	}
	// Put oversized - should not panic
	pool.Put(slice)
}

// -----------------------------------------------------------------------------
// Test: Per-P distribution - verify sharding works
// -----------------------------------------------------------------------------

func TestPerPResultPoolShardDistribution(t *testing.T) {
	cfg := &PerPResultPoolConfig{
		NumShards:   4,
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)

	// Multiple gets should distribute across shards
	const iterations = 100
	for i := 0; i < iterations; i++ {
		slice := pool.Get(10)
		pool.Put(slice)
	}

	stats := pool.Stats()
	if stats.TotalGets != iterations {
		t.Errorf("Expected %d total gets, got %d", iterations, stats.TotalGets)
	}
}

// -----------------------------------------------------------------------------
// Test: Concurrent access safety
// -----------------------------------------------------------------------------

func TestPerPResultPoolConcurrentAccess(t *testing.T) {
	pool := NewPerPResultPool(nil)

	const goroutines = 100
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				k := []int{10, 20, 50, 100, 256}[i%5]
				slice := pool.Get(k)
				// Simulate work
				for j := range slice {
					slice[j] = VectorID(j)
				}
				pool.Put(slice)
			}
		}()
	}

	wg.Wait()

	stats := pool.Stats()
	expectedOps := uint64(goroutines * opsPerGoroutine)
	if stats.TotalGets != expectedOps {
		t.Errorf("Expected %d gets, got %d", expectedOps, stats.TotalGets)
	}
	if stats.TotalPuts != expectedOps {
		t.Errorf("Expected %d puts, got %d", expectedOps, stats.TotalPuts)
	}
}

// -----------------------------------------------------------------------------
// Test: Statistics tracking
// -----------------------------------------------------------------------------

func TestPerPResultPoolStats(t *testing.T) {
	cfg := &PerPResultPoolConfig{
		NumShards:   2,
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)

	// Initial stats should be zero
	stats := pool.Stats()
	if stats.TotalGets != 0 || stats.TotalPuts != 0 {
		t.Error("Initial stats should be zero")
	}

	// Perform operations
	slice := pool.Get(10)
	pool.Put(slice)

	stats = pool.Stats()
	if stats.TotalGets != 1 {
		t.Errorf("Expected 1 get, got %d", stats.TotalGets)
	}
	if stats.TotalPuts != 1 {
		t.Errorf("Expected 1 put, got %d", stats.TotalPuts)
	}
}

func TestPerPResultPoolHitMissTracking(t *testing.T) {
	// Force single P to ensure sync.Pool deterministic behavior
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cfg := &PerPResultPoolConfig{
		NumShards:   1, // Single shard for deterministic testing
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)

	// First get should be a miss (pool empty)
	slice := pool.Get(10)
	stats := pool.Stats()
	if stats.Misses != 1 {
		t.Errorf("First get should be miss, got misses=%d", stats.Misses)
	}

	// Put it back
	pool.Put(slice)

	// Second get should be a hit, BUT sync.Pool doesn't guarantee retention.
	// Especially with -race, it might be dropped.
	slice = pool.Get(10)
	stats = pool.Stats()
	if stats.Hits != 1 {
		// If it wasn't a hit, it MUST be a miss
		if stats.Misses != 2 {
			t.Errorf("Expected either 1 hit or 2 misses (if pool dropped), got hits=%d misses=%d",
				stats.Hits, stats.Misses)
		} else {
			t.Log("Note: sync.Pool dropped the item (expected behavior under stress/race)")
		}
	}
	pool.Put(slice)
}

func TestPerPResultPoolPerShardStats(t *testing.T) {
	cfg := &PerPResultPoolConfig{
		NumShards:   4,
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)

	// Get per-shard statistics
	perShard := pool.PerShardStats()
	if len(perShard) != 4 {
		t.Errorf("Expected 4 shard stats, got %d", len(perShard))
	}

	// Perform some operations
	for i := 0; i < 10; i++ {
		slice := pool.Get(10)
		pool.Put(slice)
	}

	// Verify some distribution occurred
	perShard = pool.PerShardStats()
	totalFromShards := uint64(0)
	for _, s := range perShard {
		totalFromShards += s.Gets
	}
	if totalFromShards != 10 {
		t.Errorf("Sum of shard gets should be 10, got %d", totalFromShards)
	}
}

// -----------------------------------------------------------------------------
// Test: Nil safety
// -----------------------------------------------------------------------------

func TestPerPResultPoolPutNil(t *testing.T) {
	pool := NewPerPResultPool(nil)

	// Put nil should not panic
	pool.Put(nil)

	stats := pool.Stats()
	if stats.TotalPuts != 0 {
		t.Error("Put(nil) should not count as a put")
	}
}

// -----------------------------------------------------------------------------
// Test: Reset functionality
// -----------------------------------------------------------------------------

func TestPerPResultPoolReset(t *testing.T) {
	cfg := &PerPResultPoolConfig{
		NumShards:   2,
		EnableStats: true,
	}
	pool := NewPerPResultPool(cfg)

	// Perform operations
	for i := 0; i < 100; i++ {
		slice := pool.Get(10)
		pool.Put(slice)
	}

	// Reset stats
	pool.ResetStats()

	stats := pool.Stats()
	if stats.TotalGets != 0 || stats.TotalPuts != 0 {
		t.Error("Stats should be zero after reset")
	}
}

// -----------------------------------------------------------------------------
// Benchmarks: Compare PerP pool vs original sync.Pool
// -----------------------------------------------------------------------------

func BenchmarkPerPResultPoolGet(b *testing.B) {
	pool := NewPerPResultPool(nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slice := pool.Get(100)
			pool.Put(slice)
		}
	})
}

func BenchmarkOriginalResultPoolGet(b *testing.B) {
	pool := newResultPool()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slice := pool.get(100)
			pool.put(slice)
		}
	})
}

func BenchmarkPerPResultPoolContention(b *testing.B) {
	// Test under high contention with many goroutines
	pool := NewPerPResultPool(nil)

	b.ResetTimer()
	b.SetParallelism(100) // High parallelism
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slice := pool.Get(256)
			_ = slice
			pool.Put(slice)
		}
	})
}

func BenchmarkOriginalResultPoolContention(b *testing.B) {
	// Test under high contention with many goroutines
	pool := newResultPool()

	b.ResetTimer()
	b.SetParallelism(100) // High parallelism
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slice := pool.get(256)
			_ = slice
			pool.put(slice)
		}
	})
}
