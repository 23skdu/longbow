package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Subtask 1: ShardedRWMutex Tests (TDD Red Phase)
// =============================================================================

// --- Config Tests ---

func TestShardedRWMutexConfigDefaults(t *testing.T) {
	cfg := DefaultShardedRWMutexConfig()

	if cfg.NumShards <= 0 {
		t.Error("NumShards should be positive")
	}
	if cfg.NumShards != runtime.NumCPU() {
		t.Errorf("NumShards should default to NumCPU(), got %d, want %d", cfg.NumShards, runtime.NumCPU())
	}
}

func TestShardedRWMutexConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ShardedRWMutexConfig
		wantErr bool
	}{
		{"valid default", DefaultShardedRWMutexConfig(), false},
		{"valid custom shards", ShardedRWMutexConfig{NumShards: 16}, false},
		{"valid single shard", ShardedRWMutexConfig{NumShards: 1}, false},
		{"invalid zero shards", ShardedRWMutexConfig{NumShards: 0}, true},
		{"invalid negative shards", ShardedRWMutexConfig{NumShards: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// --- Creation Tests ---

func TestNewShardedRWMutex(t *testing.T) {
	cfg := ShardedRWMutexConfig{NumShards: 8}
	sm := NewShardedRWMutex(cfg)

	if sm == nil {
		t.Fatal("NewShardedRWMutex returned nil")
	}
	if sm.NumShards() != 8 {
		t.Errorf("NumShards() = %d, want 8", sm.NumShards())
	}
}

func TestNewShardedRWMutexDefault(t *testing.T) {
	sm := NewShardedRWMutexDefault()

	if sm == nil {
		t.Fatal("NewShardedRWMutexDefault returned nil")
	}
	if sm.NumShards() != runtime.NumCPU() {
		t.Errorf("NumShards() = %d, want %d", sm.NumShards(), runtime.NumCPU())
	}
}

// --- Shard Routing Tests ---

func TestShardedRWMutexShardSelection(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	// Same key should always route to same shard
	shard1 := sm.ShardFor(12345)
	shard2 := sm.ShardFor(12345)
	if shard1 != shard2 {
		t.Errorf("Same key routed to different shards: %d vs %d", shard1, shard2)
	}

	// Shard should be within bounds
	for i := uint64(0); i < 1000; i++ {
		shard := sm.ShardFor(i)
		if shard < 0 || shard >= 4 {
			t.Errorf("ShardFor(%d) = %d, out of bounds [0,4)", i, shard)
		}
	}
}

func TestShardedRWMutexDistribution(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 8})

	// Count distribution across shards
	counts := make([]int, 8)
	for i := uint64(0); i < 10000; i++ {
		shard := sm.ShardFor(i)
		counts[shard]++
	}

	// Each shard should have at least some keys (rough balance)
	for i, count := range counts {
		if count < 500 { // At least 5% of keys
			t.Errorf("Shard %d has poor distribution: %d keys", i, count)
		}
	}
}

// --- Lock/Unlock Tests ---

func TestShardedRWMutexLockUnlock(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	// Basic lock/unlock should not deadlock
	sm.Lock(100)
	sm.Unlock(100)

	// Should be able to lock again after unlock
	sm.Lock(100)
	sm.Unlock(100)
}

func TestShardedRWMutexRLockRUnlock(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	// Basic RLock/RUnlock should not deadlock
	sm.RLock(100)
	sm.RUnlock(100)

	// Multiple readers on same key should work
	sm.RLock(100)
	sm.RLock(100)
	sm.RUnlock(100)
	sm.RUnlock(100)
}

func TestShardedRWMutexDifferentShardsConcurrent(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	// Two different keys in different shards should lock concurrently
	// Find two keys that hash to different shards
	var key1, key2 uint64 = 0, 1
	for sm.ShardFor(key2) == sm.ShardFor(key1) {
		key2++
	}

	// Lock first key
	sm.Lock(key1)

	// Should be able to lock second key immediately (different shard)
	done := make(chan bool, 1)
	go func() {
		sm.Lock(key2)
		done <- true
		sm.Unlock(key2)
	}()

	select {
	case <-done:
	// Good - second lock acquired
	case <-time.After(100 * time.Millisecond):
		t.Error("Lock on different shard should not block")
	}

	sm.Unlock(key1)
}

// --- Concurrent Safety Tests ---

func TestShardedRWMutexConcurrentWrites(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 8})

	var counter int64
	var wg sync.WaitGroup

	// Multiple goroutines writing with locks
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sm.Lock(key)
				atomic.AddInt64(&counter, 1)
				sm.Unlock(key)
			}
		}(uint64(i % 8)) // Spread across shards
	}

	wg.Wait()

	if counter != 10000 {
		t.Errorf("Counter = %d, want 10000", counter)
	}
}

func TestShardedRWMutexConcurrentReadsWrites(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	var data int64
	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sm.Lock(key)
				atomic.StoreInt64(&data, int64(j))
				sm.Unlock(key)
			}
		}(uint64(i % 4))
	}

	// Readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sm.RLock(key)
				_ = atomic.LoadInt64(&data)
				sm.RUnlock(key)
			}
		}(uint64(i % 4))
	}

	wg.Wait()
	// If we get here without deadlock or panic, test passes
}

// --- Statistics Tests ---

func TestShardedRWMutexStats(t *testing.T) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: 4})

	// Perform some operations
	for i := uint64(0); i < 100; i++ {
		sm.Lock(i)
		sm.Unlock(i)
		sm.RLock(i)
		sm.RUnlock(i)
	}

	stats := sm.Stats()

	if stats.TotalLocks != 100 {
		t.Errorf("TotalLocks = %d, want 100", stats.TotalLocks)
	}
	if stats.TotalRLocks != 100 {
		t.Errorf("TotalRLocks = %d, want 100", stats.TotalRLocks)
	}
}

// --- Benchmark Tests ---

func BenchmarkShardedRWMutexLock(b *testing.B) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: runtime.NumCPU()})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := uint64(0)
		for pb.Next() {
			sm.Lock(key)
			sm.Unlock(key)
			key++
		}
	})
}

func BenchmarkShardedRWMutexRLock(b *testing.B) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{NumShards: runtime.NumCPU()})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := uint64(0)
		for pb.Next() {
			sm.RLock(key)
			sm.RUnlock(key)
			key++
		}
	})
}

func BenchmarkShardedVsSingleMutex(b *testing.B) {
	// Compare sharded vs single mutex under contention
	b.Run("SingleMutex", func(b *testing.B) {
		var mu sync.RWMutex
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.Lock() //nolint:gocritic // intentional for benchmark
				_ = 0
				mu.Unlock() //nolint:gocritic,staticcheck // benchmark: intentional empty critical section
			}
		})
	})

	b.Run("ShardedMutex", func(b *testing.B) {
		sm := NewShardedRWMutexDefault()
		b.RunParallel(func(pb *testing.PB) {
			key := uint64(0)
			for pb.Next() {
				sm.Lock(key)
				sm.Unlock(key)
				key++
			}
		})
	})
}
