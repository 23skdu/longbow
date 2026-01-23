package store

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// TestAlignedShardedMutex_CacheLineAlignment verifies that each shard is
// properly aligned to prevent false sharing.
func TestAlignedShardedMutex_CacheLineAlignment(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 16,
	})

	// Verify each shard is on a separate cache line (64 bytes)
	for i := 0; i < sm.NumShards()-1; i++ {
		addr1 := uintptr(unsafe.Pointer(&sm.shards[i]))
		addr2 := uintptr(unsafe.Pointer(&sm.shards[i+1]))

		// Distance should be at least 64 bytes (cache line size)
		distance := addr2 - addr1
		assert.GreaterOrEqual(t, distance, uintptr(64),
			"Shards %d and %d are not cache-line aligned (distance: %d bytes)", i, i+1, distance)
	}
}

// TestAlignedShardedMutex_BasicLocking verifies basic lock/unlock functionality.
func TestAlignedShardedMutex_BasicLocking(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 8,
	})

	// Test write lock
	key := uint64(42)
	sm.Lock(key)
	sm.Unlock(key)

	// Test read lock
	sm.RLock(key)
	sm.RUnlock(key)

	// Test multiple keys
	for i := uint64(0); i < 100; i++ {
		sm.Lock(i)
		sm.Unlock(i)
	}
}

// TestAlignedShardedMutex_ConcurrentAccess tests concurrent lock acquisitions.
func TestAlignedShardedMutex_ConcurrentAccess(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 64,
	})

	var wg sync.WaitGroup
	var counter atomic.Int64

	// 100 goroutines incrementing counter
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				key := uint64(id*1000 + j)
				sm.Lock(key)
				counter.Add(1)
				sm.Unlock(key)
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int64(100000), counter.Load())
}

// TestAlignedShardedMutex_FastPath verifies the lock-free fast path works.
func TestAlignedShardedMutex_FastPath(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 8,
	})

	key := uint64(42)

	// First acquisition should use fast path
	acquired := sm.TryLock(key)
	assert.True(t, acquired, "Fast path should succeed when uncontended")
	sm.Unlock(key)

	// Verify contention detection
	sm.Lock(key)

	// Try from another goroutine (should fail)
	done := make(chan bool)
	go func() {
		acquired := sm.TryLock(key)
		done <- acquired
	}()

	result := <-done
	assert.False(t, result, "Fast path should fail when lock is held")

	sm.Unlock(key)
}

// TestAlignedShardedMutex_ContentionMetrics verifies contention tracking.
func TestAlignedShardedMutex_ContentionMetrics(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 4,
	})

	// Create contention by having multiple goroutines compete for same shard
	var wg sync.WaitGroup
	key := uint64(0) // All goroutines use same key (same shard)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sm.Lock(key)
				time.Sleep(time.Microsecond) // Hold lock briefly
				sm.Unlock(key)
			}
		}()
	}

	wg.Wait()

	// Check that contention was detected
	stats := sm.Stats()
	assert.Greater(t, stats.TotalContentions, int64(0), "Should detect contention")
	assert.Greater(t, stats.TotalWaitNs, int64(0), "Should measure wait time")
}

// TestAlignedShardedMutex_AdaptiveScaling tests adaptive shard count adjustment.
func TestAlignedShardedMutex_AdaptiveScaling(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards:        8,
		TargetContention: 10.0, // Low threshold for testing
		EnableAdaptive:   true,
	})

	initialShards := sm.NumShards()

	// Create high contention
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// All goroutines compete for same shard
			key := uint64(0)
			for j := 0; j < 100; j++ {
				sm.Lock(key)
				time.Sleep(time.Microsecond)
				sm.Unlock(key)
			}
		}(i)
	}

	wg.Wait()

	// Trigger adaptive scaling
	sm.AdaptShardCount()

	// Should have increased shard count
	newShards := sm.NumShards()
	if sm.config.EnableAdaptive {
		assert.Greater(t, newShards, initialShards,
			"Should increase shards under high contention")
	}
}

// TestAlignedShardedMutex_ShardDistribution verifies even distribution across shards.
func TestAlignedShardedMutex_ShardDistribution(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 16,
	})

	// Count how many keys map to each shard
	shardCounts := make(map[int]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		shard := sm.ShardFor(uint64(i))
		shardCounts[shard]++
	}

	// Each shard should get roughly equal distribution
	expectedPerShard := numKeys / sm.NumShards()
	tolerance := expectedPerShard / 5 // 20% tolerance

	for shard, count := range shardCounts {
		assert.InDelta(t, expectedPerShard, count, float64(tolerance),
			"Shard %d has uneven distribution: %d keys (expected ~%d)",
			shard, count, expectedPerShard)
	}
}

// TestAlignedShardedMutex_RWLockCorrectness tests read/write lock semantics.
func TestAlignedShardedMutex_RWLockCorrectness(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 8,
	})

	key := uint64(42)
	value := int64(0)

	var wg sync.WaitGroup

	// Multiple readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sm.RLock(key)
				_ = atomic.LoadInt64(&value)
				sm.RUnlock(key)
			}
		}()
	}

	// Single writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 100; j++ {
			sm.Lock(key)
			atomic.AddInt64(&value, 1)
			sm.Unlock(key)
		}
	}()

	wg.Wait()
	assert.Equal(t, int64(100), atomic.LoadInt64(&value))
}

// BenchmarkAlignedShardedMutex_Lock measures lock acquisition performance.
func BenchmarkAlignedShardedMutex_Lock(b *testing.B) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 64,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := uint64(i)
		sm.Lock(key)
		sm.Unlock(key)
	}
}

// BenchmarkAlignedShardedMutex_RLock measures read lock performance.
func BenchmarkAlignedShardedMutex_RLock(b *testing.B) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 64,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := uint64(i)
		sm.RLock(key)
		sm.RUnlock(key)
	}
}

// BenchmarkAlignedShardedMutex_Contention measures performance under contention.
func BenchmarkAlignedShardedMutex_Contention(b *testing.B) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 64,
	})

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			sm.Lock(i)
			i++
			sm.Unlock(i - 1)
		}
	})
}

// BenchmarkStandardShardedMutex_Contention compares with existing implementation.
func BenchmarkStandardShardedMutex_Contention(b *testing.B) {
	sm := NewShardedRWMutex(ShardedRWMutexConfig{
		NumShards: 64,
	})

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			sm.Lock(i)
			i++
			sm.Unlock(i - 1)
		}
	})
}

// TestAlignedShardedMutex_MemoryFootprint verifies memory usage is reasonable.
func TestAlignedShardedMutex_MemoryFootprint(b *testing.T) {
	configs := []int{8, 64, 256, 1024}

	for _, numShards := range configs {
		sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
			NumShards: numShards,
		})

		// Rough estimate: each shard should be ~64 bytes (cache line)
		// Plus overhead for struct and slice
		expectedMax := numShards*128 + 1024 // Conservative estimate

		actualSize := int(unsafe.Sizeof(*sm)) + len(sm.shards)*int(unsafe.Sizeof(sm.shards[0]))

		b.Logf("NumShards: %d, Size: %d bytes, Expected max: %d bytes",
			numShards, actualSize, expectedMax)

		assert.Less(b, actualSize, expectedMax,
			"Memory footprint too large for %d shards", numShards)
	}
}

// TestAlignedShardedMutex_NoDeadlock verifies no deadlocks occur.
func TestAlignedShardedMutex_NoDeadlock(t *testing.T) {
	sm := NewAlignedShardedMutex(AlignedShardedMutexConfig{
		NumShards: 16,
	})

	done := make(chan bool, 1)

	go func() {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					key := uint64(id*100 + j)
					sm.Lock(key)
					runtime.Gosched() // Yield to increase chance of contention
					sm.Unlock(key)
				}
			}(i)
		}
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success - no deadlock
	case <-time.After(10 * time.Second):
		t.Fatal("Deadlock detected - test timed out")
	}
}
