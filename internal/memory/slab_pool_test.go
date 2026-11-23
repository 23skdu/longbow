package memory

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlabPool_BasicOperations runs the test slab pool_basic operations operation.
func TestSlabPool_BasicOperations(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024) // 4MB slabs

	// Get a slab
	slab1 := pool.Get()
	require.Equal(t, 4*1024*1024, cap(slab1))
	assert.Equal(t, int64(1), pool.ActiveCount())
	assert.Equal(t, int64(0), pool.PooledCount())

	// Put it back
	pool.Put(slab1)
	assert.Equal(t, int64(0), pool.ActiveCount())
	assert.Equal(t, int64(1), pool.PooledCount())

	// Get it again (should be reused)
	slab2 := pool.Get()
	assert.Equal(t, int64(1), pool.ActiveCount())
	assert.Equal(t, int64(0), pool.PooledCount())

	pool.Put(slab2)
}

// TestSlabPool_MaxPooledLimit runs the test slab pool_max pooled limit operation.
func TestSlabPool_MaxPooledLimit(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)
	pool.maxPooled = 10 // Set low limit for testing

	// Allocate and return more slabs than maxPooled
	slabs := make([][]byte, 20)
	for i := range slabs {
		slabs[i] = pool.Get()
	}

	// Return all slabs
	for _, slab := range slabs {
		pool.Put(slab)
	}

	// Pool should not exceed maxPooled
	assert.LessOrEqual(t, pool.PooledCount(), pool.maxPooled)
	assert.Equal(t, int64(0), pool.ActiveCount())
}

// TestSlabPool_MaxPooledDoesNotExpandUnderLoad prevents the old dynamic
// scale-up path from retaining thousands of 4 MB slabs after a large test.
func TestSlabPool_MaxPooledDoesNotExpandUnderLoad(t *testing.T) {
	pool := newSlabPool(1024 * 1024)

	slabs := make([][]byte, maxPooledSlabLimit+25)
	for i := range slabs {
		slabs[i] = pool.Get()
	}
	for _, slab := range slabs {
		pool.Put(slab)
	}

	assert.Equal(t, int64(maxPooledSlabLimit), atomic.LoadInt64(&pool.maxPooled))
	assert.LessOrEqual(t, pool.PooledCount(), int64(maxPooledSlabLimit))
	assert.Equal(t, int64(0), pool.ActiveCount())
}

// TestSlabPool_ReleaseUnused runs the test slab pool_release unused operation.
func TestSlabPool_ReleaseUnused(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)
	pool.maxPooled = 100

	// Allocate and return many slabs
	slabs := make([][]byte, 80)
	for i := range slabs {
		slabs[i] = pool.Get()
	}
	for _, slab := range slabs {
		pool.Put(slab)
	}

	initialPooled := pool.PooledCount()
	require.Greater(t, initialPooled, int64(0))

	// Force GC to get accurate memory stats
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	// Release unused slabs
	released := pool.ReleaseUnused()
	t.Logf("Released %d slabs", released)

	// Should have released some slabs
	assert.Greater(t, released, 0)
	assert.Less(t, pool.PooledCount(), initialPooled)

	// Force GC again to allow OS to reclaim
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
}

// TestSlabPool_ConcurrentAccess runs the test slab pool_concurrent access operation.
func TestSlabPool_ConcurrentAccess(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)

	const goroutines = 10
	const iterations = 100

	done := make(chan bool, goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < iterations; i++ {
				slab := pool.Get()
				// Simulate some work
				slab[0] = byte(i)
				pool.Put(slab)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for g := 0; g < goroutines; g++ {
		<-done
	}

	// All slabs should be returned
	assert.Equal(t, int64(0), pool.ActiveCount())
}

// TestGetSlab_StandardSizes runs the test get slab_standard sizes operation.
func TestGetSlab_StandardSizes(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"4MB", 4 * 1024 * 1024},
		{"8MB", 8 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
		{"32MB", 32 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slab := GetSlab(tt.size)
			require.Equal(t, tt.size, cap(slab))

			// Return it
			PutSlab(slab)

			// Get it again to verify pooling works
			slab2 := GetSlab(tt.size)
			require.Equal(t, tt.size, cap(slab2))
			PutSlab(slab2)
		})
	}
}

// TestGetSlab_NonStandardSize runs the test get slab_non standard size operation.
func TestGetSlab_NonStandardSize(t *testing.T) {
	// Non-standard size should be rounded up to the next standard pool size (8MB)
	// but the returned slice should have len == requested size.
	size := 5 * 1024 * 1024 // 5MB
	slab := GetSlab(size)
	require.Equal(t, size, len(slab))
	require.GreaterOrEqual(t, cap(slab), size)

	// Putting it back should recycle to the 8MB pool (no boundary violation)
	PutSlab(slab)
}

// TestGetSlab_SmallCapacityUsesHeap ensures small scratch allocations are not
// promoted to a full 4 MB off-heap slab.
func TestGetSlab_SmallCapacityUsesHeap(t *testing.T) {
	before := global4MBPool.ActiveCount()

	slab := GetSlab(4096)
	require.Equal(t, 4096, len(slab))
	require.Equal(t, 4096, cap(slab))
	assert.Equal(t, before, global4MBPool.ActiveCount())

	PutSlab(slab)
	assert.Equal(t, before, global4MBPool.ActiveCount())
}

// TestSlabArena_SmallSlabDoesNotGrowPool verifies that an arena configured with
// kilobyte-scale slabs does not retain gigabytes via the standard slab pools.
func TestSlabArena_SmallSlabDoesNotGrowPool(t *testing.T) {
	before := global4MBPool.ActiveCount()

	arena := NewSlabArena(1024)
	for i := 0; i < 1000; i++ {
		_, err := arena.Alloc(4096)
		require.NoError(t, err)
	}
	assert.Equal(t, before, global4MBPool.ActiveCount())

	arena.Free()
	assert.Equal(t, before, global4MBPool.ActiveCount())
}

// BenchmarkSlabPool_GetPut runs the benchmark slab pool_get put operation.
func BenchmarkSlabPool_GetPut(b *testing.B) {
	pool := newSlabPool(4 * 1024 * 1024)

	for b.Loop() {
		slab := pool.Get()
		pool.Put(slab)
	}
}

// BenchmarkSlabPool_Parallel runs the benchmark slab pool_parallel operation.
func BenchmarkSlabPool_Parallel(b *testing.B) {
	pool := newSlabPool(4 * 1024 * 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			slab := pool.Get()
			pool.Put(slab)
		}
	})
}

// TestSlabPool_ReslicingProtection runs the test slab pool_reslicing protection operation.
func TestSlabPool_ReslicingProtection(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)

	slab := pool.Get()
	require.Equal(t, 4*1024*1024, len(slab))

	// Slice it to a smaller size
	sliced := slab[:1024]
	require.Equal(t, 1024, len(sliced))

	// Put it back
	pool.Put(sliced)

	// Get it again
	reclaimed := pool.Get()
	// Length should be restored to its full capacity!
	require.Equal(t, 4*1024*1024, len(reclaimed))
}
