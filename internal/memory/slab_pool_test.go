package memory

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestGetSlab_NonStandardSize(t *testing.T) {
	// Non-standard size should still work but won't be pooled
	size := 5 * 1024 * 1024 // 5MB
	slab := GetSlab(size)
	require.Equal(t, size, cap(slab))

	// Putting it back should be a no-op
	PutSlab(slab)
}

func BenchmarkSlabPool_GetPut(b *testing.B) {
	pool := newSlabPool(4 * 1024 * 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slab := pool.Get()
		pool.Put(slab)
	}
}

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
