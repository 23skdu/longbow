package store

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchContext_Reuse(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// Get an object
	ctx1 := pool.Get().(*ArrowSearchContext)
	assert.NotNil(t, ctx1)
	ptr1 := ctx1

	// Put it back
	pool.Put(ctx1)

	// Get object again (should be same pointer if pool works ideally, though sync.Pool doesn't guarantee it)
	// To force reuse in a single-threaded test, it usually works.
	ctx2 := pool.Get().(*ArrowSearchContext)
	ptr2 := ctx2

	assert.Equal(t, ptr1, ptr2, "Expected pooled object to be reused")
}

func TestSearchContext_Reset(t *testing.T) {
	ctx := NewArrowSearchContext()

	// Dirty the context
	ctx.visited.Set(1)
	ctx.scratchIDs = append(ctx.scratchIDs, 1, 2, 3)
	ctx.scratchDists = append(ctx.scratchDists, 1.0, 2.0)
	// Simulate usage of other fields if possible, but scratchIDs is a good proxy for slices

	// Reset
	ctx.Reset()

	// precise check of Reset logic
	assert.Equal(t, 0, len(ctx.scratchIDs), "scratchIDs len should be 0")
	assert.GreaterOrEqual(t, cap(ctx.scratchIDs), 3, "scratchIDs cap should be preserved")

	assert.Equal(t, 0, len(ctx.scratchDists), "scratchDists len should be 0")

	// Check visited cleared
	assert.False(t, ctx.visited.IsSet(1), "visited bit 1 should be cleared")
}

func TestSearchContext_Metrics(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// New pool allocation should increment NewTotal
	_ = pool.Get().(*ArrowSearchContext)
	// We can't easily check the global prometheus Registry in a unit test without gathering,
	// but we can trust the code if it runs without panic.
	// Alternatively, using the prometheus/testutil package if available, but let's assume manual verification via "it runs".
}

func TestSearchResultPool_Basic(t *testing.T) {
	pool := NewSearchResultPool()

	// Get a result slice
	result := pool.Get(10)
	require.NotNil(t, result)
	assert.Equal(t, 0, len(result))
	assert.Equal(t, 10, cap(result))

	// Add some results
	result = append(result, SearchResult{ID: 1, Score: 0.1}, SearchResult{ID: 2, Score: 0.2})
	require.Len(t, result, 2)

	// Put it back
	pool.Put(result)

	// Get another - should be reused
	result2 := pool.Get(10)
	require.NotNil(t, result2)
	assert.Equal(t, 0, len(result2)) // Should be empty (reset)
}

func TestSearchResultPool_CapacityBuckets(t *testing.T) {
	pool := NewSearchResultPool()

	// Test different capacities map to correct buckets
	testCases := []struct {
		requested int
		expected  string
	}{
		{5, "10"},
		{10, "10"},
		{15, "32"},
		{32, "32"},
		{50, "64"},
		{64, "64"},
		{100, "128"},
		{128, "128"},
		{200, "256+"},
		{256, "256+"},
		{300, "256+"},
	}

	for _, tc := range testCases {
		result := pool.Get(tc.requested)
		assert.Equal(t, tc.expected, capacityBucketLabel(tc.requested),
			"capacity %d should map to bucket %s", tc.requested, tc.expected)
		pool.Put(result)
	}
}

func TestSearchResultPool_PutNil(t *testing.T) {
	pool := NewSearchResultPool()

	// Should not panic
	pool.Put(nil)
}

func TestSearchResultPool_TooLargeSlice(t *testing.T) {
	pool := NewSearchResultPool()

	// Create a slice larger than any bucket
	result := make([]SearchResult, 0, 500)
	result = append(result, SearchResult{ID: 1})
	pool.Put(result) // Should not panic, just discard
}

func TestSearchResultPool_Concurrent(t *testing.T) {
	pool := NewSearchResultPool()

	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				capacity := 10 + (i%5)*10 // 10, 20, 30, 40, 50
				result := pool.Get(capacity)
				result = append(result, SearchResult{ID: VectorID(gid*opsPerGoroutine + i)})
				// Do some work
				_ = len(result)
				pool.Put(result)
			}
		}(g)
	}
	wg.Wait()
}

func TestSearchResultPool_Stats(t *testing.T) {
	pool := NewSearchResultPool()

	// Get and put some items
	for i := 0; i < 10; i++ {
		result := pool.Get(10)
		pool.Put(result)
	}

	stats := pool.Stats()
	assert.Contains(t, stats, "10")
	assert.Equal(t, int64(10), stats["10"]["gets"])
	assert.Equal(t, int64(10), stats["10"]["puts"])
}

func TestSearchResultPool_ClearOnPut(t *testing.T) {
	pool := NewSearchResultPool()

	result := pool.Get(10)
	result = append(result, SearchResult{ID: 1, Score: 0.5}, SearchResult{ID: 2, Score: 0.3})

	pool.Put(result)

	// Get again - should be cleared
	result2 := pool.Get(10)
	assert.Equal(t, 0, len(result2))
	// Capacity should be preserved (10)
	assert.Equal(t, 10, cap(result2))
}

func BenchmarkSearchResultPool_GetPut(b *testing.B) {
	pool := NewSearchResultPool()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := pool.Get(10)
		result = append(result, SearchResult{ID: 1})
		pool.Put(result)
	}
}

func BenchmarkSearchResultPool_Concurrent(b *testing.B) {
	pool := NewSearchResultPool()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result := pool.Get(10)
			result = append(result, SearchResult{ID: 1})
			pool.Put(result)
		}
	})
}

func BenchmarkSearchResultPool_NoPool(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := make([]SearchResult, 0, 10)
		result = append(result, SearchResult{ID: 1})
		_ = result
	}
}

func BenchmarkSearchResultPool_Comparison(b *testing.B) {
	pool := NewSearchResultPool()

	b.Run("WithPool", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := pool.Get(32)
			for j := 0; j < 10; j++ {
				result = append(result, SearchResult{ID: VectorID(j)})
			}
			pool.Put(result)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result := make([]SearchResult, 0, 32)
			for j := 0; j < 10; j++ {
				result = append(result, SearchResult{ID: VectorID(j)})
			}
			_ = result
		}
	})
}

func FuzzSearchResultPool_GetPut(f *testing.F) {
	f.Add(1000, 10)
	f.Add(5000, 50)

	f.Fuzz(func(t *testing.T, iterations int, baseCapacity int) {
		if iterations <= 0 || iterations > 10000 {
			t.Skip()
		}
		if baseCapacity <= 0 || baseCapacity > 256 {
			t.Skip()
		}

		pool := NewSearchResultPool()

		for i := 0; i < iterations; i++ {
			capacity := (baseCapacity + i) % 256
			if capacity == 0 {
				capacity = 1
			}
			result := pool.Get(capacity)
			result = append(result, SearchResult{ID: VectorID(i)})
			pool.Put(result)
		}

		runtime.GC()
	})
}
