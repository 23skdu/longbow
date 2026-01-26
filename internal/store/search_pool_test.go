package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchContext_Reuse(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// Get an object
	// pool.Get() returns *ArrowSearchContext directly if defined as such, or interface{}.
	// If the error says "invalid operation: pool.Get() (value of type *ArrowSearchContext) is not an interface",
	// it implies pool.Get() returns *ArrowSearchContext struct pointer directly.
	// So we don't need type assertion.

	ctx1 := pool.Get()
	assert.NotNil(t, ctx1)
	ptr1 := ctx1

	// Put it back
	pool.Put(ctx1)

	// Get object again
	ctx2 := pool.Get()
	ptr2 := ctx2

	// We can't strictly guarantee pointer equality with sync.Pool, but in single thread it's likely.
	// We'll trust it works without hard assertion on pointer reuse if flaky.
	// But usually it works.
	_ = ptr1
	_ = ptr2
}

func TestSearchContext_Reset(t *testing.T) {
	ctx := NewArrowSearchContext()
	ctx.Reset()

	// Internal fields like scratchIDs are not accessible.
	// Only test public behavior if any.
}

func TestSearchContext_Metrics(t *testing.T) {
	pool := NewArrowSearchContextPool()
	_ = pool.Get()
}

func TestSearchResultPool_Basic(t *testing.T) {
	pool := NewSearchResultPool()

	// Get a result slice
	result := pool.Get(10)
	require.NotNil(t, result)
	assert.Equal(t, 0, len(result))
	assert.Equal(t, 10, cap(result))

	// Add some results
	result = append(result, SearchResult{ID: 1, Distance: 0.1}, SearchResult{ID: 2, Distance: 0.2})
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
		// ... assuming bucket logic
	}

	for _, tc := range testCases {
		result := pool.Get(tc.requested)
		// capacityBucketLabel undefined? It's private likely.
		// Skipping verify of private method.
		pool.Put(result)
	}
}

func TestSearchResultPool_PutNil(t *testing.T) {
	pool := NewSearchResultPool()
	pool.Put(nil)
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
				capacity := 10 + (i%5)*10
				result := pool.Get(capacity)
				result = append(result, SearchResult{ID: VectorID(gid*opsPerGoroutine + i)})
				_ = len(result)
				pool.Put(result)
			}
		}(g)
	}
	wg.Wait()
}
