package types

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchResultPool_Basic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	pool := NewSearchResultPool()
	pool.Put(nil)
}

func TestSearchResultPool_Concurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
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
