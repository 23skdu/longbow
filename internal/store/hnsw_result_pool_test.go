package store


import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestResultPoolBasicGet tests basic pool get operation
func TestResultPoolBasicGet(t *testing.T) {
	pool := newResultPool()

	// Get slices for common k values
	res10 := pool.get(10)
	assert.NotNil(t, res10)
	assert.Equal(t, 10, len(res10))
	assert.GreaterOrEqual(t, cap(res10), 10)

	res50 := pool.get(50)
	assert.NotNil(t, res50)
	assert.Equal(t, 50, len(res50))

	res100 := pool.get(100)
	assert.NotNil(t, res100)
	assert.Equal(t, 100, len(res100))
}

// TestResultPoolPutAndReuse tests that slices are reused
func TestResultPoolPutAndReuse(t *testing.T) {
	pool := newResultPool()

	// Get and put back
	res := pool.get(10)
	res[0] = VectorID(42)
	pool.put(res)

	// Get again - should get pooled slice
	res2 := pool.get(10)
	assert.NotNil(t, res2)
	assert.Equal(t, 10, len(res2))
	// Values should be zeroed on reuse
	assert.Equal(t, VectorID(0), res2[0])
}

// TestResultPoolNonStandardK tests fallback for non-pooled k values
func TestResultPoolNonStandardK(t *testing.T) {
	pool := newResultPool()

	// Non-standard k values should still work
	res7 := pool.get(7)
	assert.NotNil(t, res7)
	assert.Equal(t, 7, len(res7))

	res200 := pool.get(200)
	assert.NotNil(t, res200)
	assert.Equal(t, 200, len(res200))
}

// TestResultPoolConcurrentAccess tests thread safety
func TestResultPoolConcurrentAccess(t *testing.T) {
	pool := newResultPool()
	var wg sync.WaitGroup
	iterations := 1000

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				k := []int{10, 50, 100}[j%3]
				res := pool.get(k)
				assert.Equal(t, k, len(res))
				// Write to slice
				for idx := range res {
					res[idx] = VectorID(id*1000 + j)
				}
				pool.put(res)
			}
		}(i)
	}
	wg.Wait()
}

// TestResultPoolZeroK tests edge case of k=0
func TestResultPoolZeroK(t *testing.T) {
	pool := newResultPool()
	res := pool.get(0)
	assert.NotNil(t, res)
	assert.Equal(t, 0, len(res))
}

// BenchmarkResultPoolGet benchmarks pool get performance
func BenchmarkResultPoolGet(b *testing.B) {
	pool := newResultPool()

	b.Run("k=10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res := pool.get(10)
			pool.put(res)
		}
	})

	b.Run("k=50", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res := pool.get(50)
			pool.put(res)
		}
	})

	b.Run("k=100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res := pool.get(100)
			pool.put(res)
		}
	})
}

// BenchmarkResultPoolVsAlloc compares pool vs direct allocation
func BenchmarkResultPoolVsAlloc(b *testing.B) {
	pool := newResultPool()

	b.Run("pool_k=10", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := pool.get(10)
			pool.put(res)
		}
	})

	b.Run("alloc_k=10", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := make([]VectorID, 10)
			_ = res
		}
	})

	b.Run("pool_k=100", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := pool.get(100)
			pool.put(res)
		}
	})

	b.Run("alloc_k=100", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			res := make([]VectorID, 100)
			_ = res
		}
	})
}
