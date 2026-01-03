package store

import (
	"sync"
	"sync/atomic"
)

// VectorPool manages reusable float32 slices to reduce allocation churn
type VectorPool struct {
	pools map[int]*sync.Pool // keyed by vector dimension
	mu    sync.RWMutex

	// Metrics
	hits   atomic.Int64 // Pool hits (reused vectors)
	misses atomic.Int64 // Pool misses (new allocations)
	puts   atomic.Int64 // Vectors returned to pool
}

// NewVectorPool creates a new vector pool
func NewVectorPool() *VectorPool {
	return &VectorPool{
		pools: make(map[int]*sync.Pool),
	}
}

// Get retrieves a vector buffer of the specified dimension from the pool
func (vp *VectorPool) Get(dim int) *[]float32 {
	vp.mu.RLock()
	pool, exists := vp.pools[dim]
	vp.mu.RUnlock()

	if !exists {
		vp.mu.Lock()
		// Double-check after acquiring write lock
		pool, exists = vp.pools[dim]
		if !exists {
			pool = &sync.Pool{
				New: func() interface{} {
					vp.misses.Add(1)
					slice := make([]float32, dim)
					return &slice
				},
			}
			vp.pools[dim] = pool
		}
		vp.mu.Unlock()
	}

	vec := pool.Get().(*[]float32)
	// Ensure the slice is the correct length (sanity check, usually guaranteed by logic)
	if len(*vec) != dim {
		vp.misses.Add(1)
		slice := make([]float32, dim)
		vec = &slice
	} else {
		vp.hits.Add(1)
	}
	return vec
}

// Put returns a vector buffer to the pool for reuse
func (vp *VectorPool) Put(vec *[]float32) {
	if vec == nil || len(*vec) == 0 {
		return
	}

	dim := len(*vec)
	vp.mu.RLock()
	pool, exists := vp.pools[dim]
	vp.mu.RUnlock()

	if exists {
		// Clear the vector before returning to pool
		slice := *vec
		for i := range slice {
			slice[i] = 0
		}
		vp.puts.Add(1)
		pool.Put(vec)
	}
}

// Stats returns pool statistics
func (vp *VectorPool) Stats() (hits, misses, puts int64) {
	return vp.hits.Load(), vp.misses.Load(), vp.puts.Load()
}
