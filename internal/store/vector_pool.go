package store

import (
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
func (vp *VectorPool) Get(dim int) []float32 {
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
					return make([]float32, dim)
				},
			}
			vp.pools[dim] = pool
		}
		vp.mu.Unlock()
	}

	vec := pool.Get().([]float32)
	// Ensure the slice is the correct length
	if len(vec) != dim {
		vp.misses.Add(1)
		vec = make([]float32, dim)
	} else {
		vp.hits.Add(1)
	}
	return vec
}

// Put returns a vector buffer to the pool for reuse
func (vp *VectorPool) Put(vec []float32) {
	if vec == nil || len(vec) == 0 {
		return
	}

	dim := len(vec)
	vp.mu.RLock()
	pool, exists := vp.pools[dim]
	vp.mu.RUnlock()

	if exists {
		// Clear the vector before returning to pool
		for i := range vec {
			vec[i] = 0
		}
		vp.puts.Add(1)
		pool.Put(vec)
	}
}

// Stats returns pool statistics
func (vp *VectorPool) Stats() (hits, misses, puts int64) {
	return vp.hits.Load(), vp.misses.Load(), vp.puts.Load()
}

// vectorView provides zero-copy read-only access to a vector in an Arrow array
type vectorView struct {
	data  []float32
	start int
	end   int
}

// Get returns the vector as a slice (zero-copy, read-only)
func (v *vectorView) Get() []float32 {
	return v.data[v.start:v.end]
}

// CopyTo copies the vector data into the provided destination slice
func (v *vectorView) CopyTo(dst []float32) {
	copy(dst, v.data[v.start:v.end])
}

// extractVectorView returns a zero-copy view of the vector at the specified row
// WARNING: The returned view is only valid while the RecordBatch is retained
// and must not be modified. Use extractVector() if you need to store the vector.
func extractVectorView(listArr *array.FixedSizeList, rowIdx int) (*vectorView, error) {
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return nil, ErrRowIndexOutOfBounds
	}

	return &vectorView{
		data:  floatArr.Float32Values(),
		start: start,
		end:   end,
	}, nil
}
