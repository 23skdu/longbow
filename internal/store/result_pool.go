package store

import "sync"

// resultPool manages pools of []VectorID slices for common k values.
// This reduces allocations during search operations.
// Expanded to support common ML workload sizes:
// - k=10: default small queries
// - k=20: reranking workloads
// - k=50: medium queries
// - k=100: standard retrieval
// - k=256: retrieval augmented generation (RAG)
// - k=1000: batch processing
type resultPool struct {
	pool10   sync.Pool // for k=10
	pool20   sync.Pool // for k=20 (common for reranking)
	pool50   sync.Pool // for k=50
	pool100  sync.Pool // for k=100
	pool256  sync.Pool // for k=256 (retrieval augmented generation)
	pool1000 sync.Pool // for k=1000 (batch processing)
}

// newResultPool creates a new result pool instance.
func newResultPool() *resultPool {
	return &resultPool{
		pool10: sync.Pool{
			New: func() any {
				s := make([]VectorID, 10)
				return &s
			},
		},
		pool20: sync.Pool{
			New: func() any {
				s := make([]VectorID, 20)
				return &s
			},
		},
		pool50: sync.Pool{
			New: func() any {
				s := make([]VectorID, 50)
				return &s
			},
		},
		pool100: sync.Pool{
			New: func() any {
				s := make([]VectorID, 100)
				return &s
			},
		},
		pool256: sync.Pool{
			New: func() any {
				s := make([]VectorID, 256)
				return &s
			},
		},
		pool1000: sync.Pool{
			New: func() any {
				s := make([]VectorID, 1000)
				return &s
			},
		},
	}
}

// get retrieves a []VectorID slice of the specified length.
// For common k values (10, 20, 50, 100, 256, 1000), slices are pooled.
// For other values, the next larger pool is used and resliced.
// For k > 1000, a new slice is allocated.
func (p *resultPool) get(k int) []VectorID {
	var slice []VectorID

	switch {
	// Exact matches for pooled sizes
	case k == 10:
		ptr := p.pool10.Get().(*[]VectorID)
		slice = *ptr
	case k == 20:
		ptr := p.pool20.Get().(*[]VectorID)
		slice = *ptr
	case k == 50:
		ptr := p.pool50.Get().(*[]VectorID)
		slice = *ptr
	case k == 100:
		ptr := p.pool100.Get().(*[]VectorID)
		slice = *ptr
	case k == 256:
		ptr := p.pool256.Get().(*[]VectorID)
		slice = *ptr
	case k == 1000:
		ptr := p.pool1000.Get().(*[]VectorID)
		slice = *ptr
	// Reslice from next larger pool
	case k < 10:
		ptr := p.pool10.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	case k < 20:
		ptr := p.pool20.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	case k < 50:
		ptr := p.pool50.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	case k < 100:
		ptr := p.pool100.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	case k < 256:
		ptr := p.pool256.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	case k < 1000:
		ptr := p.pool1000.Get().(*[]VectorID)
		slice = (*ptr)[:k]
	default:
		// Allocate for k > 1000
		slice = make([]VectorID, k)
		return slice
	}

	// Zero the slice to prevent data leaks
	for i := range slice {
		slice[i] = 0
	}
	return slice
}

// put returns a []VectorID slice to the appropriate pool.
func (p *resultPool) put(slice []VectorID) {
	if slice == nil {
		return
	}

	// Restore full capacity before returning
	capacity := cap(slice)
	slice = slice[:capacity]

	switch capacity {
	case 10:
		p.pool10.Put(&slice)
	case 20:
		p.pool20.Put(&slice)
	case 50:
		p.pool50.Put(&slice)
	case 100:
		p.pool100.Put(&slice)
	case 256:
		p.pool256.Put(&slice)
	case 1000:
		p.pool1000.Put(&slice)
		// Non-standard capacities are not pooled, let GC handle them
	}
}
