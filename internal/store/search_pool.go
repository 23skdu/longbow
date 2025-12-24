package store

import (
	"sync"
	"sync/atomic"
)

// SearchContext holds pre-allocated buffers for HNSW search operations
// to reduce allocation churn during high-frequency searches
type SearchContext struct {
	// Pre-allocated result buffer
	results []SearchResult
	
	// Batch processing buffers (batchSize = 32)
	batchIDs   []VectorID
	batchLocs  []Location
	
	// PQ (Product Quantization) buffers
	pqFlatCodes    []byte
	pqBatchResults []float32
}

const searchBatchSize = 32

// SearchPool manages reusable SearchContext objects
type SearchPool struct {
	pool sync.Pool
	
	// Metrics
	gets atomic.Int64
	puts atomic.Int64
}

// NewSearchPool creates a new search context pool
func NewSearchPool() *SearchPool {
	return &SearchPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &SearchContext{
					results:        make([]SearchResult, 0, 100),
					batchIDs:       make([]VectorID, searchBatchSize),
					batchLocs:      make([]Location, searchBatchSize),
					pqFlatCodes:    make([]byte, searchBatchSize*32), // Max PQ code size
					pqBatchResults: make([]float32, searchBatchSize),
				}
			},
		},
	}
}

// Get retrieves a SearchContext from the pool
func (sp *SearchPool) Get() *SearchContext {
	sp.gets.Add(1)
	ctx := sp.pool.Get().(*SearchContext)
	// Reset the result slice but keep capacity
	ctx.results = ctx.results[:0]
	return ctx
}

// Put returns a SearchContext to the pool
func (sp *SearchPool) Put(ctx *SearchContext) {
	if ctx == nil {
		return
	}
	sp.puts.Add(1)
	
	// Clear results before returning to pool
	for i := range ctx.results {
		ctx.results[i] = SearchResult{}
	}
	ctx.results = ctx.results[:0]
	
	// Clear batch buffers
	for i := range ctx.batchIDs {
		ctx.batchIDs[i] = 0
	}
	for i := range ctx.batchLocs {
		ctx.batchLocs[i] = Location{}
	}
	
	sp.pool.Put(ctx)
}

// Stats returns pool statistics
func (sp *SearchPool) Stats() (gets, puts int64) {
	return sp.gets.Load(), sp.puts.Load()
}
