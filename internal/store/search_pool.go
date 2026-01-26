package store

import (
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// SearchContext holds pre-allocated buffers for HNSW search operations
// to reduce allocation churn during high-frequency searches
type SearchContext struct {
	// Pre-allocated result buffer
	results []SearchResult

	// Batch processing buffers (batchSize = 32)
	batchIDs  []VectorID
	batchLocs []Location

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
			New: func() any {
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

// SearchResultBucket represents a pool of result slices with a specific capacity
type SearchResultBucket struct {
	pool     sync.Pool
	capacity int
	gets     atomic.Int64
	puts     atomic.Int64
	newAlloc atomic.Int64
}

// SearchResultPool manages reusable SearchResult slices organized by capacity buckets
type SearchResultPool struct {
	buckets map[int]*SearchResultBucket
}

// NewSearchResultPool creates a new result pool with common capacity buckets
func NewSearchResultPool() *SearchResultPool {
	return &SearchResultPool{
		buckets: map[int]*SearchResultBucket{
			10:  newSearchResultBucket(10),
			32:  newSearchResultBucket(32),
			64:  newSearchResultBucket(64),
			128: newSearchResultBucket(128),
			256: newSearchResultBucket(256),
		},
	}
}

func newSearchResultBucket(capacity int) *SearchResultBucket {
	return &SearchResultBucket{
		pool: sync.Pool{
			New: func() any {
				return make([]SearchResult, 0, capacity)
			},
		},
		capacity: capacity,
	}
}

// Get retrieves a result slice from the appropriate bucket
func (sp *SearchResultPool) Get(capacity int) []SearchResult {
	// Find closest bucket
	bucket := sp.getBucket(capacity)
	if bucket == nil {
		// No matching bucket, allocate directly
		return make([]SearchResult, 0, capacity)
	}

	bucket.gets.Add(1)
	result := bucket.pool.Get().([]SearchResult)
	result = result[:0] // Reset slice

	metrics.SearchResultPoolGetTotal.WithLabelValues(capacityBucketLabel(capacity)).Inc()
	metrics.SearchResultPoolHitsTotal.WithLabelValues(capacityBucketLabel(capacity)).Inc()

	return result
}

// Put returns a result slice to the appropriate bucket
func (sp *SearchResultPool) Put(slice []SearchResult) {
	if slice == nil {
		return
	}

	capacity := cap(slice)
	bucket := sp.getBucket(capacity)

	if bucket == nil || len(slice) > capacity {
		// Doesn't match any bucket, discard
		return
	}

	// Clear the slice before returning
	for i := range slice {
		slice[i] = SearchResult{}
	}
	slice = slice[:0]

	bucket.puts.Add(1)
	bucket.pool.Put(slice) //nolint:staticcheck // SA6002: slice header is small, avoiding API change

	metrics.SearchResultPoolPutTotal.WithLabelValues(capacityBucketLabel(capacity)).Inc()
}

// getBucket finds the bucket that can hold the given capacity
func (sp *SearchResultPool) getBucket(capacity int) *SearchResultBucket {
	// Exact match or next larger bucket
	for _, c := range []int{10, 32, 64, 128, 256} {
		if c >= capacity {
			return sp.buckets[c]
		}
	}
	// Use largest bucket for very large capacities
	return sp.buckets[256]
}

// capacityBucketLabel returns a label for metrics based on capacity
func capacityBucketLabel(capacity int) string {
	switch {
	case capacity <= 10:
		return "10"
	case capacity <= 32:
		return "32"
	case capacity <= 64:
		return "64"
	case capacity <= 128:
		return "128"
	default:
		return "256+"
	}
}

// Stats returns pool statistics
func (sp *SearchResultPool) Stats() map[string]map[string]int64 {
	stats := make(map[string]map[string]int64)
	for c, bucket := range sp.buckets {
		label := capacityBucketLabel(c)
		stats[label] = map[string]int64{
			"gets":     bucket.gets.Load(),
			"puts":     bucket.puts.Load(),
			"newAlloc": bucket.newAlloc.Load(),
		}
	}
	return stats
}
