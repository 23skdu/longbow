package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
)

// BatchSearchRequest represents a single search request within a batch.
type BatchSearchRequest struct {
	Query    []float32
	K        int
	Filters  []query.Filter
	Options  SearchOptions
	ResultCh chan []SearchResult
	ErrorCh  chan error
}

// BatchSearchProcessor handles concurrent processing of search requests.
type BatchSearchProcessor struct {
	requests   chan BatchSearchRequest
	workers    int
	resultPool *SearchResultPool
	wg         sync.WaitGroup
}

// NewBatchSearchProcessor creates a new processor with the specified number of workers.
func NewBatchSearchProcessor(workers int, queueSize int) *BatchSearchProcessor {
	pool := NewSearchResultPool()
	return &BatchSearchProcessor{
		requests:   make(chan BatchSearchRequest, queueSize),
		workers:    workers,
		resultPool: pool,
	}
}

// Start begins processing search requests with the provided search function.
func (bp *BatchSearchProcessor) Start(ctx context.Context, searchFn func(ctx context.Context, q any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error)) {
	for i := 0; i < bp.workers; i++ {
		bp.wg.Add(1)
		go bp.worker(ctx, searchFn)
	}
}

func (bp *BatchSearchProcessor) worker(ctx context.Context, searchFn func(ctx context.Context, q any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error)) {
	defer bp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-bp.requests:
			if !ok {
				return
			}

			results, err := searchFn(ctx, req.Query, req.K, req.Filters, req.Options)
			if err != nil {
				select {
				case req.ErrorCh <- err:
				default:
				}
				continue
			}

			select {
			case req.ResultCh <- results:
			default:
			}
		}
	}
}

// Search submits a search request to the processor and waits for the results.
func (bp *BatchSearchProcessor) Search(ctx context.Context, query []float32, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	resultCh := make(chan []SearchResult, 1)
	errorCh := make(chan error, 1)

	req := BatchSearchRequest{
		Query:    query,
		K:        k,
		Filters:  filters,
		Options:  options,
		ResultCh: resultCh,
		ErrorCh:  errorCh,
	}

	select {
	case bp.requests <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case results := <-resultCh:
		return results, nil
	case err := <-errorCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops the processor and waits for all workers to finish.
func (bp *BatchSearchProcessor) Close() {
	close(bp.requests)
	bp.wg.Wait()
}

// BatchSearchResult contains the results or error for a batch search.
type BatchSearchResult struct {
	Results []SearchResult
	Err     error
}

// BatchSearchBatcher collects search requests and processes them in batches.
type BatchSearchBatcher struct {
	mu        sync.Mutex
	pending   []BatchSearchRequest
	batchSize int
	interval  int
	ticker    chan struct{}
	resultCh  chan BatchSearchResult
}

// NewBatchSearchBatcher creates a new batcher with specified size and interval.
func NewBatchSearchBatcher(batchSize int, interval int) *BatchSearchBatcher {
	return &BatchSearchBatcher{
		batchSize: batchSize,
		interval:  interval,
		ticker:    make(chan struct{}, 1),
		resultCh:  make(chan BatchSearchResult, batchSize),
	}
}

// AddRequest adds a search request to the batcher.
func (bb *BatchSearchBatcher) AddRequest(req BatchSearchRequest) {
	bb.mu.Lock()
	bb.pending = append(bb.pending, req)
	if len(bb.pending) >= bb.batchSize {
		bb.mu.Unlock()
		select {
		case bb.ticker <- struct{}{}:
		default:
		}
		return
	}
	bb.mu.Unlock()
}

// GetResults returns a channel for receiving batch search results.
func (bb *BatchSearchBatcher) GetResults() <-chan BatchSearchResult {
	return bb.resultCh
}

// ComputeBatchDistancesSIMD computes distances for a batch of queries using SIMD instructions.
func ComputeBatchDistancesSIMD(queries [][]float32, vectors [][]float32, k int) [][]SearchResult {
	if len(queries) == 0 || len(vectors) == 0 {
		return nil
	}

	results := make([][]SearchResult, len(queries))

	for i := range results {
		results[i] = make([]SearchResult, 0, k)
	}

	numVectors := len(vectors)
	dims := 0
	if numVectors > 0 {
		dims = len(vectors[0])
	} else if len(queries) > 0 {
		dims = len(queries[0])
	}

	kernel := simd.GetKernel[float32](simd.MetricEuclidean, dims)
	if kernel == nil {
		kernel = simd.EuclideanDistance
	}

	chunkSize := 64
	for start := 0; start < numVectors; start += chunkSize {
		end := start + chunkSize
		if end > numVectors {
			end = numVectors
		}

		for qIdx, query := range queries {
			for vIdx := start; vIdx < end; vIdx++ {
				dist, err := kernel(query, vectors[vIdx])
				if err != nil {
					dist = 1e10
				}

				r := &results[qIdx]
				if len(*r) < k {
					*r = append(*r, SearchResult{
						ID:    VectorID(vIdx),
						Score: dist,
					})
					if len(*r) == k {
						heapifySearchResults(*r)
					}
				} else if dist < (*r)[0].Score {
					(*r)[0] = SearchResult{
						ID:    VectorID(vIdx),
						Score: dist,
					}
					siftDownSearchResults(*r, 0)
				}
			}
		}
	}

	for i := range results {
		quickSortSearchResults(results[i])
	}

	return results
}

func heapifySearchResults(h []SearchResult) {
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDownSearchResults(h, i)
	}
}

func siftDownSearchResults(h []SearchResult, i int) {
	n := len(h)
	for {
		largest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < n && h[left].Score > h[largest].Score {
			largest = left
		}
		if right < n && h[right].Score > h[largest].Score {
			largest = right
		}

		if largest == i {
			break
		}
		h[i], h[largest] = h[largest], h[i]
		i = largest
	}
}

func quickSortSearchResults(h []SearchResult) {
	if len(h) <= 1 {
		return
	}
	quickSortSearchResultsRecursive(h, 0, len(h)-1)
}

func quickSortSearchResultsRecursive(h []SearchResult, low, high int) {
	if low < high {
		pivot := partitionSearchResults(h, low, high)
		quickSortSearchResultsRecursive(h, low, pivot-1)
		quickSortSearchResultsRecursive(h, pivot+1, high)
	}
}

func partitionSearchResults(h []SearchResult, low, high int) int {
	pivot := h[high].Score
	i := low - 1
	for j := low; j < high; j++ {
		if h[j].Score < pivot {
			i++
			h[i], h[j] = h[j], h[i]
		}
	}
	h[i+1], h[high] = h[high], h[i+1]
	return i + 1
}

// MicroBatchCoalescer groups concurrent small search queries arriving within a
// bounded window into micro-batches for amortized dispatch (roadmap C3).
type MicroBatchCoalescer struct {
	maxBatch int
	maxWait  time.Duration
	batchFn  func(context.Context, []BatchSearchRequest)

	in      chan BatchSearchRequest
	stopCh  chan struct{}
	closed  atomic.Bool
	batches atomic.Uint64
	wg      sync.WaitGroup
}

// NewMicroBatchCoalescer creates a coalescer that flushes when maxBatch
// requests accumulate or maxWait elapses after the first request in a batch.
func NewMicroBatchCoalescer(maxBatch int, maxWait time.Duration, queueSize int, batchFn func(context.Context, []BatchSearchRequest)) *MicroBatchCoalescer {
	if maxBatch <= 0 {
		maxBatch = 16
	}
	if maxWait <= 0 {
		maxWait = 500 * time.Microsecond
	}
	if queueSize < maxBatch {
		queueSize = maxBatch * 4
	}
	return &MicroBatchCoalescer{
		maxBatch: maxBatch,
		maxWait:  maxWait,
		batchFn:  batchFn,
		in:       make(chan BatchSearchRequest, queueSize),
		stopCh:   make(chan struct{}),
	}
}

// Start launches the coalescer background loop.
func (c *MicroBatchCoalescer) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.loop(ctx)
}

// Stop flushes pending requests and stops the coalescer.
func (c *MicroBatchCoalescer) Stop() {
	if c.closed.Swap(true) {
		return
	}
	close(c.stopCh)
	c.wg.Wait()
}

// BatchesDispatched returns the number of micro-batches executed.
func (c *MicroBatchCoalescer) BatchesDispatched() uint64 {
	return c.batches.Load()
}

// Submit enqueues a small query and blocks until its result arrives or ctx ends.
func (c *MicroBatchCoalescer) Submit(ctx context.Context, q []float32, k int, filters []query.Filter, opts SearchOptions) ([]SearchResult, error) {
	req := BatchSearchRequest{
		Query:    q,
		K:        k,
		Filters:  filters,
		Options:  opts,
		ResultCh: make(chan []SearchResult, 1),
		ErrorCh:  make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stopCh:
		return nil, context.Canceled
	case c.in <- req:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-req.ErrorCh:
		return nil, err
	case res := <-req.ResultCh:
		return res, nil
	}
}

func (c *MicroBatchCoalescer) loop(ctx context.Context) {
	defer c.wg.Done()
	batch := make([]BatchSearchRequest, 0, c.maxBatch)
	var timer *time.Timer
	var timerCh <-chan time.Time

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerCh = nil
		}
		c.batchFn(ctx, batch)
		c.batches.Add(1)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-c.stopCh:
			for {
				select {
				case req := <-c.in:
					batch = append(batch, req)
					if len(batch) >= c.maxBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case req := <-c.in:
			batch = append(batch, req)
			if len(batch) >= c.maxBatch {
				flush()
			} else if len(batch) == 1 {
				if timer == nil {
					timer = time.NewTimer(c.maxWait)
				} else {
					timer.Reset(c.maxWait)
				}
				timerCh = timer.C
			}
		case <-timerCh:
			timerCh = nil
			flush()
		}
	}
}
