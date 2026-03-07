package store

import (
	"context"
	"sync"

	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
)

type BatchSearchRequest struct {
	Query    []float32
	K        int
	Filters  []query.Filter
	Options  SearchOptions
	ResultCh chan []SearchResult
	ErrorCh  chan error
}

type BatchSearchProcessor struct {
	requests   chan BatchSearchRequest
	workers    int
	resultPool *SearchResultPool
	wg         sync.WaitGroup
}

func NewBatchSearchProcessor(workers int, queueSize int) *BatchSearchProcessor {
	pool := NewSearchResultPool()
	return &BatchSearchProcessor{
		requests:   make(chan BatchSearchRequest, queueSize),
		workers:    workers,
		resultPool: pool,
	}
}

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

func (bp *BatchSearchProcessor) Close() {
	close(bp.requests)
	bp.wg.Wait()
}

type BatchSearchResult struct {
	Results []SearchResult
	Err     error
}

type BatchSearchBatcher struct {
	mu        sync.Mutex
	pending   []BatchSearchRequest
	batchSize int
	interval  int
	ticker    chan struct{}
	resultCh  chan BatchSearchResult
}

func NewBatchSearchBatcher(batchSize int, interval int) *BatchSearchBatcher {
	return &BatchSearchBatcher{
		batchSize: batchSize,
		interval:  interval,
		ticker:    make(chan struct{}, 1),
		resultCh:  make(chan BatchSearchResult, batchSize),
	}
}

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

func (bb *BatchSearchBatcher) GetResults() <-chan BatchSearchResult {
	return bb.resultCh
}

func ComputeBatchDistancesSIMD(queries [][]float32, vectors [][]float32, k int) [][]SearchResult {
	if len(queries) == 0 || len(vectors) == 0 {
		return nil
	}

	results := make([][]SearchResult, len(queries))

	for i := range results {
		results[i] = make([]SearchResult, 0, k)
	}

	numVectors := len(vectors)

	chunkSize := 64
	for start := 0; start < numVectors; start += chunkSize {
		end := start + chunkSize
		if end > numVectors {
			end = numVectors
		}

		for qIdx, query := range queries {
			for vIdx := start; vIdx < end; vIdx++ {
				dist, err := simd.EuclideanDistance(query, vectors[vIdx])
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
