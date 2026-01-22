package store

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/coder/hnsw"
)

// ParallelSearchConfig controls parallel search behavior
type ParallelSearchConfig struct {
	Enabled   bool
	Workers   int
	Threshold int
}

// DefaultParallelSearchConfig returns sensible defaults
func DefaultParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{
		Enabled:   true,
		Workers:   runtime.NumCPU(),
		Threshold: 50,
	}
}

// getParallelSearchConfig returns the current config
func (h *HNSWIndex) getParallelSearchConfig() ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig updates the parallel search configuration
func (h *HNSWIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {
	h.parallelConfig = cfg
}

// processResultsParallel processes HNSW neighbors using worker pool
func (h *HNSWIndex) processResultsParallel(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], k int, filters []qry.Filter) []SearchResult {
	cfg := h.getParallelSearchConfig()
	numWorkers := cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	chunkSize := (len(neighbors) + numWorkers - 1) / numWorkers
	if chunkSize < 50 { // Increased threshold to avoid overhead for small chunks
		return h.processResultsSerial(ctx, query, neighbors, k, filters)
	}

	metrics.HnswParallelSearchSplits.WithLabelValues(h.dataset.Name).Inc()

	// Pre-allocate results array of arrays to avoid mutex on append
	// simpler than shared slice with atomics since result count per chunk varies due to filtering
	chunksResults := make([][]SearchResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(neighbors) {
			break
		}
		end := start + chunkSize
		if end > len(neighbors) {
			end = len(neighbors)
		}

		wg.Add(1)
		go func(workerID int, chunk []hnsw.Node[VectorID]) {
			defer wg.Done()
			chunksResults[workerID] = h.processChunk(ctx, query, chunk, filters)
		}(i, neighbors[start:end])
	}
	wg.Wait()

	// Merge results
	// First calculate total size
	totalLen := 0
	for _, res := range chunksResults {
		totalLen += len(res)
	}

	allResults := make([]SearchResult, 0, totalLen)
	for _, res := range chunksResults {
		allResults = append(allResults, res...)
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Score < allResults[j].Score
	})

	if len(allResults) > k {
		allResults = allResults[:k]
	}

	return allResults
}

// processChunk processes a chunk of HNSW neighbors
func (h *HNSWIndex) processChunk(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], filters []qry.Filter) []SearchResult {
	results := make([]SearchResult, 0, len(neighbors))

	// Step 1: Batch location lookup
	// Optimized: Parallel access via ChunkedLocationStore is lock-free for reads
	// and doesn't require global Mu RLock for the list itself.
	locations := make([]Location, len(neighbors))
	found := make([]bool, len(neighbors))

	prefetchCount := 4 // Prefetch 4 items ahead
	prefetchOps := 0

	// Prefetch first batch of neighbors
	for i := 0; i < prefetchCount && i < len(neighbors); i++ {
		simd.Prefetch(unsafe.Pointer(&neighbors[i]))
		prefetchOps++
	}

	for i, n := range neighbors {
		// Software prefetch next neighbor
		nextIdx := i + prefetchCount
		if nextIdx < len(neighbors) {
			simd.Prefetch(unsafe.Pointer(&neighbors[nextIdx]))
			prefetchOps++
		}

		// Context check every 32 items
		if i&31 == 0 {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}
		nodeID := n.Key
		loc, ok := h.locationStore.Get(nodeID)
		if ok {
			locations[i] = loc
			found[i] = true
		}
	}

	metrics.PrefetchOperationsTotal.Add(float64(prefetchOps))

	// Step 2: Batch record access and filtering
	// Minimize dataset.dataMu contention

	type vectorTask struct {
		id  VectorID
		vec []float32
	}
	// Pre-allocate to reduce allocs
	tasks := make([]vectorTask, 0, len(neighbors))

	h.dataset.dataMu.RLock()

	evaluators := make(map[int]*qry.FilterEvaluator)
	if len(filters) > 0 && len(h.dataset.Records) > 0 {
		// Pre-bind batch 0 if it exists
		ev, err := qry.NewFilterEvaluator(h.dataset.Records[0], filters)
		if err == nil {
			evaluators[0] = ev
		}
	}

	for i, n := range neighbors {
		// Context check every 32 items
		if i&31 == 0 {
			select {
			case <-ctx.Done():
				h.dataset.dataMu.RUnlock()
				return nil
			default:
			}
		}
		if !found[i] {
			continue
		}
		loc := locations[i]

		if loc.BatchIdx >= len(h.dataset.Records) {
			continue
		}
		rec := h.dataset.Records[loc.BatchIdx]

		if len(evaluators) > 0 {
			ev, ok := evaluators[loc.BatchIdx]
			if !ok {
				if loc.BatchIdx >= len(h.dataset.Records) {
					continue
				}
				var err error
				ev, err = qry.NewFilterEvaluator(h.dataset.Records[loc.BatchIdx], filters)
				if err != nil {
					continue
				}
				evaluators[loc.BatchIdx] = ev
			}
			if !ev.Matches(loc.RowIdx) {
				continue
			}
		}

		// Extract vector (copy)
		vec, err := h.extractVector(rec, loc.RowIdx)
		if err == nil && vec != nil {
			tasks = append(tasks, vectorTask{id: n.Key, vec: vec})
		}
	}
	h.dataset.dataMu.RUnlock()

	// Step 3: Compute distances using SIMD Batch Processing
	// Optimization: Use flat buffer to avoid [][]float32 allocation overhead
	numTasks := len(tasks)
	if numTasks == 0 {
		return results
	}

	// Pre-allocate flat buffer and scores
	dims := len(query)
	flatBuffer := make([]float32, numTasks*dims)
	scores := make([]float32, numTasks)

	// Copy vectors into flat buffer
	for i, t := range tasks {
		offset := i * dims
		copy(flatBuffer[offset:offset+dims], t.vec)

		// Prefetch next vector for distance computation
		nextIdx := i + 4
		if nextIdx < numTasks {
			nextOffset := nextIdx * dims
			simd.Prefetch(unsafe.Pointer(&flatBuffer[nextOffset]))
			metrics.PrefetchOperationsTotal.Inc()
		}
	}

	// Use flat batch SIMD (avoids [][]float32 allocation)
	if h.pqEnabled && h.pqEncoder != nil {
		// PQ path remains unchanged as it has its own optimization
		table := h.pqEncoder.ComputeDistanceTableFlat(query)
		m := h.pqEncoder.CodeSize()
		packedLen := (m + 3) / 4

		flatCodes := make([]byte, numTasks*m)
		validForBatch := make([]bool, numTasks)
		batchCount := 0

		distFunc := h.GetDistanceFunc()
		for i := 0; i < numTasks; i++ {
			if i&31 == 0 {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
			}
			vecOffset := i * dims
			vec := flatBuffer[vecOffset : vecOffset+dims]
			if len(vec) == packedLen {
				ptr := unsafe.Pointer(&vec[0])
				src := unsafe.Slice((*byte)(ptr), m)
				copy(flatCodes[batchCount*m:], src)
				validForBatch[i] = true
				batchCount++
			} else {
				// Fallback for raw / mixed
				scores[i] = distFunc(query, vec)
			}
		}

		if batchCount > 0 {
			batchResults := make([]float32, batchCount)
			if err := h.pqEncoder.ADCDistanceBatch(table, flatCodes[:batchCount*m], batchResults); err != nil {
				// Fallback
				distFunc := h.GetDistanceFunc()
				bj := 0
				for i := 0; i < numTasks; i++ {
					if validForBatch[i] {
						scores[i] = distFunc(query, flatBuffer[i*dims:(i+1)*dims])
						bj++
					}
				}
			} else {
				bj := 0
				for i := 0; i < numTasks; i++ {
					if validForBatch[i] {
						scores[i] = batchResults[bj]
						bj++
					}
				}
			}
		}
	} else {
		// Non-PQ path: Use flat batch function (eliminates [][]float32 allocation)
		if h.batchDistFunc != nil {
			// Build [][]float32 view without allocation (reuse tasks)
			vecs := make([][]float32, numTasks)
			for i, t := range tasks {
				vecs[i] = t.vec
			}
			h.batchDistFunc(query, vecs, scores)
		} else {
			// Use flat batch - no intermediate allocation needed
			simd.EuclideanDistanceBatchFlat(query, flatBuffer, numTasks, dims, scores)
		}
	}

	for i, t := range tasks {
		results = append(results, SearchResult{
			ID:    t.id,
			Score: scores[i],
		})
	}

	return results
}

// processResultsSerial is the fallback serial implementation
func (h *HNSWIndex) processResultsSerial(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], k int, filters []qry.Filter) []SearchResult {
	distFunc := h.GetDistanceFunc()

	evaluators := make(map[int]*qry.FilterEvaluator)
	if len(filters) > 0 && len(h.dataset.Records) > 0 {
		ev, err := qry.NewFilterEvaluator(h.dataset.Records[0], filters)
		if err == nil {
			evaluators[0] = ev
		}
	}

	res := make([]SearchResult, 0, len(neighbors))
	count := 0

	for i, n := range neighbors {
		if i&31 == 0 {
			select {
			case <-ctx.Done():
				return res
			default:
			}
		}
		if count >= k {
			break
		}

		loc, found := h.GetLocation(n.Key)
		if !found {
			continue
		}

		h.dataset.dataMu.RLock()
		if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
			h.dataset.dataMu.RUnlock()
			continue
		}
		rec := h.dataset.Records[loc.BatchIdx]

		if len(evaluators) > 0 {
			ev, ok := evaluators[loc.BatchIdx]
			if !ok {
				if loc.BatchIdx >= len(h.dataset.Records) {
					h.dataset.dataMu.RUnlock()
					continue
				}
				var err error
				ev, err = qry.NewFilterEvaluator(h.dataset.Records[loc.BatchIdx], filters)
				if err != nil {
					h.dataset.dataMu.RUnlock()
					continue
				}
				evaluators[loc.BatchIdx] = ev
			}
			if !ev.Matches(loc.RowIdx) {
				h.dataset.dataMu.RUnlock()
				continue
			}
		}

		vec, _ := h.extractVector(rec, loc.RowIdx)
		h.dataset.dataMu.RUnlock()

		if vec == nil {
			continue
		}

		dist := distFunc(query, vec)
		res = append(res, SearchResult{
			ID:    n.Key,
			Score: dist,
		})
		count++
	}

	return res
}
