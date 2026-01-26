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
	Enabled      bool
	Workers      int
	Threshold    int
	MinChunkSize int
	MaxChunkSize int
}

// DefaultParallelSearchConfig returns sensible defaults
func DefaultParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{
		Enabled:      true,
		Workers:      runtime.NumCPU(),
		Threshold:    100,
		MinChunkSize: 32,
		MaxChunkSize: 500,
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
func (h *HNSWIndex) processResultsParallel(ctx context.Context, query []float32, neighbors []hnsw.Node[VectorID], k int, filters []qry.Filter) []SearchResult { //nolint:unparam
	cfg := h.getParallelSearchConfig()
	numWorkers := cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	neighborCount := len(neighbors)

	// Adaptive chunk sizing
	// Calculate optimal chunk size to balance parallelism and overhead
	// Target 2-4x the worker count for good load balancing
	targetChunks := numWorkers * 3
	chunkSize := neighborCount / targetChunks

	// Apply min/max constraints
	if chunkSize < cfg.MinChunkSize {
		chunkSize = cfg.MinChunkSize
	}
	if chunkSize > cfg.MaxChunkSize {
		chunkSize = cfg.MaxChunkSize
	}

	// Record metrics
	metrics.HnswAdaptiveChunkSize.WithLabelValues(h.dataset.Name, "parallel").Observe(float64(chunkSize))
	metrics.HnswParallelSearchWorkerCount.WithLabelValues(h.dataset.Name).Observe(float64(numWorkers))

	// Determine if parallel processing is worthwhile
	// Fall back to serial if:
	// 1. Parallel is disabled
	// 2. Not enough work for effective parallelization (less than 2 chunks worth)
	if !cfg.Enabled || neighborCount < chunkSize*2 {
		metrics.HnswSerialFallbackTotal.WithLabelValues(h.dataset.Name, getFallbackReason(cfg, neighborCount, chunkSize)).Inc()
		return h.processResultsSerial(ctx, query, neighbors, k, filters)
	}

	metrics.HnswParallelSearchSplits.WithLabelValues(h.dataset.Name).Inc()

	// Calculate efficiency metric (work per worker vs chunk size)
	efficiency := float64(neighborCount) / float64(numWorkers*chunkSize)
	metrics.HnswParallelSearchEfficiency.WithLabelValues(h.dataset.Name).Observe(efficiency)

	// Pre-allocate results array of arrays to avoid mutex on append
	chunksResults := make([][]SearchResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= neighborCount {
			break
		}
		end := start + chunkSize
		if end > neighborCount {
			end = neighborCount
		}

		wg.Add(1)
		go func(workerID int, chunk []hnsw.Node[VectorID]) {
			defer wg.Done()
			chunksResults[workerID] = h.processChunk(ctx, query, chunk, filters)
		}(i, neighbors[start:end])
	}
	wg.Wait()

	// Merge results
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

// getFallbackReason returns the reason for serial fallback
func getFallbackReason(cfg ParallelSearchConfig, neighborCount int, chunkSize int) string {
	if !cfg.Enabled {
		return "disabled"
	}
	if neighborCount < chunkSize*2 {
		return "small_set"
	}
	return "efficiency"
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
			metrics.HnswContextCheckTotal.Inc()
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}

		nodeID := n.Key
		loc, ok := h.locationStore.Get(nodeID)
		// Likely path: location is found (most neighbors should be valid)
		if ok {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_found").Inc()
			locations[i] = loc
			found[i] = true
		} else {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
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
			metrics.HnswContextCheckTotal.Inc()
			select {
			case <-ctx.Done():
				h.dataset.dataMu.RUnlock()
				return nil
			default:
			}
		}
		// Likely path: neighbor was found in location store
		if !found[i] {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
			continue
		}
		metrics.HnswBranchPredictionTotal.WithLabelValues("location_found").Inc()
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
			// Likely path: filter matches (most rows pass)
			if ev.Matches(loc.RowIdx) {
				metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match").Inc()
			} else {
				metrics.HnswBranchPredictionTotal.WithLabelValues("filter_miss").Inc()
				continue
			}
		} else {
			metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match").Inc()
		}

		// Extract vector (copy)
		vec, err := h.extractVector(rec, loc.RowIdx)
		if err == nil && vec != nil {
			metrics.HnswBranchPredictionTotal.WithLabelValues("result_append").Inc()
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
			_ = simd.EuclideanDistanceBatchFlat(query, flatBuffer, numTasks, dims, scores)
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
		metrics.HnswTraversalIterationsTotal.Inc()

		if i&31 == 0 {
			metrics.HnswContextCheckTotal.Inc()
			select {
			case <-ctx.Done():
				return res
			default:
			}
		}

		// Likely path: haven't reached k results yet
		if count >= k {
			break
		}

		loc, found := h.locationStore.Get(n.Key)
		// Likely path: location is found
		if !found {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
			continue
		}
		metrics.HnswBranchPredictionTotal.WithLabelValues("location_found").Inc()

		h.dataset.dataMu.RLock()
		// Likely path: valid batch index
		if loc.BatchIdx >= len(h.dataset.Records) {
			h.dataset.dataMu.RUnlock()
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
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
			// Likely path: filter matches
			if ev.Matches(loc.RowIdx) {
				metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match").Inc()
			} else {
				metrics.HnswBranchPredictionTotal.WithLabelValues("filter_miss").Inc()
				h.dataset.dataMu.RUnlock()
				continue
			}
		} else {
			metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match").Inc()
		}

		vec, _ := h.extractVector(rec, loc.RowIdx)
		h.dataset.dataMu.RUnlock()

		// Likely path: vector extraction succeeds
		if vec == nil {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
			continue
		}
		metrics.HnswBranchPredictionTotal.WithLabelValues("result_append").Inc()

		dist := distFunc(query, vec)
		res = append(res, SearchResult{
			ID:    n.Key,
			Score: dist,
		})
		count++
	}

	return res
}
