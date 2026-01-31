package store

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// ParallelSearchHost abstracts index-specific operations needed for parallel result processing.
type ParallelSearchHost interface {
	GetDataset() *Dataset
	GetLocationForParallel(id uint32) (core.Location, bool)
	ExtractVectorForParallel(rec arrow.RecordBatch, rowIdx int) ([]float32, error)
	GetParallelSearchConfig() types.ParallelSearchConfig
	GetDistanceFuncForParallel() func(a, b []float32) float32
	GetPQEnabledForParallel() bool
	GetPQEncoderForParallel() *pq.PQEncoder
	ExtractVectorByIDForParallel(id uint32) ([]float32, error)
	SearchForParallel(query []float32, k int) []types.Candidate
}

// HNSWIndex used to implement ParallelSearchHost here, but now ArrowHNSW implements it directly.

// processResultsParallel processes candidate neighbors using worker pool
func (h *ArrowHNSW) processResultsParallel(ctx context.Context, query []float32, candidates []types.Candidate, k int, filters []qry.Filter) []SearchResult { //nolint:unparam
	return processResultsParallelInternal(ctx, h, query, candidates, k, filters, nil)
}

// processResultsParallelInternal is the generalized parallel result processing routine.
func processResultsParallelInternal(ctx context.Context, h ParallelSearchHost, query []float32, candidates []types.Candidate, k int, filters []qry.Filter, bitmap *roaring.Bitmap) []SearchResult {
	cfg := h.GetParallelSearchConfig()
	numWorkers := cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	neighborCount := len(candidates)
	dataset := h.GetDataset()
	dsName := "unknown"
	if dataset != nil {
		dsName = dataset.Name
	}

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
	metrics.HnswAdaptiveChunkSize.WithLabelValues(dsName, "parallel").Observe(float64(chunkSize))
	metrics.HnswParallelSearchWorkerCount.WithLabelValues(dsName).Observe(float64(numWorkers))

	// Determine if parallel processing is worthwhile
	if !cfg.Enabled || neighborCount < chunkSize*2 {
		metrics.HnswSerialFallbackTotal.WithLabelValues(dsName, getFallbackReason(cfg, neighborCount, chunkSize)).Inc()
		// Serial fallback implementation here is simplified to just reuse the chunk logic in serial
		return processChunkInternal(ctx, h, query, candidates, filters, bitmap)
	}

	metrics.HnswParallelSearchSplits.WithLabelValues(dsName).Inc()

	// Calculate efficiency metric
	efficiency := float64(neighborCount) / float64(numWorkers*chunkSize)
	metrics.HnswParallelSearchEfficiency.WithLabelValues(dsName).Observe(efficiency)

	// Pre-allocate results array of arrays
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
		go func(workerID int, chunk []types.Candidate) {
			defer wg.Done()
			chunksResults[workerID] = processChunkInternal(ctx, h, query, chunk, filters, bitmap)
		}(i, candidates[start:end])
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

	// Sort and limit
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Distance < allResults[j].Distance
	})

	if len(allResults) > k {
		allResults = allResults[:k]
	}

	return allResults
}

// getFallbackReason returns the reason for serial fallback
func getFallbackReason(cfg types.ParallelSearchConfig, neighborCount, chunkSize int) string {
	if !cfg.Enabled {
		return "disabled"
	}
	if neighborCount < chunkSize*2 {
		return "small_set"
	}
	return "efficiency"
}

// processChunkInternal processes a chunk of HNSW neighbors using generic ParallelSearchHost.
func processChunkInternal(ctx context.Context, h ParallelSearchHost, query []float32, candidates []types.Candidate, filters []qry.Filter, bitmap *roaring.Bitmap) []SearchResult {
	results := make([]SearchResult, 0, len(candidates))
	dataset := h.GetDataset()

	// Step 1: Batch location lookup
	locations := make([]core.Location, len(candidates))
	found := make([]bool, len(candidates))

	prefetchCount := 4 // Prefetch 4 items ahead
	prefetchOps := 0

	// Prefetch first batch of candidates
	for i := 0; i < prefetchCount && i < len(candidates); i++ {
		simd.Prefetch(unsafe.Pointer(&candidates[i]))
		prefetchOps++
	}

	for i, n := range candidates {
		// Software prefetch next candidate
		nextIdx := i + prefetchCount
		if nextIdx < len(candidates) {
			simd.Prefetch(unsafe.Pointer(&candidates[nextIdx]))
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

		nodeID := n.ID
		if bitmap != nil && !bitmap.Contains(nodeID) {
			continue
		}
		loc, ok := h.GetLocationForParallel(nodeID)
		if ok {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_found").Inc()
			locations[i] = loc
			found[i] = true
		} else {
			metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss").Inc()
		}
	}

	metrics.PrefetchOperationsTotal.Add(float64(prefetchOps))

	type vectorTask struct {
		id  uint32
		vec []float32
	}
	tasks := make([]vectorTask, 0, len(candidates))
	evaluators := make(map[int]*qry.FilterEvaluator)
	if dataset != nil {
		dataset.dataMu.RLock()
		if len(filters) > 0 && len(dataset.Records) > 0 {
			ev, err := qry.NewFilterEvaluator(dataset.Records[0], filters)
			if err == nil {
				evaluators[0] = ev
			}
		}
	}

	for i, n := range candidates {
		if i&31 == 0 {
			metrics.HnswContextCheckTotal.Inc()
			select {
			case <-ctx.Done():
				if dataset != nil {
					dataset.dataMu.RUnlock()
				}
				return nil
			default:
			}
		}

		if !found[i] {
			continue
		}

		if dataset != nil {
			loc := locations[i]
			if loc.BatchIdx < 0 || loc.BatchIdx >= len(dataset.Records) {
				continue
			}
			rec := dataset.Records[loc.BatchIdx]

			if len(evaluators) > 0 {
				ev, ok := evaluators[loc.BatchIdx]
				if !ok {
					var err error
					ev, err = qry.NewFilterEvaluator(dataset.Records[loc.BatchIdx], filters)
					if err != nil {
						continue
					}
					evaluators[loc.BatchIdx] = ev
				}
				if !ev.Matches(loc.RowIdx) {
					metrics.HnswBranchPredictionTotal.WithLabelValues("filter_miss").Inc()
					continue
				}
				metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match").Inc()
			}

			vec, err := h.ExtractVectorForParallel(rec, loc.RowIdx)
			if err == nil && vec != nil {
				tasks = append(tasks, vectorTask{id: n.ID, vec: vec})
			}
		} else {
			// Fallback if no dataset (structured filters not supported in this mode)
			if len(filters) > 0 {
				continue // Skip if filters required but dataset unavailable
			}
			vec, err := h.ExtractVectorByIDForParallel(n.ID)
			if err == nil && vec != nil {
				tasks = append(tasks, vectorTask{id: n.ID, vec: vec})
			}
		}
	}
	if dataset != nil {
		dataset.dataMu.RUnlock()
	}

	// Step 3: Compute distances
	numTasks := len(tasks)
	if numTasks == 0 {
		return results
	}

	dims := len(query)
	flatBuffer := make([]float32, numTasks*dims)
	scores := make([]float32, numTasks)

	for i, t := range tasks {
		offset := i * dims
		copy(flatBuffer[offset:offset+dims], t.vec)
		if i+4 < numTasks {
			simd.Prefetch(unsafe.Pointer(&flatBuffer[(i+4)*dims]))
		}
	}

	if h.GetPQEnabledForParallel() && h.GetPQEncoderForParallel() != nil {
		encoder := h.GetPQEncoderForParallel()
		table := encoder.ComputeDistanceTableFlat(query)
		m := encoder.CodeSize()
		// packedLen := (m + 3) / 4 (unused)

		flatCodes := make([]byte, numTasks*m)
		validForBatch := make([]bool, numTasks)
		batchCount := 0

		distFunc := h.GetDistanceFuncForParallel()
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
			if len(vec) == m { // In memory it might be stored as []byte? No, ExtractVector returns []float32
				// This part is tricky because ExtractVector returns []float32, but PQ codes are bytes.
				// If we are in PQ mode, the vectors returned by ExtractVector should already be PQ codes packed into []float32 bits.
				ptr := unsafe.Pointer(&vec[0])
				src := unsafe.Slice((*byte)(ptr), m)
				copy(flatCodes[batchCount*m:], src)
				validForBatch[i] = true
				batchCount++
			} else {
				scores[i] = distFunc(query, vec)
			}
		}

		if batchCount > 0 {
			batchResults := make([]float32, batchCount)
			if err := encoder.ADCDistanceBatch(table, flatCodes[:batchCount*m], batchResults); err != nil {
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
		if err := simd.EuclideanDistanceBatchFlat(query, flatBuffer, numTasks, dims, scores); err != nil {
			distFunc := h.GetDistanceFuncForParallel()
			for i := 0; i < numTasks; i++ {
				scores[i] = distFunc(query, flatBuffer[i*dims:(i+1)*dims])
			}
		}
	}

	for i, t := range tasks {
		dist := scores[i]
		results = append(results, SearchResult{
			ID:       VectorID(t.id),
			Distance: dist,
			Score:    1.0 / (1.0 + dist),
		})
	}

	return results
}
