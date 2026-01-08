package store

import (
	"fmt"
	"sort"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
)

// Search performs k-NN search using the provided query vector.
func (h *HNSWIndex) Search(query []float32, k int) ([]VectorID, error) {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	// PQ Encoding for query
	var graphQuery = query
	// Use RLock for config check to avoid race
	h.pqCodesMu.RLock()
	// Capture locals to avoid holding lock too long if encoding is slow?
	// Encoding is fast enough.
	if h.pqEnabled && h.pqEncoder != nil {
		codes := h.pqEncoder.Encode(query)
		graphQuery = PackBytesToFloat32s(codes)
	}
	h.pqCodesMu.RUnlock()

	// coder/hnsw library requires synchronization between Search and Add
	// Use global RLock for graph search (multiple concurrent searches OK)
	h.mu.RLock()
	neighbors := h.Graph.Search(graphQuery, k)
	h.mu.RUnlock()

	res := make([]VectorID, len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res, nil
}

// GetNeighbors returns the nearest neighbors for a given vector ID from the graph.
// Note: Since the underlying coder/hnsw library doesn't expose direct graph edges,
// we perform a search using the node's vector to find its closest neighbors.
func (h *HNSWIndex) GetNeighbors(id VectorID) ([]VectorID, error) {
	vec := h.getVector(id)
	if vec == nil {
		return nil, fmt.Errorf("vector ID %d not found", id)
	}

	// Search for neighbors.
	neighbors, err := h.Search(vec, 16)
	if err != nil {
		return nil, err
	}

	// Filter out the query ID itself
	res := make([]VectorID, 0, len(neighbors))
	for _, n := range neighbors {
		if n != id {
			res = append(res, n)
		}
	}
	return res, nil
}

// SearchVectors performs k-NN search returning full results with scores (distances).
// Uses striped locks for location access to reduce contention in result processing.
func (h *HNSWIndex) SearchVectors(query []float32, k int, filters []qry.Filter) ([]SearchResult, error) {
	defer func(start time.Time) {
		metrics.SearchLatencySeconds.WithLabelValues(h.dataset.Name, "vector").Observe(time.Since(start).Seconds())
	}(time.Now())

	// Post-filtering approach:
	// 1. Search for K * factor candidates
	// 2. Filter candidates
	// 3. Keep top K

	// Post-filtering approach with Adaptive Expansion:
	// 1. Search for K * factor candidates
	// 2. Filter candidates
	// 3. Keep top K
	// 4. If < K results, retry with larger factor (Auto-Expansion)

	initialFactor := 10
	retryFactor := 50
	maxRetries := 1

	limit := k
	if len(filters) > 0 {
		limit = k * initialFactor
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// PQ Encoding for query
		var graphQuery = query
		h.pqCodesMu.RLock()
		if h.pqEnabled && h.pqEncoder != nil {
			codes := h.pqEncoder.Encode(query)
			graphQuery = PackBytesToFloat32s(codes)
		}
		h.pqCodesMu.RUnlock()

		// Graph search requires global lock
		h.mu.RLock()
		neighbors := h.Graph.Search(graphQuery, limit)
		h.mu.RUnlock()

		// Use parallel processing for large result sets
		cfg := h.getParallelSearchConfig()
		if cfg.Enabled && len(neighbors) >= cfg.Threshold {
			res := h.processResultsParallel(query, neighbors, k, filters)
			// If we found enough, or it's the last attempt, return
			if len(res) >= k || attempt == maxRetries || len(filters) == 0 {
				return res, nil
			}
			// Prepare for retry
			limit = k * retryFactor
			continue
		}

		// Fall back to serial processing
		distFunc := h.GetDistanceFunc()

		// Pre-process filters once per search
		var evaluator *qry.FilterEvaluator
		if len(filters) > 0 {
			h.dataset.dataMu.RLock()
			if len(h.dataset.Records) > 0 {
				var err error
				evaluator, err = qry.NewFilterEvaluator(h.dataset.Records[0], filters)
				if err != nil {
					h.dataset.dataMu.RUnlock()
					return nil, fmt.Errorf("filter creation failed: %w", err)
				}
			}
			h.dataset.dataMu.RUnlock()
		}

		// Get search context from pool
		ctx := h.searchPool.Get()

		// Use pooled buffers
		// Note: ctx buffers might need resetting if reused in loop, but we defer Put at end of function or loop?
		// Better to Put at end of loop or reuse.
		// Since we might return, defer is tricky inside loop if we want to release before retry.
		// Let's defer Put for the whole function scope, reusing the *same* ctx is fine.
		// Just explicitly clear what we need.
		// Actually, ctx.results is just a buffer, we reconstruct `res` anyway.
		// Wait, `processResultsParallel` returns a new slice, serial path below appends to `res`.
		// Let's be careful.

		res := make([]SearchResult, 0, k) // Allocate fresh result slice for this attempt

		batchIDs := ctx.batchIDs
		batchLocs := ctx.batchLocs

		// PQ Context setup...
		var pqTable []float32
		var pqFlatCodes []byte
		var pqBatchResults []float32
		var pqM int
		var packedLen int
		h.pqCodesMu.RLock()
		if h.pqEnabled && h.pqEncoder != nil {
			pqTable = h.pqEncoder.ComputeDistanceTableFlat(query)
			pqM = h.pqEncoder.CodeSize()
			const batchSize = searchBatchSize
			pqFlatCodes = ctx.pqFlatCodes[:batchSize*pqM]
			pqBatchResults = ctx.pqBatchResults
			packedLen = (pqM + 3) / 4
		}
		h.pqCodesMu.RUnlock()

		count := 0
		const batchSize = searchBatchSize
		for i := 0; i < len(neighbors); i += batchSize {
			if count >= k {
				break
			}

			end := i + batchSize
			if end > len(neighbors) {
				end = len(neighbors)
			}
			batchLen := end - i

			// Copy IDs
			for j := 0; j < batchLen; j++ {
				batchIDs[j] = neighbors[i+j].Key
			}

			h.locationStore.GetBatch(batchIDs[:batchLen], batchLocs[:batchLen])

			h.enterEpoch()
			h.dataset.dataMu.RLock()

			if pqTable != nil {
				for j := 0; j < batchSize; j++ {
					pqBatchResults[j] = -2
				}
			}

			for j := 0; j < batchLen; j++ {
				if count >= k {
					break
				}
				id := batchIDs[j]
				loc := batchLocs[j]

				if loc.BatchIdx == -1 {
					continue
				}

				if evaluator != nil {
					if !evaluator.Matches(loc.RowIdx) {
						continue
					}
				}

				vec := h.getVectorLockedUnsafe(loc)
				if vec != nil {
					if pqTable != nil && len(vec) == packedLen {
						ptr := unsafe.Pointer(&vec[0])
						srcCodes := unsafe.Slice((*byte)(ptr), pqM)
						copy(pqFlatCodes[j*pqM:], srcCodes)
						pqBatchResults[j] = -1
					} else {
						dist := distFunc(query, vec)
						res = append(res, SearchResult{ID: id, Score: dist})
						count++
						if pqTable != nil {
							pqBatchResults[j] = -2
						}
					}
				}
			}

			if pqTable != nil {
				h.pqEncoder.ADCDistanceBatch(pqTable, pqFlatCodes, pqBatchResults)
				for j := 0; j < batchLen; j++ {
					if pqBatchResults[j] >= 0 {
						if count < k {
							res = append(res, SearchResult{ID: batchIDs[j], Score: pqBatchResults[j]})
							count++
						}
					}
				}
			}

			h.dataset.dataMu.RUnlock()
			h.exitEpoch()
		}

		h.searchPool.Put(ctx) // Release context for this attempt

		// Check if we satisfied K
		if len(res) >= k || attempt == maxRetries || len(filters) == 0 {
			return res, nil
		}

		// Retry with larger limit
		limit = k * retryFactor
	}

	return nil, nil // Should be unreachable given return in loop
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (h *HNSWIndex) SearchVectorsWithBitmap(query []float32, k int, filter *qry.Bitset) []SearchResult {
	if filter == nil || filter.Count() == 0 {
		return []SearchResult{}
	}

	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	count := filter.Count()
	if count < 1000 {
		return h.searchBruteForceWithBitmap(query, k, filter)
	}

	// Adaptive limit calculation based on filter selectivity
	limit := calculateAdaptiveLimit(k, count, h.Len())

	h.mu.RLock()
	neighbors := h.Graph.Search(query, limit)
	h.mu.RUnlock()

	distFunc := h.GetDistanceFunc()
	res := make([]SearchResult, 0, k)
	resultCount := 0

	// Batch configuration
	const batchSize = 32
	batchIDs := make([]VectorID, batchSize)
	batchLocs := make([]Location, batchSize)

	// PQ Context
	var pqTable []float32
	var pqFlatCodes []byte
	var pqBatchResults []float32
	var pqM int
	var packedLen int
	h.pqCodesMu.RLock()
	if h.pqEnabled && h.pqEncoder != nil {
		pqTable = h.pqEncoder.ComputeDistanceTableFlat(query)
		pqM = h.pqEncoder.CodeSize()
		pqFlatCodes = make([]byte, batchSize*pqM)
		pqBatchResults = make([]float32, batchSize)
		packedLen = (pqM + 3) / 4
	}
	h.pqCodesMu.RUnlock()

	for i := 0; i < len(neighbors); i += batchSize {
		if resultCount >= k {
			break
		}

		end := i + batchSize
		if end > len(neighbors) {
			end = len(neighbors)
		}

		batchLen := end - i
		for j := 0; j < batchLen; j++ {
			batchIDs[j] = neighbors[i+j].Key
		}

		h.locationStore.GetBatch(batchIDs[:batchLen], batchLocs[:batchLen])

		h.enterEpoch()
		h.dataset.dataMu.RLock()

		// Reset pqBatchResults for this batch
		if pqTable != nil {
			for j := 0; j < batchSize; j++ {
				pqBatchResults[j] = -2 // Default to skipped/unprocessed
			}
		}

		for j := 0; j < batchLen; j++ {
			if resultCount >= k {
				break
			}

			id := batchIDs[j]
			loc := batchLocs[j]

			if !filter.Contains(int(id)) {
				continue
			}

			if loc.BatchIdx == -1 {
				continue
			}

			vec := h.getVectorLockedUnsafe(loc)
			if vec != nil {
				// Case 1: PQ Batch processing
				if pqTable != nil && len(vec) == packedLen {
					ptr := unsafe.Pointer(&vec[0])
					srcCodes := unsafe.Slice((*byte)(ptr), pqM)
					copy(pqFlatCodes[j*pqM:], srcCodes)
					pqBatchResults[j] = -1 // Mark as needing SIMD
				} else {
					// Case 2: Standard distance
					dist := distFunc(query, vec)
					res = append(res, SearchResult{ID: id, Score: dist})
					resultCount++
					if pqTable != nil {
						pqBatchResults[j] = -2 // Mark as already processed
					}
				}
			}
		}

		// Run batch SIMD for all marked candidates
		if pqTable != nil {
			h.pqEncoder.ADCDistanceBatch(pqTable, pqFlatCodes, pqBatchResults)

			for j := 0; j < batchLen; j++ {
				if pqBatchResults[j] >= 0 {
					if resultCount < k {
						res = append(res, SearchResult{ID: batchIDs[j], Score: pqBatchResults[j]})
						resultCount++
					}
				}
			}
		}

		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
	}
	return res
}

func (h *HNSWIndex) searchBruteForceWithBitmap(query []float32, k int, filter *qry.Bitset) []SearchResult {
	// 1. Get all IDs from filter
	ids := filter.ToUint32Array()
	if len(ids) == 0 {
		return []SearchResult{}
	}

	// 2. Iterate and compute distances
	distFunc := h.GetDistanceFunc()
	results := make([]SearchResult, 0, len(ids))

	for _, id := range ids {
		vec, release := h.getVectorUnsafe(VectorID(id))
		if vec == nil {
			continue
		}

		dist := distFunc(query, vec)
		results = append(results, SearchResult{
			ID:    VectorID(id),
			Score: dist,
		})
		release()
	}

	// 3. Sort and truncate to K
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score < results[j].Score
	})

	if len(results) > k {
		results = results[:k]
	}

	return results
}

func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	if k <= 0 {
		return nil
	}

	// Use zero-copy access for the query vector (Unsafe path)
	vec, release := h.getVectorUnsafe(id)
	if vec == nil || release == nil {
		return nil
	}
	defer release()

	h.mu.RLock()
	neighbors := h.Graph.Search(vec, k)
	h.mu.RUnlock()

	res := h.resultPool.get(len(neighbors))
	idx := 0
	for _, n := range neighbors {
		loc, ok := h.locationStore.Get(n.Key)
		if !ok {
			continue
		}
		if loc.BatchIdx != -1 {
			res[idx] = n.Key
			idx++
		}
	}
	return res[:idx]
}

// PutResults returns a search result slice to the pool for reuse.
func (h *HNSWIndex) PutResults(results []VectorID) {
	h.resultPool.put(results)
}

// SearchWithArena performs k-NN search using the provided arena for allocations.
func (h *HNSWIndex) SearchWithArena(query []float32, k int, arena *SearchArena) []VectorID {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	if len(query) == 0 || k <= 0 {
		return nil
	}

	h.mu.RLock()
	neighbors := h.Graph.Search(query, k)
	h.mu.RUnlock()

	if len(neighbors) == 0 {
		return nil
	}

	// Try to allocate result slice from arena
	var res []VectorID
	if arena != nil {
		res = arena.AllocVectorIDSlice(len(neighbors))
	}

	if res == nil {
		res = make([]VectorID, len(neighbors))
	}

	for i, n := range neighbors {
		res[i] = n.Key
	}

	return res

}

// SearchByIDUnsafe performs k-NN search using zero-copy vector access.
func (h *HNSWIndex) SearchByIDUnsafe(id VectorID, k int) []VectorID {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	if k <= 0 {
		return nil
	}

	// Get vector with zero-copy - epoch protection is inside getVectorUnsafe
	vec, release := h.getVectorUnsafe(id)
	if vec == nil || release == nil {
		return nil
	}
	// Ensure release is called when done with vector data
	defer release()

	// coder/hnsw is not thread-safe for concurrent Search and Add.
	h.mu.RLock()
	neighbors := h.Graph.Search(vec, k)
	h.mu.RUnlock()

	if len(neighbors) == 0 {
		return nil
	}

	// Allocate result slice from pool
	res := h.resultPool.get(len(neighbors))
	idx := 0
	for _, n := range neighbors {
		loc, ok := h.locationStore.Get(n.Key)
		if !ok {
			return nil
		}
		if loc.BatchIdx != -1 {
			res[idx] = n.Key
			idx++
		}
	}
	return res[:idx]
}
