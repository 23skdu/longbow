package hnsw2

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(query []float32, k, ef int, filter *store.Bitset) ([]store.SearchResult, error) {
	// Lock-free access: load snapshot of graph data
	data := h.data.Load()
	if data == nil || h.nodeCount.Load() == 0 {
		return []store.SearchResult{}, nil
	}
	
	if k <= 0 {
		return []store.SearchResult{}, nil
	}
	
	// Determine refinement parameters
	targetK := k
	useRefinement := h.config.SQ8Enabled && h.config.RefinementFactor > 1.0
	if useRefinement {
		targetK = int(float64(k) * h.config.RefinementFactor)
		if targetK > h.Size() {
			targetK = h.Size()
		}
	}

	if ef <= 0 {
		ef = k * 2 // Default ef to 2*k
	}
	// Ensure ef is at least as large as targetK
	if ef < targetK {
		ef = targetK
	}
	
	// Get search context from pool
	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)
	
	// Encode query if SQ8 enabled
	if h.quantizer != nil {
		if cap(ctx.querySQ8) < h.dims {
			ctx.querySQ8 = make([]byte, h.dims)
		}
		ctx.querySQ8 = ctx.querySQ8[:h.dims]
		encoded := h.quantizer.Encode(query)
		copy(ctx.querySQ8, encoded)
	}

	// Ensure visited bitset is large enough
	nodeCount := int(h.nodeCount.Load())
	if ctx.visited.Size() < nodeCount {
		ctx.visited = NewBitset(nodeCount)
	}
	
	// Start from entry point (atomic load)
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	
	// Search from top layer to layer 1
	for level := maxL; level > 0; level-- {
		ep, _ = h.searchLayer(query, ep, 1, level, ctx, data, nil)
	}
	
	// Search layer 0 with ef candidates
	_, _ = h.searchLayer(query, ep, ef, 0, ctx, data, filter)
	
	// Extract results
	results := make([]store.SearchResult, 0, targetK)
	extractCount := targetK
	
	for i := 0; i < extractCount && ctx.candidates.Len() > 0; i++ {
		cand, ok := ctx.candidates.Pop()
		if !ok {
			break
		}
		
		id := cand.ID
		score := cand.Dist
		
		// If Rep-ranking, recompute score
		if useRefinement {
			vec := h.mustGetVectorFromData(data, id)
			// Compute exact L2 distance
			score = simd.EuclideanDistance(query, vec)
		}
		
		results = append(results, store.SearchResult{
			ID:    store.VectorID(id),
			Score: score,
		})
	}
	
	// If refinement used, sort and truncate
	if useRefinement {
		sort.Slice(results, func(i, j int) bool {
			return results[i].Score < results[j].Score // Ascending distance (smallest first)
		})
		
		if len(results) > k {
			results = results[:k]
		}
	}
	
	return results, nil
}

// searchLayer performs greedy search at a specific layer.
// Returns the closest node found and its distance.
func (h *ArrowHNSW) searchLayer(query []float32, entryPoint uint32, ef, layer int, ctx *SearchContext, data *GraphData, filter *store.Bitset) (uint32, float32) {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	
	// Initialize with entry point
	entryDist := h.distance(query, entryPoint, data)
	
	ctx.candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.visited.Set(entryPoint)
	
	// W: result set (max-heap to track furthest result)
	resultSet := ctx.resultSet
	if resultSet.cap < ef {
		resultSet = NewMaxHeap(ef)
		ctx.resultSet = resultSet
	}
	resultSet.Clear()
	resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	
	closest := entryPoint
	closestDist := entryDist
	
	// SQ8 Setup
	useSQ8 := h.quantizer != nil && len(data.VectorsSQ8) > 0
	
	// Greedy search
	for ctx.candidates.Len() > 0 {
		// Get nearest candidate (min-heap)
		curr, ok := ctx.candidates.Pop()
		if !ok {
			break
		}
		
		// Stop if current is farther than furthest result (and result set is full)
		if resultSet.Len() >= ef {
			worst, ok := resultSet.Peek()
			if ok && curr.Dist > worst.Dist {
				break
			}
		}
		
		// Explore neighbors
		neighborCount := int(atomic.LoadInt32(&data.Counts[layer][curr.ID]))
		baseIdx := int(curr.ID) * MaxNeighbors
		
		// 1. Collect unvisited neighbors - Batching Phase
		ctx.scratchIDs = ctx.scratchIDs[:0]
		
		for i := 0; i < neighborCount; i++ {
			neighborID := data.Neighbors[layer][baseIdx+i]
			if !ctx.visited.IsSet(neighborID) {
				ctx.visited.Set(neighborID)
				ctx.scratchIDs = append(ctx.scratchIDs, neighborID)
			}
		}
		
		count := len(ctx.scratchIDs)
		if count == 0 {
			continue
		}

		// 2. Compute Distances - Batch Processing Phase
		var dists []float32
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists = ctx.scratchDists[:count]
		
		// Adaptive batching: Use BatchDistanceComputer for large candidate sets
		useBatchCompute := h.batchComputer != nil && h.batchComputer.ShouldUseBatchCompute(count)
		
		if useSQ8 {
			// SQ8 Path
			// Simple loop for now
			for i, nid := range ctx.scratchIDs {
				off := int(nid) * h.dims
				// Bounds check
				if off+h.dims <= len(data.VectorsSQ8) {
					d := simd.EuclideanDistanceSQ8(ctx.querySQ8, data.VectorsSQ8[off:off+h.dims])
					dists[i] = float32(d)
				} else {
					dists[i] = math.MaxFloat32
				}
			}
		} else if useBatchCompute {
			// Vectorized batch distance computation using Arrow compute
			// Gather vectors for batch processing
			if cap(ctx.scratchVecs) < count {
				ctx.scratchVecs = make([][]float32, count*2)
			}
			vecs := ctx.scratchVecs[:count]
			
			for i, nid := range ctx.scratchIDs {
				vecs[i] = h.mustGetVectorFromData(data, nid)
			}
			
			// Use batch computer for vectorized distances
			batchDists, err := h.batchComputer.ComputeL2Distances(query, vecs)
			if err != nil {
				// Fallback to SIMD on error
				simd.EuclideanDistanceBatch(query, vecs, dists)
			} else {
				copy(dists, batchDists)
			}
		} else {
			// Float32 Path (SIMD for small batches)
			if cap(ctx.scratchVecs) < count {
				ctx.scratchVecs = make([][]float32, count*2)
			}
			vecs := ctx.scratchVecs[:count]
			
			for i, nid := range ctx.scratchIDs {
				vecs[i] = h.mustGetVectorFromData(data, nid)
			}
			simd.EuclideanDistanceBatch(query, vecs, dists)
		}
		
		// 3. Process Results
		for i, neighborID := range ctx.scratchIDs {
			dist := dists[i]
			
			// Update closest
			if dist < closestDist {
				closest = neighborID
				closestDist = dist
			}
			
			// Update result set
			shouldAdd := resultSet.Len() < ef
			if !shouldAdd {
				worst, ok := resultSet.Peek()
				if ok && dist < worst.Dist {
					shouldAdd = true
				}
			}
			
			if shouldAdd {
				if resultSet.Len() >= ef {
					resultSet.Pop()
				}
				resultSet.Push(Candidate{ID: neighborID, Dist: dist})
				ctx.candidates.Push(Candidate{ID: neighborID, Dist: dist})
			}
		}
	}
	
	// For layer 0, copy result set to ctx.candidates for final extraction
	if layer == 0 {
		ctx.candidates.Clear()
		for resultSet.Len() > 0 {
			cand, ok := resultSet.Pop()
			if ok {
				ctx.candidates.Push(cand)
			}
		}
	}
	
	return closest, closestDist
}

// distance computes the distance between a query vector and a stored vector.
// Uses zero-copy Arrow access and SIMD optimizations for maximum performance.
func (h *ArrowHNSW) distance(query []float32, id uint32, data *GraphData) float32 {
	// PQ check
	// SQ8 Check
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		off := int(id) * h.dims
		if off+h.dims <= len(data.VectorsSQ8) {
			
			// We need query to be quantized too.
			// If we are in 'Search', we have ctx.querySQ8.
			// But 'distance' doesn't take context.
			// Just quantize on fly for single distance check (negligible for entry point).
			qSQ8 := h.quantizer.Encode(query)
			d := simd.EuclideanDistanceSQ8(qSQ8, data.VectorsSQ8[off:off+h.dims])
			return float32(d)
		}
	}

	// Optimization: Check for cached vector pointer (avoids Arrow overhead)
	// This is safe because VectorPtr is pinned to the Arrow RecordBatch which is kept alive by Dataset
	if int(id) < len(data.VectorPtrs) {
		ptr := data.VectorPtrs[id]
		if ptr != nil && h.dims > 0 {
			vec := unsafe.Slice((*float32)(ptr), h.dims)
			return simd.EuclideanDistance(query, vec)
		}
	}

	// Fallback: Get vector from Arrow storage (zero-copy)
	vec, err := h.getVector(id)
	if err != nil {
		return float32(math.Inf(1))
	}
	
	// Use SIMD-optimized distance calculation
	return simd.EuclideanDistance(query, vec)
}

// l2Distance computes Euclidean (L2) distance between two vectors.
// Kept for testing and as fallback.
func l2Distance(a, b []float32) float32 {
	return simd.EuclideanDistance(a, b)
}

// getVector retrieves a vector from Arrow storage using zero-copy access.
// This uses the Dataset's Index locationStore to map VectorID to (BatchIdx, RowIdx).
func (h *ArrowHNSW) getVector(id uint32) ([]float32, error) {
	// Relaxed check: Rely on dataset to validation ID presence
	// if int(id) >= int(h.nodeCount.Load()) { ... }
	
	// Try internal location store first (primary path for ArrowHNSW)
	if h.locationStore != nil {
		if loc, ok := h.locationStore.Get(store.VectorID(id)); ok {
			rec, ok := h.dataset.GetRecord(loc.BatchIdx)
			if !ok {
				return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
			}
			return extractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
		}
	}

	// Fallback to dataset index (legacy/hybrid mode)
	idx := h.dataset.Index
	if idx == nil {
		return nil, fmt.Errorf("dataset index is nil and vector %d not in internal store", id)
	}
	
	// Get location from index
	loc, ok := idx.GetLocation(store.VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector %d not found in locationStore", id)
	}
	
	// Get the Arrow record batch
	rec, ok := h.dataset.GetRecord(loc.BatchIdx)
	if !ok {
		return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
	}
	
	// Extract vector using zero-copy Arrow access
	return extractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
}


