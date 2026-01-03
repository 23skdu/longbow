package store

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/simd"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(query []float32, k, ef int, filter *Bitset) ([]SearchResult, error) {
	// Lock-free access: load snapshot of graph data
	data := h.data.Load()
	if data == nil || h.nodeCount.Load() == 0 {
		return []SearchResult{}, nil
	}

	if k <= 0 {
		return []SearchResult{}, nil
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
	// Encode query if SQ8 enabled
	dims := int(h.dims.Load())
	if h.quantizer != nil {
		if cap(ctx.querySQ8) < dims {
			ctx.querySQ8 = make([]byte, dims)
		}
		ctx.querySQ8 = ctx.querySQ8[:dims]
		h.quantizer.Encode(query, ctx.querySQ8)
	}

	// Ensure visited bitset is large enough
	// Reuse or create visited bitset
	nodeCount := int(h.nodeCount.Load())
	if ctx.visited == nil || ctx.visited.Size() < nodeCount {
		ctx.visited = NewArrowBitset(nodeCount)
	} else {
		ctx.visited.ClearSIMD()
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
	results := make([]SearchResult, 0, targetK)

	// Extract results from resultSet (MaxHeap returns Worst first)
	// We pop all, then reverse to get Best -> Worst
	// Or if using refinement, we'll sort anyway.

	for ctx.resultSet.Len() > 0 {
		cand, ok := ctx.resultSet.Pop()
		if !ok {
			break
		}

		id := cand.ID
		score := cand.Dist

		// If Re-ranking, recompute score
		if useRefinement {
			vec := h.mustGetVectorFromData(data, id)
			if len(vec) == dims {
				// Compute exact L2 distance
				score = simd.EuclideanDistance(query, vec)
			}
		}

		results = append(results, SearchResult{
			ID:    VectorID(id),
			Score: score,
		})
	}

	// Reverse results (since MaxHeap Pop gave Worst...Best)
	// Unless we re-rank and sort later.
	if !useRefinement {
		slices.Reverse(results)
		if len(results) > k {
			results = results[:k]
		}
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
func (h *ArrowHNSW) searchLayer(query []float32, entryPoint uint32, ef, layer int, ctx *ArrowSearchContext, data *GraphData, filter *Bitset) (uint32, float32) {
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
	if !h.IsDeleted(entryPoint) {
		resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	}

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

		// 1. Collect unvisited neighbors - Batching Phase
		for {
			ctx.scratchIDs = ctx.scratchIDs[:0]

			// Seqlock read start
			// Versions[layer][cID] is *[]uint32.
			verAddr := &(*data.Versions[layer][chunkID(curr.ID)])[chunkOffset(curr.ID)]
			ver := atomic.LoadUint32(verAddr)

			if ver%2 != 0 {
				runtime.Gosched()
				continue
			}

			// Get neighbors
			cID := chunkID(curr.ID)
			cOff := chunkOffset(curr.ID)

			// Check if chunk exists
			if data.Counts[layer][cID] == nil {
				// Should not happen for visited nodes, but safe to skip
				continue
			}

			// 1. Get number of neighbors
			count := atomic.LoadInt32(&(*data.Counts[layer][cID])[cOff])
			neighborCount := int(count)
			baseIdx := int(cOff) * MaxNeighbors
			neighborsChunk := (*data.Neighbors[layer][cID])

			for i := 0; i < neighborCount; i++ {
				// Atomic load to satisfy race detector
				neighborID := atomic.LoadUint32(&neighborsChunk[baseIdx+i])
				if !ctx.visited.IsSet(neighborID) {
					// Speculative add - do not Set visited yet
					ctx.scratchIDs = append(ctx.scratchIDs, neighborID)
				}
			}

			// Seqlock read end check
			if atomic.LoadUint32(verAddr) == ver {
				break
			}
		}

		// Commit visited state
		for _, nid := range ctx.scratchIDs {
			ctx.visited.Set(nid)
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
			// Ensure querySQ8 is correctly set in context for useSQ8 path
			if len(ctx.querySQ8) == 0 {
				ctx.querySQ8 = h.quantizer.Encode(query, nil)
			}

			for i, nid := range ctx.scratchIDs {
				cID := chunkID(nid)
				cOff := chunkOffset(nid)
				dims := int(h.dims.Load())
				off := int(cOff) * dims

				if int(cID) < len(data.VectorsSQ8) && data.VectorsSQ8[cID] != nil && off+dims <= len(*data.VectorsSQ8[cID]) {
					d := simd.EuclideanDistanceSQ8(ctx.querySQ8, (*data.VectorsSQ8[cID])[off:off+dims])
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

			// Check filter if provided
			if filter != nil && !filter.Contains(int(neighborID)) {
				continue
			}

			// Update result set - only add if not deleted
			isDeleted := h.IsDeleted(neighborID)
			shouldAdd := !isDeleted && resultSet.Len() < ef
			if !isDeleted && !shouldAdd {
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
			}

			// Always add to candidates to maintain graph navigability
			ctx.candidates.Push(Candidate{ID: neighborID, Dist: dist})

		}
	}

	// For layer 0, we leave results in resultSet for the caller to extract.

	return closest, closestDist
}

// distance computes the distance between a query vector and a stored vector.
// Uses zero-copy Arrow access and SIMD optimizations for maximum performance.
func (h *ArrowHNSW) distance(query []float32, id uint32, data *GraphData) float32 {
	// PQ check
	// SQ8 Check
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		cID := chunkID(id)
		cOff := chunkOffset(id)
		dims := int(h.dims.Load())
		off := int(cOff) * dims

		if int(cID) < len(data.VectorsSQ8) && data.VectorsSQ8[cID] != nil && off+dims <= len(*data.VectorsSQ8[cID]) {

			// We need query to be quantized too.
			// Just quantize on fly for single distance check (negligible for entry point).
			// Optimization: use scratch from somewhere? No, local alloc for single dist is okay?
			// Ideally reuse buffer.
			qSQ8 := h.quantizer.Encode(query, nil)
			d := simd.EuclideanDistanceSQ8(qSQ8, (*data.VectorsSQ8[cID])[off:off+dims])
			return float32(d)
		}
	}

	// Optimization: Check for cached vector pointer (avoids Arrow overhead)
	// This is safe because VectorPtr is pinned to the Arrow RecordBatch which is kept alive by Dataset
	// Optimization: Check for dense vector storage (avoids Arrow overhead)
	cID := chunkID(id)
	cOff := chunkOffset(id)
	dims := int(h.dims.Load())
	if int(cID) < len(data.Vectors) && data.Vectors[cID] != nil {
		start := int(cOff) * dims
		if start+dims <= len(*data.Vectors[cID]) {
			vec := (*data.Vectors[cID])[start : start+dims]
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
		if loc, ok := h.locationStore.Get(VectorID(id)); ok {
			rec, ok := h.dataset.GetRecord(loc.BatchIdx)
			if !ok {
				return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
			}
			return ExtractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
		}
	}

	// Fallback to dataset index (legacy/hybrid mode)
	idx := h.dataset.Index
	if idx == nil {
		return nil, fmt.Errorf("dataset index is nil and vector %d not in internal store", id)
	}

	// Get location from index
	loc, ok := idx.GetLocation(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector %d not found in locationStore", id)
	}

	// Get the Arrow record batch
	rec, ok := h.dataset.GetRecord(loc.BatchIdx)
	if !ok {
		return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
	}
	// Extract vector using zero-copy Arrow access
	return ExtractVectorFromArrow(rec, loc.RowIdx, h.vectorColIdx)
}
