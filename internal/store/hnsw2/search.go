package hnsw2

import (
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(query []float32, k int, ef int) ([]store.SearchResult, error) {
	// Lock-free access: load snapshot of graph data
	data := h.data.Load()
	if data == nil || h.nodeCount.Load() == 0 {
		return []store.SearchResult{}, nil
	}
	
	if k <= 0 {
		return []store.SearchResult{}, nil
	}
	
	if ef <= 0 {
		ef = k * 2 // Default ef to 2*k
	}
	
	// Get search context from pool
	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)
	
	// Pre-compute PQ table if needed
	if h.pqEncoder != nil && len(data.QuantizedVectors) > 0 {
		// Use scratch buffer for table
		ctx.scratchPQTable = h.pqEncoder.ComputeTableFlatInto(query, ctx.scratchPQTable)
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
		ep, _ = h.searchLayer(query, ep, 1, level, ctx, data)
	}
	
	// Search layer 0 with ef candidates
	_, _ = h.searchLayer(query, ep, ef, 0, ctx, data)
	
	// Extract top k results from candidates in context
	results := make([]store.SearchResult, 0, k)
	for i := 0; i < k && ctx.candidates.Len() > 0; i++ {
		cand, ok := ctx.candidates.Pop()
		if !ok {
			break
		}
		results = append(results, store.SearchResult{
			ID:    store.VectorID(cand.ID),
			Score: cand.Dist,
		})
	}
	
	return results, nil
}

// searchLayer performs greedy search at a specific layer.
// Returns the closest node found and its distance.
func (h *ArrowHNSW) searchLayer(query []float32, entryPoint uint32, ef int, layer int, ctx *SearchContext, data *GraphData) (uint32, float32) {
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
	
	// PQ setup
	usePQ := h.pqEncoder != nil && len(data.QuantizedVectors) > 0
	pqM := 0
	if h.pqEncoder != nil {
		pqM = h.pqEncoder.config.M
	}
	
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
		
		if usePQ {
			// PQ Path (ADC)
			// Gather codes
			// TODO: Use pre-allocated, aligned scratch buffer for codes if possible to avoid allocs?
			// Ideally SearchContext has scratchBytes. For now, use make, but optimize later.
			// Actually, let's look at doing it zero-alloc if possible.
			// Just verify allocs later.
			flatCodes := make([]byte, count * pqM) // Temp allocation - optimize out!
			for i, nid := range ctx.scratchIDs {
				offset := int(nid) * pqM
				copy(flatCodes[i*pqM:], data.QuantizedVectors[offset:offset+pqM])
			}
			simd.ADCDistanceBatch(ctx.scratchPQTable, flatCodes, pqM, dists)
		} else {
			// Float32 Path
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
	if h.pqEncoder != nil && len(data.QuantizedVectors) > 0 {
		// Use generic/helper or assume this is only called for entry point / single checks
		// Ideally we use a batch context even for single usage, but here:
		// SDC or ADC? Query is float, target is code. ADC.
		// Need table. But 'distance' doesn't take context/table. 
		// Fallback to on-the-fly table or standard logic?
		// For single point, overhead is small. 
		// Use ComputeTableFlat + ADCDistance? Costly.
		// Fallback to "mustGetVectorFromData" (full res) if available? 
		// If using PQ, we might not have full res loaded? 
		// Assumption: We might have both. If PQ enabled, use PQ for speed?
		// For consistency, if PQ enabled for graph, use PQ distance.
		
		// However, to avoid computing table for 1 point, maybe we stick to Full Res for initial entry point check?
		// Or pass Context to distance().
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
	
	// Get the HNSW index from dataset to access locationStore
	hnswIdx, ok := h.dataset.Index.(*store.HNSWIndex)
	if !ok {
		return nil, fmt.Errorf("dataset index is not HNSWIndex")
	}
	
	// Get location from HNSW index's locationStore
	loc, ok := hnswIdx.GetLocation(store.VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector %d not found in locationStore", id)
	}
	
	// Get the Arrow record batch
	rec, ok := h.dataset.GetRecord(loc.BatchIdx)
	if !ok {
		return nil, fmt.Errorf("batch index %d out of bounds", loc.BatchIdx)
	}
	
	// Extract vector using zero-copy Arrow access
	return extractVectorFromArrow(rec, loc.RowIdx)
}


