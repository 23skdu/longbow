package hnsw2

import (
	"fmt"
	"math"
	
	"github.com/23skdu/longbow/internal/store"
)

// Search performs k-NN search using the provided query vector.
// Returns the k nearest neighbors sorted by distance.
func (h *ArrowHNSW) Search(query []float32, k int, ef int) ([]store.SearchResult, error) {
	h.nodeMu.RLock()
	defer h.nodeMu.RUnlock()
	
	if len(h.nodes) == 0 {
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
	
	// Ensure visited bitset is large enough
	if ctx.visited.Size() < len(h.nodes) {
		ctx.visited = NewBitset(len(h.nodes))
	}
	
	// Start from entry point
	ep := h.entryPoint
	
	// Search from top layer to layer 1
	for level := h.maxLevel; level > 0; level-- {
		ep, _ = h.searchLayer(query, ep, 1, level, ctx)
	}
	
	// Search layer 0 with ef candidates
	_, _ = h.searchLayer(query, ep, ef, 0, ctx)
	
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
func (h *ArrowHNSW) searchLayer(query []float32, entryPoint uint32, ef int, layer int, ctx *SearchContext) (uint32, float32) {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	
	// Initialize with entry point
	entryDist := h.distance(query, entryPoint)
	ctx.candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.visited.Set(entryPoint)
	
	// W: result set (for layer 0, this becomes our final candidates)
	resultSet := NewFixedHeap(ef)
	resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	
	closest := entryPoint
	closestDist := entryDist
	
	// Greedy search
	for ctx.candidates.Len() > 0 {
		// Get nearest candidate
		curr, ok := ctx.candidates.Pop()
		if !ok {
			break
		}
		
		// Stop if current is farther than furthest result
		if resultSet.Len() >= ef {
			worst, ok := resultSet.Peek()
			if ok && curr.Dist > worst.Dist {
				break
			}
		}
		
		// Explore neighbors
		node := &h.nodes[curr.ID]
		neighborCount := int(node.NeighborCounts[layer])
		
		for i := 0; i < neighborCount; i++ {
			neighborID := node.Neighbors[layer][i]
			
			// Skip if already visited
			if ctx.visited.IsSet(neighborID) {
				continue
			}
			ctx.visited.Set(neighborID)
			
			// Calculate distance to neighbor
			dist := h.distance(query, neighborID)
			
			// Update closest if needed
			if dist < closestDist {
				closest = neighborID
				closestDist = dist
			}
			
			// Add to result set if better than worst or we need more
			shouldAdd := resultSet.Len() < ef
			if !shouldAdd {
				worst, ok := resultSet.Peek()
				if ok && dist < worst.Dist {
					shouldAdd = true
				}
			}
			if shouldAdd {
				resultSet.Push(Candidate{ID: neighborID, Dist: dist})
				if resultSet.Len() > ef {
					resultSet.Pop() // Remove worst
				}
				
				// Also add to candidates queue for exploration
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
func (h *ArrowHNSW) distance(query []float32, id uint32) float32 {
	// Get vector from Arrow storage (zero-copy)
	vec, err := h.getVector(id)
	if err != nil {
		return float32(math.Inf(1))
	}
	
	// Use SIMD-optimized distance calculation
	return distanceSIMD(query, vec)
}

// l2Distance computes Euclidean (L2) distance between two vectors.
// Kept for testing and as fallback.
func l2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}
	
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// getVector retrieves a vector from Arrow storage using zero-copy access.
// This uses the Dataset's Index locationStore to map VectorID to (BatchIdx, RowIdx).
func (h *ArrowHNSW) getVector(id uint32) ([]float32, error) {
	if int(id) >= len(h.nodes) {
		return nil, fmt.Errorf("vector ID %d out of bounds", id)
	}
	
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
