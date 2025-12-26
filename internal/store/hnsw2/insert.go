package hnsw2

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	
	"time"
	
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
)

// Grow implementation moved to graph.go

// TrainPQ trains the PQ encoder on the provided sample vectors and enables PQ.
func (h *ArrowHNSW) TrainPQ(vectors [][]float32) error {
	// Deprecated in favor of ScalarQuantizer?
	// Leaving as no-op or TODO for now to avoid breaking existing code if any.
	return nil
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWInsertDurationSeconds.Observe(time.Since(start).Seconds())
		metrics.HNSWNodesTotal.WithLabelValues("default").Set(float64(h.nodeCount.Load()))
	}()

	// 1. Optimistic Load (Lock-Free)
	data := h.data.Load()
	
	// 2. Check Capacity & SQ8/Vectors Status
	sq8Missing := h.config.SQ8Enabled && len(data.VectorsSQ8) == 0 && h.dims > 0
	vectorsMissing := len(data.Vectors) == 0 && h.dims > 0
	
	if data.Capacity <= int(id) || sq8Missing || vectorsMissing {
		// Need to grow. Grow() handles serialization internally via growMu.
		// If just SQ8/Vectors missing, we pass current capacity to force check/allocation of chunks
		targetCap := int(id) + 1
		if (sq8Missing || vectorsMissing) && targetCap < data.Capacity {
			targetCap = data.Capacity
		}
		
		h.Grow(targetCap)
		
		// Reload data after potential resize
		data = h.data.Load()
	}
	
	// Get current graph data (refresh not needed as Load() is atomic and pointer swap implies strictly newer data, but we already have `data` from above.)
	// Actually, wait. Insert flow:
	// If we resized, `data` was updated at line 119.
	// If we didn't resize, `data` is from line 107.
	// So `data` is current. We don't need to reload it.
	// Or if we do, just assign it.
	
	// Initialize the new node
	data.Levels[chunkID(id)][chunkOffset(id)] = uint8(level)

	// Get vector for distance calculations (and caching)
	vec, err := h.getVector(id)
	if err != nil {
		return err
	}
	
	// Cache dimensions on first insert
	if h.dims == 0 && len(vec) > 0 {
		h.dims = len(vec)
	}
	
	// Store Dense Vector (Copy for L2 locality)
	if len(vec) > 0 && h.dims > 0 {
		cID := chunkID(id)
		cOff := chunkOffset(id)
		
		// Ensure vectors are allocated (should be handled by Grow above, but double check for safety)
		if int(cID) < len(data.Vectors) {
			dest := data.Vectors[cID][int(cOff)*h.dims : (int(cOff)+1)*h.dims]
			copy(dest, vec)
		}
	}
	
	// Check if this is the first node
	if h.nodeCount.Load() == 0 {
		h.initMu.Lock()
		if h.nodeCount.Load() == 0 {
			h.entryPoint.Store(id)
			h.maxLevel.Store(int32(level))
			h.nodeCount.Store(1)
			h.initMu.Unlock()
			return nil
		}
		h.initMu.Unlock()
	}
	
	// SQ8 Encoding
	if h.quantizer == nil && h.config.SQ8Enabled && h.dims > 0 {
		h.quantizer = NewScalarQuantizer(h.dims)
	}
	
	if h.quantizer != nil {
		// Auto-train on first vector if needed?
		// For now assume quantizer is pre-configured or defaults are used.
		// Ensure capacity
		// data.VectorsSQ8[id*h.dims:] access needs chunking
		cID := chunkID(id)
		cOff := chunkOffset(id)
		
		// We need to write to data.VectorsSQ8[cID][cOff*dims : (cOff+1)*dims]
		// Check if the chunk and offset are valid
		if int(cID) >= len(data.VectorsSQ8) || int(int(cOff)*h.dims) >= len(data.VectorsSQ8[cID]) || int((int(cOff)+1)*h.dims) > len(data.VectorsSQ8[cID]) {
			return fmt.Errorf("SQ8 storage insufficient for vector %d (chunk %d, offset %d)", id, cID, cOff)
		}
		dest := data.VectorsSQ8[cID][int(cOff)*h.dims : int(cOff+1)*h.dims]
		
		// Encode
		h.quantizer.Encode(vec, dest)
	}
	
	// Search for nearest neighbors at each layer
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	
	// Search from top layer down to level+1
	
	// Use search pool to avoid allocations
	ctx := h.searchPool.Get()
	defer h.searchPool.Put(ctx)
	
	for lc := maxL; lc > level; lc-- {
		// Find closest point at this layer
		neighbors := h.searchLayerForInsert(ctx, vec, ep, 1, lc, data)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Insert at layers 0 to level
	for lc := level; lc >= 0; lc-- {
		// Find M nearest neighbors at this layer
		candidates := h.searchLayerForInsert(ctx, vec, ep, h.efConstruction, lc, data)
		
		// Select M neighbors (Target)
		m := h.m
		if lc == 0 {
			m = h.m * 2 // Layer 0 usually denser
		}
		// Ensure m <= Max capacity
		maxCap := h.mMax
		if lc > 0 {
			maxCap = h.mMax0
		}
		if m > maxCap {
			m = maxCap
		}
		
		neighbors := h.selectNeighbors(ctx, candidates, m, data)
		
		// Add connections
		maxConn := h.mMax
		if lc > 0 {
			maxConn = h.mMax0
		}
		
		for _, neighbor := range neighbors {
			h.addConnection(ctx, data, id, neighbor.ID, lc, maxConn)
			h.addConnection(ctx, data, neighbor.ID, id, lc, maxConn)
			
			// Prune neighbor if needed (still useful to keep strict M_max if buffer wasn't hit)
			// Read count atomically
			// Read count atomically
			cID := chunkID(neighbor.ID)
			cOff := chunkOffset(neighbor.ID)
			count := atomic.LoadInt32(&data.Counts[lc][cID][cOff])
			if int(count) > maxConn {
				h.pruneConnections(ctx, data, neighbor.ID, maxConn, lc)
			}
		}
		
		// Update entry point
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Update max level if needed
	if level > maxL {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	}
	
	h.nodeCount.Add(1)
	return nil
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(ctx *SearchContext, query []float32, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	// Ensure visited bitset is large enough
	// Use maxID from location store to cover all possible IDs, including the one currently being inserted
	// Add safety margin
	maxID := int(h.locationStore.MaxID()) + 1000
	if ctx.visited == nil || ctx.visited.Size() < maxID {
		ctx.visited = NewBitset(maxID)
	} else {
		ctx.visited.ClearSIMD()
	}
	ctx.candidates.Clear()
	
	// Ensure candidates heap is large enough
	if ctx.candidates.cap < ef*2 {
		// If pool heap is too small, allocate a temporary one or resize
		// For now, just allocate a new one to be safe, though pool should usually suffice
		ctx.candidates = NewFixedHeap(ef * 2)
	}
	
	visited := ctx.visited
	candidates := ctx.candidates
	
	ctx.resultSet.Clear()
	if ctx.resultSet.cap < ef {
		ctx.resultSet = NewMaxHeap(ef * 2)
	}
	resultSet := ctx.resultSet
	
	// Initialize with entry point
	var entryDist float32
	var querySQ8 []byte
	
	if h.quantizer != nil {
		querySQ8 = h.quantizer.Encode(query, nil)
		// Access SQ8 data for entryPoint
		cID := chunkID(entryPoint)
		cOff := chunkOffset(entryPoint)
		off := int(cOff) * h.dims
		if int(cID) < len(data.VectorsSQ8) && off+h.dims <= len(data.VectorsSQ8[cID]) {
			dSQ8 := simd.EuclideanDistanceSQ8(querySQ8, data.VectorsSQ8[cID][off:off+h.dims])
			entryDist = float32(dSQ8)
		} else {
			// Fallback if not quantized yet? Should not happen if strictly maintained.
			entryDist = 0 // Or MaxFloat
		}
	} else {
		entryDist = simd.EuclideanDistance(query, h.mustGetVectorFromData(data, entryPoint))
	}

	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	visited.Set(entryPoint)
	
	// Reset results from context
	ctx.scratchResults = ctx.scratchResults[:0]
	ctx.scratchResults = append(ctx.scratchResults, Candidate{ID: entryPoint, Dist: entryDist})
	results := ctx.scratchResults
	
	// Circuit breaker for traversal
	iterations := 0
	maxOps := h.nodeCount.Load() * 2 // Generous limit based on graph size
	if maxOps < 10000 { maxOps = 10000 }
	
	for candidates.Len() > 0 {
		iterations++
		if uint32(iterations) > maxOps {
			// Safety break to prevent infinite loops if visited set fails
			break
		}
		curr, ok := candidates.Pop()
		if !ok {
			break
		}
		
		// Stop if current is farther than furthest result
		if best, ok := resultSet.Peek(); ok && resultSet.Len() >= ef && curr.Dist > best.Dist {
			break
		}
		
		// Explore neighbors
		// Explore neighbors
		// Seqlock retry loop handles loading count/neighbors
		
		// Batch Distance Calculation for Neighbors
		// Collect unvisited neighbors with Seqlock retry
		var unvisitedIDs []uint32
		
		for {
			// Seqlock read start
			// Seqlock read start
			ver := atomic.LoadUint32(&data.Versions[layer][chunkID(curr.ID)][chunkOffset(curr.ID)])
			// If odd (dirty), wait/retry
			if ver%2 != 0 {
				runtime.Gosched()
				continue
			}
			
			// Get neighbors of current node
			cID := chunkID(curr.ID)
			cOff := chunkOffset(curr.ID)
			
			// Seqlock read start (recheck version if we calculated it before?)
			// Version was loaded from [curr.ID], we need to use chunk access
			// But wait, the line 303 `ver := atomic.LoadUint32(&data.Versions[layer][curr.ID])` ALSO needs fix
			// I should include that in replacement or previous block.
			// Let's assume I missed line 303.
			
			count := int(atomic.LoadInt32(&data.Counts[layer][cID][cOff]))
			if count > MaxNeighbors { 
				count = MaxNeighbors 
			}
			
			baseIdx := int(cOff)*MaxNeighbors
			neighborhood := data.Neighbors[layer][cID][baseIdx : baseIdx+count]
			
			unvisitedIDs = ctx.scratchIDs[:0]
			for _, nid := range neighborhood {
				if !visited.IsSet(nid) {
					unvisitedIDs = append(unvisitedIDs, nid)
				}
			}
			
			// Seqlock read end check
			// Seqlock read end check
			if atomic.LoadUint32(&data.Versions[layer][chunkID(curr.ID)][chunkOffset(curr.ID)]) == ver {
				break // Success
			}
			// Failed, retry
		}

		// Mark visited for the successfully collected neighbors
		finalCount := 0
		for _, nid := range unvisitedIDs {
			if !visited.IsSet(nid) { // Double check? No, already checked. 
				// But between versions, visited doesn't change.
				// Wait, inside loop we checked !visited.IsSet.
				// So just Set.
				// But we need to filter unvisitedIDs in place? 
				// The previous loop appended ONLY unvisited.
				visited.Set(nid)
				// We need this ID.
				// Wait, if duplicates in graph? (should not happen in HNSW)
				// Anyhow, simple Set is fine.
				unvisitedIDs[finalCount] = nid
				finalCount++
			}
		}
		unvisitedIDs = unvisitedIDs[:finalCount]
		
		count := len(unvisitedIDs)
		if count == 0 {
			continue
		}
		
		// Use scratch buffer from context
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists := ctx.scratchDists[:count]
		
		if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
			// SQ8 Path
			// Serialize loop for now, TODO: Batch SIMD
			for i, nid := range unvisitedIDs {
				cID := chunkID(nid)
				cOff := chunkOffset(nid)
				off := int(cOff) * h.dims
				// Bounds check?
				if int(cID) < len(data.VectorsSQ8) && off+h.dims <= len(data.VectorsSQ8[cID]) {
					d := simd.EuclideanDistanceSQ8(querySQ8, data.VectorsSQ8[cID][off:off+h.dims])
					dists[i] = float32(d)
				} else {
					dists[i] = math.MaxFloat32
				}
			}
		} else {
			if cap(ctx.scratchVecs) < count {
				ctx.scratchVecs = make([][]float32, count*2)
			}
			vecs := ctx.scratchVecs[:count]
			for i, nid := range unvisitedIDs {
				vecs[i] = h.mustGetVectorFromData(data, nid)
			}
			simd.EuclideanDistanceVerticalBatch(query, vecs, dists)
		}
		
		// Process results
		for i, nid := range unvisitedIDs {
			dist := dists[i]
			best, _ := resultSet.Peek()
			if resultSet.Len() < ef || dist < best.Dist {
				resultSet.Push(Candidate{ID: nid, Dist: dist})
				if resultSet.Len() > ef {
					resultSet.Pop() // Remove the furthest element
				}
				candidates.Push(Candidate{ID: nid, Dist: dist}) // Add to candidates for further exploration
			}
		}
	}
	
	// Sort and return results from resultSet
	results = results[:0]
	for resultSet.Len() > 0 {
		cand, _ := resultSet.Pop()
		results = append(results, cand)
	}
	// resultSet returns closest-at-bottom (MaxHeap), so for return, 
	// we usually want them sorted by distance.
	slices.SortFunc(results, func(a, b Candidate) int {
		if a.Dist < b.Dist { return -1 }
		if a.Dist > b.Dist { return 1 }
		return 0
	})
	
	// Sync context results
	ctx.scratchResults = results
	return results
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *SearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
	if len(candidates) <= m {
		return candidates
	}
	
	// Optimization: Limit the scope of the diversity check
	// If limit is set, we only consider the top K candidates for diversity.
	// This avoids O(M * Ef) complexity when Ef is large.
	limit := h.config.SelectionHeuristicLimit
	if limit > 0 && len(candidates) > limit {
		// candidates are already sorted by distance (closest first)
		candidates = candidates[:limit]
	}
	
	// RobustPrune heuristic: select diverse neighbors
	// Use scratch in context or allocate (fallback)
	var selected []Candidate
	var remaining []Candidate
	var selectedVecs [][]float32
	var remainingVecs [][]float32
	
	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m*2)
		}
		selected = ctx.scratchSelected[:0]
		
		if cap(ctx.scratchSelectedVecs) < m {
			ctx.scratchSelectedVecs = make([][]float32, 0, m*2)
		}
		selectedVecs = ctx.scratchSelectedVecs[:0]

		if cap(ctx.scratchRemaining) < len(candidates) {
			ctx.scratchRemaining = make([]Candidate, len(candidates)*2)
		}
		remaining = ctx.scratchRemaining[:len(candidates)]

		if cap(ctx.scratchRemainingVecs) < len(candidates) {
			ctx.scratchRemainingVecs = make([][]float32, len(candidates)*2)
		}
		remainingVecs = ctx.scratchRemainingVecs[:len(candidates)]
	} else {
		selected = make([]Candidate, 0, m)
		remaining = make([]Candidate, len(candidates))
		selectedVecs = make([][]float32, 0, m)
		remainingVecs = make([][]float32, len(candidates))
	}
	copy(remaining, candidates)
	
	// Pre-fetch vectors once
	for i := range candidates {
		remainingVecs[i] = h.mustGetVectorFromData(data, candidates[i].ID)
	}

	discarded := ctx.scratchDiscarded[:0]
	
	alpha := h.config.Alpha
	if alpha < 1.0 {
		alpha = 1.0
	}
	
	// Use scratchSelectedIdxs to track which candidates are still "remaining"
	// but instead of removing from slices, we'll mark them.
	// Actually, the current "remaining" logic is O(M * remaining).
	// Let's keep it but ensure NO ALLOCATIONS.
	
	for len(selected) < m && len(remaining) > 0 {
		// Find the closest remaining candidate
		bestIdx := 0
		bestDist := remaining[0].Dist
		
		for i := 1; i < len(remaining); i++ {
			if remaining[i].Dist < bestDist {
				bestDist = remaining[i].Dist
				bestIdx = i
			}
		}
		
		// Add to selected
		selCand := remaining[bestIdx]
		selVec := remainingVecs[bestIdx]
		selected = append(selected, selCand)
		selectedVecs = append(selectedVecs, selVec)
		
		// Remove from remaining (O(1) swap and pop)
		remaining[bestIdx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
		remainingVecs[bestIdx] = remainingVecs[len(remainingVecs)-1]
		remainingVecs = remainingVecs[:len(remainingVecs)-1]
		
		// Prune remaining candidates that are too close to the selected one
		if len(selected) < m && len(remaining) > 0 {
			// Batch optimization: Compute distances to selected neighbor
			count := len(remaining)
			if cap(ctx.scratchDists) < count {
				ctx.scratchDists = make([]float32, count*2)
			}
			dists := ctx.scratchDists[:count]
			
			if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
				// SQ8 Path
				cID := chunkID(selCand.ID)
				cOff := chunkOffset(selCand.ID)
				offSel := int(cOff) * h.dims
				
				if int(cID) < len(data.VectorsSQ8) && offSel+h.dims <= len(data.VectorsSQ8[cID]) {
					vecSel := data.VectorsSQ8[cID][offSel : offSel+h.dims]
					for i := range remaining {
						nCID := chunkID(remaining[i].ID)
						nCOff := chunkOffset(remaining[i].ID)
						offRem := int(nCOff) * h.dims
						
						if int(nCID) < len(data.VectorsSQ8) && offRem+h.dims <= len(data.VectorsSQ8[nCID]) {
							d := simd.EuclideanDistanceSQ8(vecSel, data.VectorsSQ8[nCID][offRem : offRem+h.dims])
							dists[i] = float32(d)
						} else {
							dists[i] = math.MaxFloat32
						}
					}
				} else {
					for i := range dists { dists[i] = math.MaxFloat32 }
				}
			} else {
				// Float32 Path
				if cap(ctx.scratchVecs) < count {
					ctx.scratchVecs = make([][]float32, count*2)
				}
				remVecs := ctx.scratchVecs[:count]
				for i := range remaining {
					remVecs[i] = remainingVecs[i]
				}
				
				if h.batchComputer != nil {
					_, _ = h.batchComputer.ComputeL2DistancesInto(selVec, remVecs, dists)
				} else {
					simd.EuclideanDistanceVerticalBatch(selVec, remVecs, dists)
				}
			}
			
			// Filter remaining
			filteredCount := 0
			for i, cand := range remaining {
				if dists[i] * alpha > cand.Dist {
					remaining[filteredCount] = cand
					remainingVecs[filteredCount] = remainingVecs[i]
					filteredCount++
				} else {
					discarded = append(discarded, cand)
				}
			}
			remaining = remaining[:filteredCount]
			remainingVecs = remainingVecs[:filteredCount]
		}
	}
	
	// Update context's discarded for return if needed (heuristic keepPruned)
	ctx.scratchDiscarded = discarded
	
	// If we haven't filled M, backfill from discarded (keepPrunedConnections logic)
	// This ensures we maintain connectivity even if heuristic prunes aggressively
	if h.config.KeepPrunedConnections && len(selected) < m && len(discarded) > 0 {
		// Sort discarded by distance (closest first)
		// Use slices.SortFunc to avoid reflection overhead of sort.Slice
		slices.SortFunc(discarded, func(a, b Candidate) int {
			if a.Dist < b.Dist {
				return -1
				}
			if a.Dist > b.Dist {
				return 1
			}
			return 0
		})
		
		for _, cand := range discarded {
			if len(selected) >= m {
				break
			}
			selected = append(selected, cand)
		}
	}
	
	return selected
}

// addConnection adds a directed edge from source to target at the given layer.
func (h *ArrowHNSW) addConnection(ctx *SearchContext, data *GraphData, source, target uint32, layer int, maxConn int) {
	// Acquire lock for the specific node (shard)
	lockID := source % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	cID := chunkID(source)
	cOff := chunkOffset(source)
	
	// Ensure chunk exists (it should, as we resized before insert)
	// countAddr := &data.Counts[layer][cID][cOff]
	countAddr := &data.Counts[layer][cID][cOff]
	count := int(atomic.LoadInt32(countAddr))
	baseIdx := int(cOff) * MaxNeighbors
	neighborsChunk := data.Neighbors[layer][cID]
	
	// Check if already connected
	for i := 0; i < count; i++ {
		if neighborsChunk[baseIdx+i] == target {
			return // Already connected
		}
	}
	
	// Create space if needed
	if count >= MaxNeighbors {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
		count = int(atomic.LoadInt32(countAddr))
	}
	
	// Add connection if space available (should be now)
	if count < MaxNeighbors {
		// Seqlock write: increment version (odd = dirty)
		verAddr := &data.Versions[layer][cID][cOff]
		atomic.AddUint32(verAddr, 1)
		
		// Atomic store to satisfy race detector
		atomic.StoreUint32(&neighborsChunk[baseIdx+count], target)
		atomic.StoreInt32(countAddr, int32(count+1))
		
		// Seqlock write: increment version (even = clean)
		atomic.AddUint32(verAddr, 1)
	}
}

// pruneConnections reduces the number of connections to maxConn using the heuristic.
func (h *ArrowHNSW) pruneConnections(ctx *SearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	// Acquire lock for the specific node
	lockID := nodeID % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	h.pruneConnectionsLocked(ctx, data, nodeID, maxConn, layer)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *SearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)
	
	countAddr := &data.Counts[layer][cID][cOff]
	count := int(atomic.LoadInt32(countAddr))
	
	if count <= maxConn {
		return
	}
	
	// Collect all current neighbors as candidates
	baseIdx := int(cOff) * MaxNeighbors
	neighborsChunk := data.Neighbors[layer][cID]
	
	var candidates []Candidate
	if ctx != nil {
		if cap(ctx.scratchRemaining) < count {
			ctx.scratchRemaining = make([]Candidate, count*2)
		}
		candidates = ctx.scratchRemaining[:count]
	} else {
		candidates = make([]Candidate, count)
	}
	
	nodeVec := h.mustGetVectorFromData(data, nodeID)
    
	// Batch processing optimization:
	var dists []float32
	if ctx != nil {
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists = ctx.scratchDists[:count]
	} else {
		dists = make([]float32, count)
	}

	// Logic Switch: SQ8 vs Float32
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		// SQ8 Path
		// Node itself is in VectorsSQ8
		offNode := int(cOff) * h.dims
		if int(cID) < len(data.VectorsSQ8) && offNode+h.dims <= len(data.VectorsSQ8[cID]) {
			nodeSQ8 := data.VectorsSQ8[cID][offNode : offNode+h.dims]
			
			for i := 0; i < count; i++ {
				neighborID := neighborsChunk[baseIdx+i]
				
				// Neighbor chunk access
				nCID := chunkID(neighborID)
				nCOff := chunkOffset(neighborID)
				offRem := int(nCOff) * h.dims
				
				if int(nCID) < len(data.VectorsSQ8) && offRem+h.dims <= len(data.VectorsSQ8[nCID]) {
					d := simd.EuclideanDistanceSQ8(nodeSQ8, data.VectorsSQ8[nCID][offRem : offRem+h.dims])
					dists[i] = float32(d)
				} else {
					dists[i] = math.MaxFloat32 
				}
			}
		} else {
			for i := 0; i < count; i++ { dists[i] = math.MaxFloat32 }
		}
		
	} else {
		// Float32 Path
		// Use scratch buffers from context to avoid allocations
		if ctx != nil && cap(ctx.scratchVecs) < count {
			ctx.scratchVecs = make([][]float32, count*2)
		}
		
		var neighborVecs [][]float32
		if ctx != nil {
			neighborVecs = ctx.scratchVecs[:count]
		} else {
			neighborVecs = make([][]float32, count)
		}
		
		for i := 0; i < count; i++ {
			neighborID := neighborsChunk[baseIdx+i]
			vec := h.mustGetVectorFromData(data, neighborID)
			neighborVecs[i] = vec
			
			// Compute distance once for initial Candidate
			dists[i] = simd.EuclideanDistance(nodeVec, vec)
		}
	}
	
	// Populate candidates with IDs and distances
	for i := 0; i < count; i++ {
		candidates[i] = Candidate{ID: neighborsChunk[baseIdx+i], Dist: dists[i]}
	}
	
	// Run heuristic to select best M neighbors
	// Prevent infinite recursion by limiting depth
	if ctx != nil {
		ctx.pruneDepth++
		defer func() { ctx.pruneDepth-- }()
		
		// Circuit breaker: if we're too deep in recursion, just return current neighbors
		if ctx.pruneDepth > 5 {
			// Too deep, return current neighbors without pruning
			return
		}
	}
	
	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	
	// Write back
	newCount := len(selected)
	
	// Seqlock write start
	verAddr := &data.Versions[layer][cID][cOff]
	atomic.AddUint32(verAddr, 1)

	for i := 0; i < newCount; i++ {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], selected[i].ID)
	}
	atomic.StoreInt32(countAddr, int32(newCount))
	
	// Seqlock write end
	atomic.AddUint32(verAddr, 1)
}

func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32 {
	// Optimization: Check for dense vector storage
	// if int(id) < len(data.Vectors) // This check is hard with chunks, assume valid
	cID := chunkID(id)
	cOff := chunkOffset(id)
	if int(cID) < len(data.Vectors) {
		start := int(cOff) * h.dims
		if start+h.dims <= len(data.Vectors[cID]) {
			return data.Vectors[cID][start : start+h.dims]
		}
	}
	
	// Fallback
	vec, err := h.getVector(id)
	if err != nil {
		return []float32{}
	}
	return vec
}


// LevelGenerator generates random levels for new nodes.
type LevelGenerator struct {
	ml  float64
	rng *rand.Rand
	mu  sync.Mutex
}

// NewLevelGenerator creates a new level generator.
func NewLevelGenerator(ml float64) *LevelGenerator {
	return &LevelGenerator{
		ml:  ml,
		rng: rand.New(rand.NewSource(42)), // Fixed seed for reproducibility
	}
}

// Generate returns a random level using exponential decay.
func (lg *LevelGenerator) Generate() int {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	
	// Use exponential distribution: -ln(uniform(0,1)) * ml
	uniform := lg.rng.Float64()
	level := int(-math.Log(uniform) * lg.ml)
	
	// Cap at MaxLayers - 1
	if level >= MaxLayers {
		level = MaxLayers - 1
	}
	
	return level
}
