package store

import (
	"fmt"
	"math"
	"math/rand"
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
	h.growMu.Lock()
	defer h.growMu.Unlock()

	if len(vectors) == 0 {
		return fmt.Errorf("no vectors for training")
	}

	dims := len(vectors[0])
	if dims == 0 {
		return fmt.Errorf("vector dimension is 0")
	}

	// Default config for PQ if mostly unset
	m := h.config.PQM
	if m == 0 {
		// Heuristic: M = dims / 4 or dims / 8
		if dims%8 == 0 {
			m = dims / 8
		} else if dims%4 == 0 {
			m = dims / 4
		} else {
			m = 1 // No split
		}
	}

	k := h.config.PQK
	if k == 0 {
		k = 256
	}

	cfg := &PQConfig{
		Dimensions:    dims,
		NumSubVectors: m,
		NumCentroids:  k,
	}

	encoder, err := TrainPQEncoder(cfg, vectors, 15) // 15 iterations
	if err != nil {
		return err
	}

	h.pqEncoder = encoder
	// Update config to reflect enabled state
	h.config.PQEnabled = true
	h.config.PQM = m
	h.config.PQK = k

	// Note: We do NOT automatically re-encode existing vectors here because
	// ArrowHNSW stores vectors in GraphData chunks which are managed by Grow/ensureChunk.
	// If existing vectors need PQ, we would need to iterate and backfill.
	// For now, we assume TrainPQ is called BEFORE substantial data load or we implement backfill later.

	return nil
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// Get vector for distance calculations (and caching)
	vec, err := h.getVector(id)
	if err != nil {
		return err
	}
	return h.InsertWithVector(id, vec, level)
}

// InsertWithVector inserts a vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVector(id uint32, vec []float32, level int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWInsertDurationSeconds.Observe(time.Since(start).Seconds())
		metrics.HNSWNodesTotal.WithLabelValues("default").Set(float64(h.nodeCount.Load()))
	}()

	// 2. Check Capacity & SQ8/Vectors Status
	// Check dimensions lock-free
	// Check dimensions lock-free
	dims := int(h.dims.Load())

	data := h.data.Load()

	// Invariant: If dims > 0, Vectors/SQ8 arrays MUST exist in data.
	// We only check Capacity here. Structural integrity is guaranteed by Grow/Init order.
	if data.Capacity <= int(id) {
		h.Grow(int(id)+1, dims)
		data = h.data.Load()
	}

	// Lazy Chunk Allocation
	cID := chunkID(id)
	cOff := chunkOffset(id)
	var err error // Declare err to avoid shadowing data
	data, err = h.ensureChunk(data, cID, cOff, dims)
	if err != nil {
		return err
	}

	// Initialize the new node
	(*data.Levels[cID])[cOff] = uint8(level)

	// Cache dimensions on first insert
	if dims == 0 && len(vec) > 0 {
		h.initMu.Lock()
		dims = int(h.dims.Load())
		if dims == 0 {
			// Update dimensions atomically - CORRECT ORDER
			newDims := len(vec)

			// 1. Force Grow with new dimensions first.
			// This ensures 'data' structure is upgraded (Vectors allocated) BEFORE
			// we advertise the dimension change via h.dims.Store.
			h.Grow(data.Capacity, newDims)
			data = h.data.Load() // Reload updated data

			// 2. Advertise dimensions
			h.dims.Store(int32(newDims))
			dims = newDims

			// Re-ensure chunk with new dims
			// cID/cOff relies on ID which hasn't changed
			data, err = h.ensureChunk(data, cID, cOff, dims)
			if err != nil {
				h.initMu.Unlock()
				return err
			}
		} else {
			// Someone else initialized global dimensions while we waited.
			// Our local 'data' snapshot corresponds to dims=0 (likely no vectors).
			// We MUST reload data to get the snapshot that has the vectors allocated.
			data = h.data.Load()
		}
		h.initMu.Unlock()
	}

	// Recovery block removed: Invariant guarantees Vectors exist if dims > 0.

	// Store Dense Vector (Copy for L2 locality)
	if len(vec) > 0 && dims > 0 {
		// ensureChunk guarantees allocation (or recovery above)
		vecChunk := data.LoadVectorChunk(cID)
		if data.Vectors == nil || int(cID) >= len(data.Vectors) || vecChunk == nil {
			// This path indicates a critical synchronization failure or logic error in ensureChunk/Grow
			// Instead of panicking, return error to allow caller to handle/retry
			return fmt.Errorf("vector allocation failure for ID %d (dims=%d): inconsistent state", id, dims)
		}

		dest := (*vecChunk)[int(cOff)*dims : (int(cOff)+1)*dims]
		copy(dest, vec)
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
	sq8Handled := false
	if h.config.SQ8Enabled && dims > 0 {
		// First check if quantizer is trained (read-only, no lock needed for atomic check)
		needsTraining := h.quantizer == nil || !h.quantizer.IsTrained()

		if needsTraining {
			// Need to acquire lock for initialization/training
			h.growMu.Lock()
			// Double-check after acquiring lock (another goroutine may have initialized)
			if h.quantizer == nil {
				h.quantizer = NewScalarQuantizer(dims)
			}

			if !h.quantizer.IsTrained() {
				// Buffer vector for training
				vecCopy := make([]float32, len(vec))
				copy(vecCopy, vec)
				h.sq8TrainingBuffer = append(h.sq8TrainingBuffer, vecCopy)

				// Check if threshold reached
				threshold := h.config.SQ8TrainingThreshold
				if threshold <= 0 {
					threshold = 1000
				}

				if len(h.sq8TrainingBuffer) >= threshold {
					// Train!
					h.quantizer.Train(h.sq8TrainingBuffer)

					// Backfill existing vectors
					currentData := h.data.Load()
					maxID := h.locationStore.MaxID()
					for i := uint32(0); i <= uint32(maxID); i++ {
						cID := chunkID(i)
						cOff := chunkOffset(i)

						vecChunk := currentData.LoadVectorChunk(cID)
						if vecChunk == nil {
							continue
						}

						off := int(cOff) * dims
						if off+dims > len(*vecChunk) {
							continue
						}
						srcVec := (*vecChunk)[off : off+dims]

						// Encode to SQ8 chunk
						sq8Chunk := currentData.LoadSQ8Chunk(cID)
						if sq8Chunk != nil && off+dims <= len(*sq8Chunk) {
							dest := (*sq8Chunk)[off : off+dims]
							h.quantizer.Encode(srcVec, dest)
						}
					}

					// Clear buffer
					h.sq8TrainingBuffer = nil
				}
			}

			// If trained (now or before), encode current vector
			if h.quantizer.IsTrained() {
				cID := chunkID(id)
				cOff := chunkOffset(id)
				// Encode
				if data.VectorsSQ8 != nil {
					dest := (*data.VectorsSQ8[cID])[int(cOff)*dims : int(cOff+1)*dims]
					h.quantizer.Encode(vec, dest)
				}
			}

			sq8Handled = true
			h.growMu.Unlock()
		} else {
			// Quantizer is already trained, encode without lock
			cID := chunkID(id)
			cOff := chunkOffset(id)
			if data.VectorsSQ8 != nil && int(cID) < len(data.VectorsSQ8) && data.VectorsSQ8[cID] != nil {
				dest := (*data.VectorsSQ8[cID])[int(cOff)*dims : int(cOff+1)*dims]
				h.quantizer.Encode(vec, dest)
			}
			sq8Handled = true
		}
	}

	// Fallback encoding if not handled above
	if !sq8Handled && h.quantizer != nil {
		cID := chunkID(id)
		cOff := chunkOffset(id)
		if data.VectorsSQ8 != nil && int(cID) < len(data.VectorsSQ8) && data.VectorsSQ8[cID] != nil {
			dest := (*data.VectorsSQ8[cID])[int(cOff)*dims : int(cOff+1)*dims]
			h.quantizer.Encode(vec, dest)
		}
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

	// Determine efConstruction for this insertion
	// If adaptive ef is enabled, scale based on current graph size
	ef := h.efConstruction
	if h.config.AdaptiveEf {
		nodeCount := int(h.nodeCount.Load())
		ef = h.getAdaptiveEf(nodeCount)
	}

	// Insert at layers 0 to level
	for lc := level; lc >= 0; lc-- {
		// Find M nearest neighbors at this layer using adaptive ef
		candidates := h.searchLayerForInsert(ctx, vec, ep, ef, lc, data)

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
			h.AddConnection(ctx, data, id, neighbor.ID, lc, maxConn)
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn)

			// Prune neighbor if needed (still useful to keep strict M_max if buffer wasn't hit)
			// Read count atomically
			// Read count atomically
			// Read count atomically
			cID := chunkID(neighbor.ID)
			cOff := chunkOffset(neighbor.ID)

			countsChunk := data.LoadCountsChunk(lc, cID)
			if countsChunk == nil {
				// Chunk not yet loaded in this data snapshot, reload data
				data = h.data.Load() // Reload the outer 'data' variable
				countsChunk = data.LoadCountsChunk(lc, cID)
				if countsChunk == nil {
					// Still nil after reload, something is wrong, skip pruning for this neighbor
					continue
				}
			}

			count := atomic.LoadInt32(&(*countsChunk)[cOff])
			if int(count) > maxConn {
				h.PruneConnections(ctx, data, neighbor.ID, maxConn, lc)
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
func (h *ArrowHNSW) searchLayerForInsert(ctx *ArrowSearchContext, query []float32, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	ctx.visited.Clear()
	ctx.candidates.Clear()
	// Ensure visited bitset is large enough
	// Use maxID from location store to cover all possible IDs, including the one currently being inserted
	// Add safety margin
	maxID := int(h.locationStore.MaxID()) + 1000
	if ctx.visited == nil || ctx.visited.Size() < maxID {
		ctx.visited = NewArrowBitset(maxID)
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

	if h.metric == MetricEuclidean && h.quantizer != nil && h.quantizer.IsTrained() {
		querySQ8 = h.quantizer.Encode(query, nil)
		// Access SQ8 data for entryPoint
		cID := chunkID(entryPoint)
		cOff := chunkOffset(entryPoint)
		off := int(cOff) * int(h.dims.Load())
		dims := int(h.dims.Load())
		vecSQ8Chunk := data.LoadSQ8Chunk(cID)
		if vecSQ8Chunk != nil && off+dims <= len(*vecSQ8Chunk) {
			dSQ8 := simd.EuclideanDistanceSQ8(querySQ8, (*vecSQ8Chunk)[off:off+dims])
			entryDist = float32(dSQ8)
		} else {
			// Fallback if not quantized yet? Should not happen if strictly maintained.
			entryDist = 0 // Or MaxFloat
		}
	} else {
		entryDist = h.distFunc(query, h.mustGetVectorFromData(data, entryPoint))
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
	if maxOps < 10000 {
		maxOps = 10000
	}

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
		// Collect unvisited neighbors with Seqlock retry (now handled by GetNeighbors)
		var unvisitedIDs []uint32

		if cap(ctx.scratchNeighbors) < MaxNeighbors {
			ctx.scratchNeighbors = make([]uint32, MaxNeighbors)
		}
		rawNeighbors := data.GetNeighbors(layer, curr.ID, ctx.scratchNeighbors)

		// Filter unvisited
		// scratchIDs is reused for unvisited list
		ctx.scratchIDs = ctx.scratchIDs[:0]
		for _, nid := range rawNeighbors {
			if !visited.IsSet(nid) {
				ctx.scratchIDs = append(ctx.scratchIDs, nid)
			}
		}
		unvisitedIDs = ctx.scratchIDs

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

		if h.metric == MetricEuclidean && h.quantizer != nil && len(data.VectorsSQ8) > 0 {
			// SQ8 Path
			// SQ8 Path: Batch SIMD
			if cap(ctx.scratchVecsSQ8) < count {
				ctx.scratchVecsSQ8 = make([][]byte, count*2)
			}
			vecsSQ8 := ctx.scratchVecsSQ8[:count]

			// Collect vectors
			// We track invalid indices to overwrite their distance later
			hasInvalid := false

			for i, nid := range unvisitedIDs {
				cID := chunkID(nid)
				cOff := chunkOffset(nid)
				off := int(cOff) * int(h.dims.Load())
				dims := int(h.dims.Load())

				vecSQ8Chunk := data.LoadSQ8Chunk(cID)
				if vecSQ8Chunk != nil && off+dims <= len(*vecSQ8Chunk) {
					vecsSQ8[i] = (*vecSQ8Chunk)[off : off+dims]
				} else {
					// Invalid/Missing vector: use query as safe dummy (dist=0)
					vecsSQ8[i] = querySQ8
					hasInvalid = true
					// We'll mark this as MaxFloat AFTER batch calc.
					// To avoid another loop or allocation, we could use bitset,
					// but simply checking validity again is fast (cached pointers).
					// Actually, simpler: write MaxFloat to 'dists' now?
					// No, batch function overwrites 'dists'.
					// We need to fixup later.
				}
			}

			simd.EuclideanDistanceSQ8Batch(querySQ8, vecsSQ8, dists)

			if hasInvalid {
				// Fixup invalid entries
				for i, nid := range unvisitedIDs {
					cID := chunkID(nid)
					// Quick re-check or logic?
					// Since we don't want to re-calculate offsets, maybe just re-check nil?
					// Re-calculating bounds is cheap.
					vecSQ8Chunk := data.LoadSQ8Chunk(cID)
					if vecSQ8Chunk == nil {
						dists[i] = math.MaxFloat32
						continue
					}
					// If we are here, chunk exists. Check offset.
					cOff := chunkOffset(nid)
					dims := int(h.dims.Load())
					off := int(cOff) * dims
					if off+dims > len(*vecSQ8Chunk) {
						dists[i] = math.MaxFloat32
					}
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
			h.batchDistFunc(query, vecs, dists)
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
		if a.Dist < b.Dist {
			return -1
		}
		if a.Dist > b.Dist {
			return 1
		}
		return 0
	})

	// Sync context results
	ctx.scratchResults = results
	return results
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(ctx *ArrowSearchContext, candidates []Candidate, m int, data *GraphData) []Candidate {
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
		// Fallback for tests/cases without context
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

	var discarded []Candidate
	if ctx != nil {
		discarded = ctx.scratchDiscarded[:0]
	} else {
		discarded = make([]Candidate, 0, m)
	}

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

			if h.metric == MetricEuclidean && h.quantizer != nil && h.quantizer.IsTrained() && len(data.VectorsSQ8) > 0 {
				// SQ8 Path
				cID := chunkID(selCand.ID)
				cOff := chunkOffset(selCand.ID)
				dims := int(h.dims.Load())
				offSel := int(cOff) * dims

				vecSQ8Chunk := data.LoadSQ8Chunk(cID)
				if vecSQ8Chunk != nil && offSel+dims <= len(*vecSQ8Chunk) {
					vecSel := (*vecSQ8Chunk)[offSel : offSel+dims]
					// Batch SIMD
					if cap(ctx.scratchVecsSQ8) < count {
						ctx.scratchVecsSQ8 = make([][]byte, count*2)
					}
					vecsSQ8 := ctx.scratchVecsSQ8[:count]
					hasInvalid := false

					for i := range remaining {
						nCID := chunkID(remaining[i].ID)
						nCOff := chunkOffset(remaining[i].ID)
						offRem := int(nCOff) * dims

						nVecSQ8Chunk := data.LoadSQ8Chunk(nCID)
						if nVecSQ8Chunk != nil && offRem+dims <= len(*nVecSQ8Chunk) {
							vecsSQ8[i] = (*nVecSQ8Chunk)[offRem : offRem+dims]
						} else {
							vecsSQ8[i] = vecSel // Safe dummy
							hasInvalid = true
						}
					}

					simd.EuclideanDistanceSQ8Batch(vecSel, vecsSQ8, dists)

					if hasInvalid {
						for i := range remaining {
							nCID := chunkID(remaining[i].ID)
							nVecSQ8Chunk := data.LoadSQ8Chunk(nCID)
							if nVecSQ8Chunk == nil {
								dists[i] = math.MaxFloat32
								continue
							}
							nCOff := chunkOffset(remaining[i].ID)
							offRem := int(nCOff) * dims
							if offRem+dims > len(*nVecSQ8Chunk) {
								dists[i] = math.MaxFloat32
							}
						}
					}
				} else {
					for i := range dists {
						dists[i] = math.MaxFloat32
					}
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

				if h.batchComputer != nil && h.metric == MetricEuclidean {
					// Use batch computer only for Euclidean if it's L2 optimized
					_, _ = h.batchComputer.ComputeL2DistancesInto(selVec, remVecs, dists)
				} else {
					h.batchDistFunc(selVec, remVecs, dists)
				}
			}

			// Filter remaining
			filteredCount := 0
			for i, cand := range remaining {
				if dists[i]*alpha > cand.Dist {
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
	if ctx != nil {
		ctx.scratchDiscarded = discarded
	}

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

// AddConnection adds a directed edge from source to target at the given layer.
func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *GraphData, source, target uint32, layer int, maxConn int) {
	// Acquire lock for the specific node (shard)
	lockID := source % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	cID := chunkID(source)
	cOff := chunkOffset(source)

	// Ensure chunk exists (it should, as we resized before insert)
	countsChunk := data.LoadCountsChunk(layer, cID)
	neighborsChunk := data.LoadNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// This indicates a stale data snapshot or a logic error.
		// Reload data and try again, or return error. For now, reload.
		data = h.data.Load()
		countsChunk = data.LoadCountsChunk(layer, cID)
		neighborsChunk = data.LoadNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			// Still nil, cannot proceed. This should ideally not happen if Grow/ensureChunk works.
			return
		}
	}

	// Check for duplicates first to ensure idempotency
	// This requires reading current neighbors
	currentCount := atomic.LoadInt32(&(*countsChunk)[cOff])
	baseIdx := int(cOff) * MaxNeighbors

	for i := 0; i < int(currentCount); i++ {
		if (*neighborsChunk)[baseIdx+i] == target {
			return // Already connected
		}
	}

	// 3a. Update metadata (Counts)
	// Counts[level][chunkID][chunkOffset] atomic increment
	countAddr := &(*countsChunk)[cOff]
	newCount := atomic.AddInt32(countAddr, 1)

	// Slot index is count - 1
	slot := int(newCount) - 1
	if slot >= MaxNeighbors {
		// Should not happen if MaxNeighbors >> maxConn and pruning works
		// But if it does, decrement and return
		atomic.AddInt32(countAddr, -1)
		return
	}

	// Seqlock write start: increment version to odd
	verChunk := data.LoadVersionsChunk(layer, cID)
	if verChunk == nil {
		// Should not happen if countsChunk/neighborsChunk are valid
		return
	}
	verAddr := &(*verChunk)[cOff]
	atomic.AddUint32(verAddr, 1)

	// Atomic store to satisfy race detector
	atomic.StoreUint32(&(*neighborsChunk)[baseIdx+slot], target)

	// Seqlock write: increment version (even = clean)
	atomic.AddUint32(verAddr, 1)

	// Prune if needed
	if int(newCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
	}
}

// PruneConnections reduces the number of connections to maxConn using the heuristic.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	// Acquire lock for the specific node
	lockID := nodeID % 1024
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	h.pruneConnectionsLocked(ctx, data, nodeID, maxConn, layer)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

	countsChunk := data.LoadCountsChunk(layer, cID)
	neighborsChunk := data.LoadNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		// Data snapshot is stale or corrupted, reload and retry
		data = h.data.Load()
		countsChunk = data.LoadCountsChunk(layer, cID)
		neighborsChunk = data.LoadNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return // Cannot prune if chunks are missing
		}
	}

	countAddr := &(*countsChunk)[cOff]
	count := int(atomic.LoadInt32(countAddr))

	if count <= maxConn {
		return
	}

	// Collect all current neighbors as candidates
	baseIdx := int(cOff) * MaxNeighbors

	var dists []float32
	if ctx != nil {
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists = ctx.scratchDists[:count]
	} else {
		dists = make([]float32, count)
	}

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

	// Logic Switch: SQ8 vs Float32
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		// SQ8 Path
		// Node itself is in VectorsSQ8
		dims := int(h.dims.Load())
		offNode := int(cOff) * dims
		nodeSQ8Chunk := data.LoadSQ8Chunk(cID)
		if nodeSQ8Chunk != nil && offNode+dims <= len(*nodeSQ8Chunk) {
			nodeSQ8 := (*nodeSQ8Chunk)[offNode : offNode+dims]

			for i := 0; i < count; i++ {
				neighborID := (*neighborsChunk)[baseIdx+i]

				// Neighbor chunk access
				nCID := chunkID(neighborID)
				nCOff := chunkOffset(neighborID)
				offRem := int(nCOff) * dims

				nVecSQ8Chunk := data.LoadSQ8Chunk(nCID)
				if nVecSQ8Chunk != nil && offRem+dims <= len(*nVecSQ8Chunk) {
					d := simd.EuclideanDistanceSQ8(nodeSQ8, (*nVecSQ8Chunk)[offRem:offRem+dims])
					dists[i] = float32(d)
				} else {
					dists[i] = math.MaxFloat32
				}
			}
		} else {
			for i := 0; i < count; i++ {
				dists[i] = math.MaxFloat32
			}
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
			neighborID := (*neighborsChunk)[baseIdx+i]
			vec := h.mustGetVectorFromData(data, neighborID)
			neighborVecs[i] = vec

			// Compute distance once for initial Candidate
			dists[i] = simd.EuclideanDistance(nodeVec, vec)
		}
	}

	// Populate candidates with IDs and distances
	for i := 0; i < count; i++ {
		candidates[i] = Candidate{ID: (*neighborsChunk)[baseIdx+i], Dist: dists[i]}
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
	verChunk := data.LoadVersionsChunk(layer, cID)
	if verChunk == nil {
		// Should not happen if other chunks exist
		return
	}
	verAddr := &(*verChunk)[cOff]
	atomic.AddUint32(verAddr, 1)

	for i := 0; i < newCount; i++ {
		atomic.StoreUint32(&(*neighborsChunk)[baseIdx+i], selected[i].ID)
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
	// Check if we can store in dense vector storage
	if vecChunk := data.LoadVectorChunk(cID); vecChunk != nil {
		start := int(cOff) * int(h.dims.Load())
		dims := int(h.dims.Load())
		if start+dims <= len(*vecChunk) {
			return (*vecChunk)[start : start+dims]
		}
	}

	// Fallback
	vec, err := h.getVector(id)
	if err != nil {
		// Return zero vector to avoid panic in distance calc
		dims := int(h.dims.Load())
		return make([]float32, dims)
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

	// Cap at ArrowMaxLayers - 1
	if level >= ArrowMaxLayers {
		level = ArrowMaxLayers - 1
	}

	return level
}
