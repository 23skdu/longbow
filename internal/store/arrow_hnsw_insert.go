package store

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"math/rand"
	"slices"
	"sync"
	"sync/atomic"

	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
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
		switch {
		case dims%8 == 0:
			m = dims / 8
		case dims%4 == 0:
			m = dims / 4
		default:
			m = 1 // No split
		}
	}

	k := h.config.PQK
	if k == 0 {
		k = 256
	}

	encoder, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return err
	}

	if err := encoder.Train(vectors); err != nil {
		return err
	}

	h.pqEncoder = encoder
	// Update config to reflect enabled state
	h.config.PQEnabled = true
	h.config.PQM = m
	h.config.PQK = k

	// Backfill existing vectors
	data := h.data.Load()

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 100
		}

		if count == threshold {
			h.adjustMParameter(data, threshold)
		}
	}
	if data != nil {
		// If data doesn't have VectorsPQ allocated, we need to upgrade it
		// Grow will check PQDims and PQEnabled
		h.growNoLock(data.Capacity, data.Dims)
		data = h.data.Load()

		if data.VectorsPQ != nil {
			nodeCount := int(h.nodeCount.Load())
			for i := uint32(0); i < uint32(nodeCount); i++ {
				v := h.mustGetVectorFromData(data, i)
				if v == nil {
					continue
				}
				vf32, ok := v.([]float32)
				if !ok {
					continue
				}
				code, err := encoder.Encode(vf32)
				if err != nil {
					continue
				}
				cID := chunkID(i)
				cOff := chunkOffset(i)

				// Ensure PQ chunk exists (lazily allocated)
				data = h.ensureChunk(data, cID, cOff, data.Dims)

				if chunk := data.GetVectorsPQChunk(cID); chunk != nil {
					copy(chunk[int(cOff)*m:(int(cOff)+1)*m], code)
				}
			}
		}
	}

	return nil
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// Zero-Copy FP16 Ingestion Path
	if h.config.Float16Enabled {
		vecF16, err := h.getVectorF16(id)
		if err == nil {
			return h.InsertWithVectorF16(id, vecF16, level)
		}
		// If error (e.g. not found), we could return error or fall through?
		// getVectorF16 handles F32->F16 conversion too.
		return err
	}

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
		h.metricInsertDuration.Observe(time.Since(start).Seconds())
		nodeCount := float64(h.nodeCount.Load())
		h.metricNodeCount.Set(nodeCount)
		if h.config.BQEnabled {
			h.metricBQVectors.Set(nodeCount)
		}
	}()

	// Make a defensive copy of the vector is avoided by copying into GraphData first.
	// We rely on the copy at line 200 to provide a stable reference.

	// 2. Check Capacity & SQ8/Vectors Status
	// Check dimensions lock-free
	dims := int(h.dims.Load())

	data := h.data.Load()

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 100
		}

		if count == threshold {
			h.adjustMParameter(data, threshold)
		}
	}

	// Invariant: If dims > 0, Vectors/SQ8 arrays MUST exist in data.
	// We check both Capacity and existence of structures.
	if data.Capacity <= int(id) || (dims > 0 && data.Vectors == nil) {
		h.Grow(int(id)+1, dims)
		data = h.data.Load()
	}

	// Lazy Chunk Allocation
	cID := chunkID(id)
	cOff := chunkOffset(id)
	data = h.ensureChunk(data, cID, cOff, dims)

	// Acquire search context from pool early to use its scratch buffers
	metrics.HNSWInsertPoolGetTotal.Inc()
	ctx := h.searchPool.Get().(*ArrowSearchContext)
	ctx.Reset()
	defer func() {
		metrics.HNSWInsertPoolPutTotal.Inc()
		h.searchPool.Put(ctx)
	}()

	// Initialize the new node
	// Access the chunk pointer atomically
	levelsChunk := data.GetLevelsChunk(cID)
	// We ensured chunk exists, so levelsChunk should not be nil.
	// We ensured chunk exists, so levelsChunk should not be nil.
	levelsChunk[cOff] = uint8(level)

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
			data = h.ensureChunk(data, cID, cOff, dims)
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
		if err := data.SetVectorFromFloat32(id, vec); err != nil {
			return err
		}
	}

	// Store Binary Quantized Vector
	if h.config.BQEnabled && dims > 0 {
		// Initialize Encoder if needed (lazy)
		if h.bqEncoder == nil {
			h.initMu.Lock()
			if h.bqEncoder == nil {
				h.bqEncoder = NewBQEncoder(dims)
			}
			h.initMu.Unlock()
		}

		encoded := h.bqEncoder.Encode(vec)
		encodedChunk := data.GetVectorsBQChunk(cID)
		if encodedChunk != nil {
			numWords := len(encoded)
			baseIdx := int(cOff) * numWords
			copy(encodedChunk[baseIdx:baseIdx+numWords], encoded)
		}

		// Also populate search context for insertion search phase
		numWords := len(encoded)
		if cap(ctx.queryBQ) < numWords {
			ctx.queryBQ = make([]uint64, numWords)
		}
		ctx.queryBQ = ctx.queryBQ[:numWords]
		copy(ctx.queryBQ, encoded)
	}

	// Store PQ Vector
	if h.config.PQEnabled && h.pqEncoder != nil && dims > 0 {
		code, err := h.pqEncoder.Encode(vec)
		if err == nil {
			encodedChunk := data.GetVectorsPQChunk(cID)
			if encodedChunk != nil {
				pqM := data.PQDims
				if pqM > 0 {
					baseIdx := int(cOff) * pqM
					copy(encodedChunk[baseIdx:baseIdx+pqM], code)
				}
			}
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
	// Strategy: Use local buffer for encoding, then copy to shared memory
	// This avoids race conditions from concurrent writes to the same chunk
	var localSQ8Buffer []byte
	sq8Handled := false

	if h.config.SQ8Enabled && dims > 0 {
		h.ensureTrained(int(h.locationStore.MaxID()), [][]float32{vec})
	}

	// Encode to local buffer if quantizer is trained (and ready)
	if h.sq8Ready.Load() {
		if cap(ctx.querySQ8) < dims {
			ctx.querySQ8 = make([]byte, dims)
		}
		ctx.querySQ8 = ctx.querySQ8[:dims]
		h.quantizer.Encode(vec, ctx.querySQ8)
		localSQ8Buffer = ctx.querySQ8

		// Now copy local buffer to shared memory (single write operation)
		cID := chunkID(id)
		cOff := chunkOffset(id)
		// Check if SQ8 vector array is allocated for this chunk
		if chunk := data.GetVectorsSQ8Chunk(cID); chunk != nil {
			offset := int(cOff) * dims
			// Bounds check (though ensureChunk should guarantee size)
			if offset+dims <= len(chunk) {
				dest := chunk[offset : offset+dims]
				copy(dest, localSQ8Buffer)
			}
		}
		sq8Handled = true
	}

	// Fallback encoding if not handled above
	if !sq8Handled && h.sq8Ready.Load() {
		cID := chunkID(id)
		cOff := chunkOffset(id)

		if chunk := data.GetVectorsSQ8Chunk(cID); chunk != nil {
			// Use local buffer
			if cap(ctx.querySQ8) < dims {
				ctx.querySQ8 = make([]byte, dims)
			}
			ctx.querySQ8 = ctx.querySQ8[:dims]
			h.quantizer.Encode(vec, ctx.querySQ8)
			localSQ8Buffer = ctx.querySQ8

			offset := int(cOff) * dims
			if offset+dims <= len(chunk) {
				dest := chunk[offset : offset+dims]
				copy(dest, localSQ8Buffer)
			}
		}
	}

	// Search for nearest neighbors at each layer
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Fix for Growth Race:
	// If the entry point 'ep' was inserted by another thread that forced a Grow(),
	// our local 'data' snapshot might be too small (stale) to contain 'ep'.
	// We must reload 'data' to avoid out-of-bounds access during search.
	if int(ep) >= data.Capacity {
		// Reload latest data which matches current entry point
		data = h.data.Load()

		// Validate again - if it fails now, something is very wrong (corruption)
		if int(ep) >= data.Capacity {
			return fmt.Errorf("entry point %d beyond capacity %d even after reload", ep, data.Capacity)
		}

		// Re-ensure OUR chunk exists in this new data view for safety
		// (though likely it does if Grow copied it, or we allocate it again)
		// We need to ensure we can write links to our node later.
		data = h.ensureChunk(data, cID, cOff, dims)
	}

	// Search from top layer down to level+1

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
			h.AddConnection(ctx, data, id, neighbor.ID, lc, maxConn, neighbor.Dist)
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn, neighbor.Dist)

			// Prune neighbor if needed (still useful to keep strict M_max if buffer wasn't hit)
			// Read count atomically
			cID := chunkID(neighbor.ID)
			cOff := chunkOffset(neighbor.ID)

			countsChunk := data.GetCountsChunk(lc, cID)
			if countsChunk == nil {
				// Chunk not yet loaded in this data snapshot, reload data
				data = h.data.Load() // Reload the outer 'data' variable
				countsChunk = data.GetCountsChunk(lc, cID)
				if countsChunk == nil {
					// Still nil after reload, something is wrong, skip pruning for this neighbor
					continue
				}
			}

			count := atomic.LoadInt32(&countsChunk[cOff])
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

// ensureTrained checks if SQ8 training is needed and performs it if sufficient data is accumulated.
// limitID specifies the max ID to backfill (inclusive).
func (h *ArrowHNSW) ensureTrained(limitID int, extraSamples [][]float32) {
	if h.sq8Ready.Load() {
		return
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	if h.sq8Ready.Load() {
		return
	}

	dims := int(h.dims.Load())
	if h.quantizer == nil {
		h.quantizer = NewScalarQuantizer(dims)
	}

	if h.quantizer.IsTrained() {
		h.sq8Ready.Store(true)
		return
	}

	// Buffer vectors for training
	for _, v := range extraSamples {
		vecCopy := make([]float32, len(v))
		copy(vecCopy, v)
		h.sq8TrainingBuffer = append(h.sq8TrainingBuffer, vecCopy)
	}

	// Check if threshold reached
	threshold := h.config.SQ8TrainingThreshold
	if threshold <= 0 {
		threshold = 1000
	}

	if len(h.sq8TrainingBuffer) >= threshold {
		// Train!
		h.quantizer.Train(h.sq8TrainingBuffer)

		// Backfill existing vectors
		if limitID >= 0 {
			currentData := h.data.Load()
			for i := uint32(0); i <= uint32(limitID); i++ {
				cID := chunkID(i)
				cOff := chunkOffset(i)

				vecChunk := currentData.GetVectorsChunk(cID)
				if vecChunk == nil {
					continue
				}

				off := int(cOff) * dims
				if off+dims > len(vecChunk) {
					continue
				}
				srcVec := vecChunk[off : off+dims]

				// Encode to SQ8 chunk
				// We MUST check if chunk exists, if not, we can't write.
				// But ensureChunk should have allocated it if we have SQ8Enabled.
				sq8Chunk := currentData.GetVectorsSQ8Chunk(cID)
				if sq8Chunk != nil && off+dims <= len(sq8Chunk) {
					dest := sq8Chunk[off : off+dims]
					h.quantizer.Encode(srcVec, dest)
				}
			}
		}

		// Clear buffer
		h.sq8TrainingBuffer = nil
		h.sq8Ready.Store(true)
	}
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(ctx *ArrowSearchContext, query []float32, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	computer := h.resolveHNSWComputer(data, ctx, query)
	_ = h.searchLayer(computer, entryPoint, ef, layer, ctx, data, nil)

	// Transfer results from resultSet to a slice
	res := make([]Candidate, ctx.resultSet.Len())
	for i := len(res) - 1; i >= 0; i-- {
		c, _ := ctx.resultSet.Pop()
		res[i] = c
	}
	return res
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
	var selected []Candidate //nolint:prealloc // conditional allocation
	var remaining []Candidate

	if ctx != nil {
		if cap(ctx.scratchSelected) < m {
			ctx.scratchSelected = make([]Candidate, 0, m*2)
		}
		selected = ctx.scratchSelected[:0]

		if cap(ctx.scratchRemaining) < len(candidates) {
			ctx.scratchRemaining = make([]Candidate, len(candidates)*2)
		}
		remaining = ctx.scratchRemaining[:len(candidates)]

		// Initialize/Reset Request Batches
		if ctx.selectedBatch == nil {
			ctx.selectedBatch = h.newVectorBatch(data)
		} else {
			ctx.selectedBatch.Reset()
		}
		if ctx.remainingBatch == nil {
			ctx.remainingBatch = h.newVectorBatch(data)
		} else {
			ctx.remainingBatch.Reset()
		}

		// Map variables to batches for clarity (optional, or just use ctx.X)
	} else {
		// Fallback for tests/cases without context
		selected = make([]Candidate, 0, m)
		remaining = make([]Candidate, len(candidates))
		// We need transient batches here too.
		// Since we don't have context, we creating new batches implies allocation.
		// This path is for tests mostly.
		ctx = &ArrowSearchContext{
			selectedBatch:  h.newVectorBatch(data),
			remainingBatch: h.newVectorBatch(data),
		}
	}
	copy(remaining, candidates)

	// Pre-fetch vectors once
	for i := range candidates {
		// Add returns bool, but here we just want to ensure it's added.
		// If fetch fails, we might just have nil inside the batch (handled by implementation)
		// but VectorBatch interface `Add` returns false on failure.
		// How to handle partial failure in batch?
		// float32VectorBatch.Add appends. If it fails (nil), it returns false and DOES NOT append.
		// So batch size < candidates size.
		// This will desync `remaining` (Candidates) and `remainingBatch` (Vectors).
		// We must ensure sync.
		added := ctx.remainingBatch.Add(candidates[i].ID)
		if !added {
			// If we fail to get vector, we should probably treat it as a "nil" vector
			// or remove the candidate?
			// Existing logic set `remainingVecs[i] = nil`.
			// To maintain sync with `remaining` index, `VectorBatch` should probably support adding nil?
			// Or we manually add "nil" or "zero" representation?
			// My `Add` implementation doesn't support adding nil.
			// Let's modify `Add` to append nil placehoder or simply use AddVec(nil)?
			ctx.remainingBatch.AddVec(nil)
		}
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
		selVecAny := ctx.remainingBatch.Get(bestIdx)

		selected = append(selected, selCand)
		ctx.selectedBatch.AddVec(selVecAny)

		// Remove from remaining (O(1) swap and pop)
		lastIdx := len(remaining) - 1
		remaining[bestIdx] = remaining[lastIdx]
		remaining = remaining[:lastIdx]
		ctx.remainingBatch.Swap(bestIdx, lastIdx)
		ctx.remainingBatch.Pop()

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

				vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
				if vecSQ8Chunk != nil && offSel+dims <= len(vecSQ8Chunk) {
					vecSel := vecSQ8Chunk[offSel : offSel+dims]
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

						nVecSQ8Chunk := data.GetVectorsSQ8Chunk(nCID)
						if nVecSQ8Chunk != nil && offRem+dims <= len(nVecSQ8Chunk) {
							vecsSQ8[i] = nVecSQ8Chunk[offRem : offRem+dims]
						} else {
							vecsSQ8[i] = vecSel // Safe dummy
							hasInvalid = true
						}
					}

					simd.EuclideanDistanceSQ8Batch(vecSel, vecsSQ8, dists)

					if hasInvalid {
						for i := range remaining {
							nCID := chunkID(remaining[i].ID)
							nVecSQ8Chunk := data.GetVectorsSQ8Chunk(nCID)
							if nVecSQ8Chunk == nil {
								dists[i] = math.MaxFloat32
								continue
							}
							nCOff := chunkOffset(remaining[i].ID)
							offRem := int(nCOff) * dims
							if offRem+dims > len(nVecSQ8Chunk) {
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
				// Float32 Path / Default Path
				// Replaced h.batchComputer / h.batchDistFunc with VectorBatch.ComputeDistances
				ctx.remainingBatch.ComputeDistances(selVecAny, dists)
			}

			// Filter remaining
			filteredCount := 0
			for i, cand := range remaining {
				if dists[i]*alpha > cand.Dist {
					if i != filteredCount {
						remaining[filteredCount] = cand
						ctx.remainingBatch.Swap(i, filteredCount)
					}
					filteredCount++
				} else {
					discarded = append(discarded, cand)
				}
			}
			remaining = remaining[:filteredCount]
			// Pop truncated elements from batch
			currentBatchLen := ctx.remainingBatch.Len()
			for k := 0; k < currentBatchLen-filteredCount; k++ {
				ctx.remainingBatch.Pop()
			}
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
func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *GraphData, source, target uint32, layer, maxConn int, dist float32) {
	// Acquire lock for the specific node (shard)
	lockID := source % ShardedLockCount
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	cID := chunkID(source)
	cOff := chunkOffset(source)

	// Ensure chunk exists (it should, as we resized before insert)
	// Promote node from Disk if needed (Copy-On-Write)
	data = h.promoteNode(data, source)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		// This indicates a stale data snapshot or a logic error.
		// Reload data and try again, or return error. For now, reload.
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			// Still nil, cannot proceed. This should ideally not happen if Grow/ensureChunk works.
			return
		}
	}

	// Check for duplicates first to ensure idempotency
	// This requires reading current neighbors
	currentCount := atomic.LoadInt32(&countsChunk[cOff])
	baseIdx := int(cOff) * MaxNeighbors

	for i := 0; i < int(currentCount); i++ {
		if atomic.LoadUint32(&neighborsChunk[baseIdx+i]) == target {
			return // Already connected
		}
	}

	// 3a. Update metadata (Counts)
	// Counts[level][chunkID][chunkOffset] atomic increment
	// 3a. Prepare for write
	// Calculate slot using currentCount (stable under shard lock)
	slot := int(currentCount)
	if slot >= MaxNeighbors {
		// Should not happen if MaxNeighbors >> maxConn and pruning works
		return
	}

	// Seqlock write start: increment version to odd
	verChunk := data.GetVersionsChunk(layer, cID)
	if verChunk == nil {
		return
	}
	verAddr := &verChunk[cOff]
	atomic.AddUint32(verAddr, 1)

	// Atomic store to satisfy race detector
	atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)

	// Update metadata (Counts) - make visible AFTER data write
	countAddr := &countsChunk[cOff]
	newCount := atomic.AddInt32(countAddr, 1)

	// Seqlock write: increment version (even = clean)
	atomic.AddUint32(verAddr, 1)

	// --- Packed Neighbors Integration (v0.1.4-rc1) ---
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		// Get existing neighbors from packed to check for duplicates (though we checked above)
		existing, ok := pn.GetNeighbors(source)
		if !ok || len(existing) == 0 {
			if h.config.Float16Enabled {
				pn.SetNeighborsF16(source, []uint32{target}, []float16.Num{float16.New(dist)})
			} else {
				pn.SetNeighbors(source, []uint32{target})
			}
		} else {
			// Check if already present in packed
			found := false
			for _, nid := range existing {
				if nid == target {
					found = true
					break
				}
			}
			if !found {
				newNeighbors := make([]uint32, len(existing)+1)
				copy(newNeighbors, existing)
				newNeighbors[len(existing)] = target

				// Optional: Store distances if Float16 enabled
				if h.config.Float16Enabled {
					// We need to manage distances too
					_, existingDists, f16ok := pn.GetNeighborsF16(source)
					newDists := make([]float16.Num, len(existing)+1)
					if f16ok && len(existingDists) == len(existing) {
						copy(newDists, existingDists)
					} else {
						// Create dummy distances if missing
						for i := range existing {
							newDists[i] = float16.New(0)
						}
					}
					newDists[len(existing)] = float16.New(dist)
					pn.SetNeighborsF16(source, newNeighbors, newDists)
				} else {
					pn.SetNeighbors(source, newNeighbors)
				}
			}
		}
	}

	// Prune if needed
	if int(newCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
	}
}

// AddConnectionsBatch adds multiple directed edges to a single target node at the given layer.
// This is an optimized version of AddConnection for batch scenarios to reduce lock contention.
func (h *ArrowHNSW) AddConnectionsBatch(ctx *ArrowSearchContext, data *GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) {
	if len(sources) == 0 {
		return
	}

	// Acquire lock for the target node
	lockID := target % ShardedLockCount
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	cID := chunkID(target)
	cOff := chunkOffset(target)

	// Ensure chunk exists
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	if countsChunk == nil || neighborsChunk == nil {
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return
		}
	}

	// Read current state
	countAddr := &countsChunk[cOff]
	currentCount := atomic.LoadInt32(countAddr)

	// Collect actual additions (filter duplicates)
	// We read current neighbors to check against incoming sources.
	// Since duplicates are rare in HNSW construction (unless duplicate vectors),
	// we can do a simple check.
	baseIdx := int(cOff) * MaxNeighbors

	// Identify distinct new sources that are NOT already connected
	// Optimization: Use a small stack map or simple loop if small.
	var toAddIdxs []int

	for i, src := range sources {
		found := false
		for j := 0; j < int(currentCount); j++ {
			if atomic.LoadUint32(&neighborsChunk[baseIdx+j]) == src {
				found = true
				break
			}
		}
		if !found {
			toAddIdxs = append(toAddIdxs, i)
		}
	}

	if len(toAddIdxs) == 0 {
		return
	}

	// Double check capacity
	// In extremely rare cases (huge batch), we might exceed MaxNeighbors even before pruning?
	// MaxNeighbors is usually 64-128. If we add 100, we overflow.
	// HNSW paper allows temporary overflow or hard cap.
	// Our storage is fixed size `MaxNeighbors`. We CANNOT exceed it.
	// So we must bound `toAddIdxs` to fit available space OR implement a "pre-prune" strategy.
	// A simpler safe approach: Cap at MaxNeighbors - currentCount

	available := MaxNeighbors - int(currentCount)
	if available < 0 {
		available = 0
	}

	// If we have more new edges than space, we should prioritize?
	// But we haven't computed distances of *existing* edges here to compare.
	// Standard HNSW connects and then prunes. If we can't fit, we have a problem with fixed storage.
	// Longbow `MaxNeighbors` should be `M_max * 2` or similar to allow overflow.
	// If `mMax` ~= 32 and `MaxNeighbors` ~= 64, we have room for 32 additions.
	if len(toAddIdxs) > available {
		// Just take the first N that fit.
		// Or better: Pre-sort incoming by distance and take best?
		// Assuming sources/dists are correlated.
		// Let's implement pre-sorting if we are overflowing.
		// For now simple truncation to avoid buffer overflow panic.
		toAddIdxs = toAddIdxs[:available]
	}

	if len(toAddIdxs) == 0 {
		return
	}

	// Perform Writes
	verChunk := data.GetVersionsChunk(layer, cID)
	var verAddr *uint32
	if verChunk != nil {
		verAddr = &verChunk[cOff]
		atomic.AddUint32(verAddr, 1) // Odd = dirty
	}

	for _, idx := range toAddIdxs {
		slot := int(currentCount)
		src := sources[idx]

		atomic.StoreUint32(&neighborsChunk[baseIdx+slot], src)
		currentCount++
		// We don't update atomic count yet, we do it at once or incrementally?
		// Incrementally is safer if we crash? No, batch write.
	}

	// Update Count Atomically
	atomic.StoreInt32(countAddr, currentCount)

	if verAddr != nil {
		atomic.AddUint32(verAddr, 1) // Even = clean
	}

	// Packed Neighbors Updates
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		// This is expensive: Read-Modify-Write Packed Neighbors
		// But AddConnection does it per edge. Here we batch.
		// "SetNeighbors" implements replacement or merge?
		// Looking at usage in AddConnection, it seems to be "Set" (overwrite) or manual merge?
		// AddConnection logic: "Get existing... if !found copy... SetNeighbors".
		// We can optimize: Get existing once, append all new, Set.

		existing, ok := pn.GetNeighbors(target)
		// We already filtered duplicates against *Arrow* chunks. Should match.
		// Construct new list.
		// Careful: `existing` might be reused buffer? No, usually copy.

		var newNeighbors []uint32
		if ok {
			newNeighbors = make([]uint32, len(existing)+len(toAddIdxs))
			copy(newNeighbors, existing)
		} else {
			newNeighbors = make([]uint32, len(toAddIdxs))
		}

		offset := 0
		if ok {
			offset = len(existing)
		}
		for i, idx := range toAddIdxs {
			newNeighbors[offset+i] = sources[idx]
		}

		if h.config.Float16Enabled {
			// Handle distances
			var newF16Dists []float16.Num
			if ok {
				_, existingDists, _ := pn.GetNeighborsF16(target)
				// If existingDists missing but existing present (rare sync issue), pad.
				newF16Dists = make([]float16.Num, len(existing)+len(toAddIdxs))
				copy(newF16Dists, existingDists)
			} else {
				newF16Dists = make([]float16.Num, len(toAddIdxs))
			}

			for i, idx := range toAddIdxs {
				newF16Dists[offset+i] = float16.New(dists[idx])
			}
			pn.SetNeighborsF16(target, newNeighbors, newF16Dists)
		} else {
			pn.SetNeighbors(target, newNeighbors)
		}
	}

	// Prune if needed
	if int(currentCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer)
	}
}

// PruneConnections removes excess connections from a node's neighbor list.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *GraphData, id uint32, maxConn, layer int) {
	lockID := id % ShardedLockCount
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	// COW Promotion
	data = h.promoteNode(data, id)

	h.pruneConnectionsLocked(ctx, data, id, maxConn, layer)
}

// pruneConnectionsLocked reduces connections assuming lock is held.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		// Data snapshot is stale or corrupted, reload and retry
		data = h.data.Load()
		countsChunk = data.GetCountsChunk(layer, cID)
		neighborsChunk = data.GetNeighborsChunk(layer, cID)
		if countsChunk == nil || neighborsChunk == nil {
			return // Cannot prune if chunks are missing
		}
	}

	countAddr := &countsChunk[cOff]
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

	// Logic Switch: SQ8 vs Float32
	if h.quantizer != nil && len(data.VectorsSQ8) > 0 {
		// SQ8 Path
		// Node itself is in VectorsSQ8
		dims := int(h.dims.Load())
		offNode := int(cOff) * dims
		nodeSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
		if nodeSQ8Chunk != nil && offNode+dims <= len(nodeSQ8Chunk) {
			nodeSQ8 := nodeSQ8Chunk[offNode : offNode+dims]

			for i := 0; i < count; i++ {
				neighborID := neighborsChunk[baseIdx+i]

				// Neighbor chunk access
				nCID := chunkID(neighborID)
				nCOff := chunkOffset(neighborID)
				offRem := int(nCOff) * dims

				nVecSQ8Chunk := data.GetVectorsSQ8Chunk(nCID)
				if nVecSQ8Chunk != nil && offRem+dims <= len(nVecSQ8Chunk) {
					d := simd.EuclideanDistanceSQ8(nodeSQ8, nVecSQ8Chunk[offRem:offRem+dims])
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

		// nodeVec needed for distance calc
		// nodeVec needed for distance calc
		// Note: for now we assume float32. Future refactor will make DistFunc polymorphic.
		nodeVecAny := h.mustGetVectorFromData(data, nodeID)
		nodeVec, okNode := nodeVecAny.([]float32)

		for i := 0; i < count; i++ {
			neighborID := neighborsChunk[baseIdx+i]
			vecAny := h.mustGetVectorFromData(data, neighborID)
			vec, okVec := vecAny.([]float32)

			if okVec {
				neighborVecs[i] = vec
			} else {
				neighborVecs[i] = nil
			}

			// Compute distance once for initial Candidate
			if okNode && okVec {
				dists[i] = simd.DistFunc(nodeVec, vec)
			} else {
				dists[i] = math.MaxFloat32 // Push to bottom if invalid type
			}
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
	for i, cand := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], cand.ID)
	}
	// Update count
	atomic.StoreInt32(countAddr, int32(len(selected)))

	// Seqlock write start
	verChunk := data.GetVersionsChunk(layer, cID)
	if verChunk == nil {
		// Should not happen if other chunks exist
		return
	}
	verAddr := &verChunk[cOff]
	atomic.AddUint32(verAddr, 1)

	// Seqlock write end
	atomic.AddUint32(verAddr, 1)

	// --- Packed Neighbors Integration (v0.1.4-rc1) ---
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		ids := make([]uint32, len(selected))
		for i, cand := range selected {
			ids[i] = cand.ID
		}
		if h.config.Float16Enabled {
			f16Dists := make([]float16.Num, len(selected))
			for i, cand := range selected {
				f16Dists[i] = float16.New(cand.Dist)
			}
			pn.SetNeighborsF16(nodeID, ids, f16Dists)
		} else {
			pn.SetNeighbors(nodeID, ids)
		}
	}
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

// InsertWithVectorF16 inserts a Float16 vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVectorF16(id uint32, vec []float16.Num, level int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWInsertDurationSeconds.Observe(time.Since(start).Seconds())
		metrics.HNSWNodesTotal.WithLabelValues("default").Inc()
	}()

	dims := int(h.dims.Load())
	data := h.data.Load()

	// Ensure Capacity & Chunks
	if data.Capacity <= int(id) || (dims > 0 && data.VectorsF16 == nil) {
		h.Grow(int(id)+1, dims)
		data = h.data.Load()
	}

	cID := chunkID(id)
	cOff := chunkOffset(id)
	data = h.ensureChunk(data, cID, cOff, dims)

	// Acquire search context
	metrics.HNSWInsertPoolGetTotal.Inc()
	ctx := h.searchPool.Get().(*ArrowSearchContext)
	ctx.Reset()
	defer func() {
		metrics.HNSWInsertPoolPutTotal.Inc()
		h.searchPool.Put(ctx)
	}()

	// Initialize new node level
	levelsChunk := data.GetLevelsChunk(cID)
	levelsChunk[cOff] = uint8(level)

	// Cache dimensions on first insert
	if dims == 0 && len(vec) > 0 {
		h.initMu.Lock()
		dims = int(h.dims.Load())
		if dims == 0 {
			newDims := len(vec)
			h.Grow(data.Capacity, newDims)
			data = h.data.Load()
			h.dims.Store(int32(newDims))
			dims = newDims
			data = h.ensureChunk(data, cID, cOff, dims)
		} else {
			data = h.data.Load()
		}
		h.initMu.Unlock()
	}

	// Store FP16 Vector (Zero Copy / Memcpy)
	if len(vec) > 0 && dims > 0 {
		f16Chunk := data.GetVectorsF16Chunk(cID)
		if f16Chunk != nil {
			dest := f16Chunk[int(cOff)*dims : (int(cOff)+1)*dims]
			copy(dest, vec)
		}
	}

	// Setup Context Query for Search
	if cap(ctx.queryF16) < len(vec) {
		ctx.queryF16 = make([]float16.Num, len(vec))
	}
	ctx.queryF16 = ctx.queryF16[:len(vec)]
	copy(ctx.queryF16, vec)

	// Logic for BQ/PQ/SQ8 if enabled would go here (omitted for brevity/focus on F16 path)
	// Generally F16 path implies we use F16 for distance.

	// Search and Connect
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	if int(ep) >= data.Capacity {
		data = h.data.Load()
		data = h.ensureChunk(data, cID, cOff, dims)
	}

	// Descent
	for lc := maxL; lc > level; lc-- {
		neighbors := h.searchLayerForInsertF16(ctx, ctx.queryF16, ep, 1, lc, data)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	// Insertion
	ef := h.efConstruction
	if h.config.AdaptiveEf {
		ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
	}

	for lc := level; lc >= 0; lc-- {
		candidates := h.searchLayerForInsertF16(ctx, ctx.queryF16, ep, ef, lc, data)

		m := h.m
		if lc == 0 {
			m = h.m * 2
		}
		maxConn := h.mMax
		if lc > 0 {
			maxConn = h.mMax0
		}
		if m > maxConn {
			m = maxConn
		}

		neighbors := h.selectNeighbors(ctx, candidates, m, data)

		for _, neighbor := range neighbors {
			h.AddConnection(ctx, data, id, neighbor.ID, lc, maxConn, neighbor.Dist)
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn, neighbor.Dist)
			h.pruneIfNecessary(ctx, data, neighbor.ID, lc, maxConn)
		}

		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	if level > maxL {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	}
	h.nodeCount.Add(1)

	return nil
}

// searchLayerForInsertF16 performs search using FP16 query.
func (h *ArrowHNSW) searchLayerForInsertF16(ctx *ArrowSearchContext, query []float16.Num, entryPoint uint32, ef, layer int, data *GraphData) []Candidate {
	maxID := data.Capacity + 1000
	if ctx.visited == nil {
		ctx.visited = NewArrowBitset(maxID)
	} else if ctx.visited.Size() < maxID {
		ctx.visited.Grow(maxID)
	}
	ctx.ResetVisited()

	if ctx.candidates == nil {
		ctx.candidates = NewFixedHeap(ef * 2)
	} else {
		ctx.candidates.Grow(ef * 2)
		ctx.candidates.Clear()
	}
	candidates := ctx.candidates

	if ctx.resultSet == nil {
		ctx.resultSet = NewMaxHeap(ef * 2)
	} else {
		ctx.resultSet.Grow(ef * 2)
		ctx.resultSet.Clear()
	}
	resultSet := ctx.resultSet

	// Entry Distance (F16)
	entryDist := h.distanceF16(query, entryPoint, data, ctx)

	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	resultSet.Push(Candidate{ID: entryPoint, Dist: entryDist})
	ctx.Visit(entryPoint)

	ctx.scratchResults = ctx.scratchResults[:0]
	ctx.scratchResults = append(ctx.scratchResults, Candidate{ID: entryPoint, Dist: entryDist})
	results := ctx.scratchResults // To return at end if we break early? No, we return sorted copy.

	iterations := 0
	maxOps := h.nodeCount.Load() * 2
	if maxOps < 10000 {
		maxOps = 10000
	}

	for candidates.Len() > 0 {
		iterations++
		if int64(iterations) > maxOps {
			break
		}
		curr, ok := candidates.Pop()
		if !ok {
			break
		}

		if best, ok := resultSet.Peek(); ok && resultSet.Len() >= ef && curr.Dist > best.Dist {
			break
		}

		// Neighbors
		ctx.scratchNeighbors = ctx.scratchNeighbors[:0]
		// Need cap check?
		if cap(ctx.scratchNeighbors) < MaxNeighbors {
			ctx.scratchNeighbors = make([]uint32, MaxNeighbors)
		}
		// Assuming GetNeighbors fills it
		rawNeighbors := data.GetNeighbors(layer, curr.ID, ctx.scratchNeighbors)

		ctx.scratchIDs = ctx.scratchIDs[:0]
		for _, nid := range rawNeighbors {
			if !ctx.visited.IsSet(nid) {
				ctx.scratchIDs = append(ctx.scratchIDs, nid)
				ctx.Visit(nid) // Sparse mark
			}
		}
		unvisited := ctx.scratchIDs
		count := len(unvisited)
		if count == 0 {
			continue
		}

		// Calculate Distances (F16 Batch)
		if cap(ctx.scratchDists) < count {
			ctx.scratchDists = make([]float32, count*2)
		}
		dists := ctx.scratchDists[:count]

		// Batched F16 Distance
		// We can loop or use SIMD if available.
		// For now simple loop calling distanceF16 which uses SIMD.
		for i, nid := range unvisited {
			dists[i] = h.distanceF16(query, nid, data, ctx)
		}

		for i, nid := range unvisited {
			dist := dists[i]
			best, _ := resultSet.Peek()
			if resultSet.Len() < ef || dist < best.Dist {
				resultSet.Push(Candidate{ID: nid, Dist: dist})
				if resultSet.Len() > ef {
					resultSet.Pop()
				}
				candidates.Push(Candidate{ID: nid, Dist: dist})
			}
		}
	}

	results = results[:0]
	for resultSet.Len() > 0 {
		cand, _ := resultSet.Pop()
		results = append(results, cand)
	}

	slices.SortFunc(results, func(a, b Candidate) int {
		if a.Dist < b.Dist {
			return -1
		} else if a.Dist > b.Dist {
			return 1
		}
		return 0
	})

	ctx.scratchResults = results
	return results
}
