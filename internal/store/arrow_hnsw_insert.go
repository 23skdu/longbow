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
				code, err := encoder.Encode(v)
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
		nodeCount := float64(h.nodeCount.Load())
		metrics.HNSWNodesTotal.WithLabelValues("default").Set(nodeCount)
		if h.config.BQEnabled {
			metrics.BQVectorsTotal.WithLabelValues("default").Set(nodeCount)
		}
	}()

	// Make a defensive copy of the vector is avoided by copying into GraphData first.
	// We rely on the copy at line 200 to provide a stable reference.

	// 2. Check Capacity & SQ8/Vectors Status
	// Check dimensions lock-free
	dims := int(h.dims.Load())

	data := h.data.Load()

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
		if h.config.Float16Enabled { 
			f16Chunk := data.GetVectorsF16Chunk(cID) 
			if f16Chunk != nil { 
				dest := f16Chunk[int(cOff)*dims : (int(cOff)+1)*dims] 
				for i, v := range vec { 
					dest[i] = float16.New(v) 
				} 
			} 
		} else { 
			vecChunk := data.GetVectorsChunk(cID) 
			if data.Vectors == nil || int(cID) >= len(data.Vectors) || vecChunk == nil { 
				return fmt.Errorf("vector allocation failure for ID %d (dims=%d): inconsistent state", id, dims) 
			} 
			dest := vecChunk[int(cOff)*dims : (int(cOff)+1)*dims] 
			copy(dest, vec) 
			vec = dest 
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
			dest := chunk[int(cOff)*dims : int(cOff+1)*dims]
			copy(dest, localSQ8Buffer)
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

			dest := chunk[int(cOff)*dims : int(cOff+1)*dims]
			copy(dest, localSQ8Buffer)
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
			h.AddConnection(ctx, data, id, neighbor.ID, lc, maxConn)
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn)

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
	// Reuse visited bitset
	maxID := data.Capacity + 1000
	if ctx.visited == nil {
		ctx.visited = NewArrowBitset(maxID)
	} else {
		ctx.visited.Grow(maxID)
		ctx.visited.ClearSIMD()
	}

	// Reuse candidates heap
	if ctx.candidates == nil {
		ctx.candidates = NewFixedHeap(ef * 2)
	} else {
		ctx.candidates.Grow(ef * 2)
		ctx.candidates.Clear()
	}

	visited := ctx.visited
	candidates := ctx.candidates

	// Reuse result set heap
	if ctx.resultSet == nil {
		ctx.resultSet = NewMaxHeap(ef * 2)
	} else {
		ctx.resultSet.Grow(ef * 2)
		ctx.resultSet.Clear()
	}
	resultSet := ctx.resultSet

	// Initialize with entry point
	var entryDist float32
	var querySQ8 []byte

	// Use sq8Ready to avoid race with backfill
	if h.metric == MetricEuclidean && h.sq8Ready.Load() {
		if cap(ctx.querySQ8) < len(query) {
			ctx.querySQ8 = make([]byte, len(query))
		}
		ctx.querySQ8 = ctx.querySQ8[:len(query)]
		h.quantizer.Encode(query, ctx.querySQ8)
		querySQ8 = ctx.querySQ8

		// Access SQ8 data for entryPoint
		cID := chunkID(entryPoint)
		cOff := chunkOffset(entryPoint)
		off := int(cOff) * int(h.dims.Load())
		dims := int(h.dims.Load())
		vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
		if vecSQ8Chunk != nil && off+dims <= len(vecSQ8Chunk) {
			dSQ8 := simd.EuclideanDistanceSQ8(querySQ8, vecSQ8Chunk[off:off+dims])
			entryDist = float32(dSQ8)
		} else {
			// Fallback if not quantized yet? Should not happen if strictly maintained.
			entryDist = 0 // Or MaxFloat
		}
	} else {
		v := h.mustGetVectorFromData(data, entryPoint)
		if v == nil {
			entryDist = math.MaxFloat32
		} else {
			entryDist = h.distFunc(query, v)
		}
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
		if int64(iterations) > maxOps {
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

				vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
				if vecSQ8Chunk != nil && off+dims <= len(vecSQ8Chunk) {
					vecsSQ8[i] = vecSQ8Chunk[off : off+dims]
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
					vecSQ8Chunk := data.GetVectorsSQ8Chunk(cID)
					if vecSQ8Chunk == nil {
						dists[i] = math.MaxFloat32
						continue
					}
					// If we are here, chunk exists. Check offset.
					cOff := chunkOffset(nid)
					dims := int(h.dims.Load())
					off := int(cOff) * dims
					if off+dims > len(vecSQ8Chunk) {
						dists[i] = math.MaxFloat32
					}
				}
			}
			if cap(ctx.scratchVecs) < count {
				ctx.scratchVecs = make([][]float32, count*2)
			}
			vecs := ctx.scratchVecs[:count]

			allValid := true
			for i, nid := range unvisitedIDs {
				v := h.mustGetVectorFromData(data, nid)
				if v == nil {
					dists[i] = math.MaxFloat32
					vecs[i] = nil
					allValid = false
				} else {
					vecs[i] = v
				}
			}

			if allValid {
				h.batchDistFunc(query, vecs, dists)
			} else {
				for i := 0; i < len(unvisitedIDs); i++ {
					if vecs[i] != nil {
						h.batchDistFunc(query, vecs[i:i+1], dists[i:i+1])
					}
				}
			}
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
	var selected []Candidate     //nolint:prealloc // conditional allocation
	var selectedVecs [][]float32 //nolint:prealloc // conditional allocation
	var remaining []Candidate
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
				// Float32 Path
				if cap(ctx.scratchVecs) < count {
					ctx.scratchVecs = make([][]float32, count*2)
				}
				remVecs := ctx.scratchVecs[:count]
				allValid := true
				for i := range remaining {
					if remainingVecs[i] == nil {
						dists[i] = math.MaxFloat32
						remVecs[i] = nil
						allValid = false
					} else {
						remVecs[i] = remainingVecs[i]
					}
				}

				if allValid {
					if bc, ok := h.batchComputer.(interface {
						ComputeL2DistancesInto(query []float32, vectors [][]float32, dest []float32) (int, error)
					}); ok {
						_, _ = bc.ComputeL2DistancesInto(selVec, remVecs, dists)
					} else {
						h.batchDistFunc(selVec, remVecs, dists)
					}
				} else {
					for i := range remaining {
						if remVecs[i] != nil {
							h.batchDistFunc(selVec, remVecs[i:i+1], dists[i:i+1])
						}
					}
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
func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *GraphData, source, target uint32, layer, maxConn int) {
	// Acquire lock for the specific node (shard)
	lockID := source % ShardedLockCount
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	cID := chunkID(source)
	cOff := chunkOffset(source)

	// Ensure chunk exists (it should, as we resized before insert)
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

	// Prune if needed
	if int(newCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer)
	}
}

// PruneConnections reduces the number of connections to maxConn using the heuristic.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *GraphData, nodeID uint32, maxConn, layer int) {
	// Acquire lock for the specific node
	lockID := nodeID % ShardedLockCount
	h.shardedLocks[lockID].Lock()
	defer h.shardedLocks[lockID].Unlock()

	h.pruneConnectionsLocked(ctx, data, nodeID, maxConn, layer)
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
		nodeVec := h.mustGetVectorFromData(data, nodeID)

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
	verChunk := data.GetVersionsChunk(layer, cID)
	if verChunk == nil {
		// Should not happen if other chunks exist
		return
	}
	verAddr := &verChunk[cOff]
	atomic.AddUint32(verAddr, 1)

	for i := 0; i < newCount; i++ {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], selected[i].ID)
	}
	atomic.StoreInt32(countAddr, int32(newCount))

	// Seqlock write end
	atomic.AddUint32(verAddr, 1)
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
