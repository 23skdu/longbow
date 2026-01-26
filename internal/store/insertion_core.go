package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/metrics"
)

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	// Zero-Copy Ingestion Path
	// Get vector for distance calculations (and caching)
	// We use generic getVectorAny to support all types.
	vec, err := h.getVectorAny(id)
	if err != nil {
		return err
	}
	return h.InsertWithVector(id, vec, level)
}

// InsertWithVector inserts a vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVector(id uint32, vec any, level int) error {
	start := time.Now()
	var dims int
	defer func() {
		duration := time.Since(start).Seconds()
		h.metricInsertDuration.Observe(duration)
		nodeCount := float64(h.nodeCount.Load())
		h.metricNodeCount.Set(nodeCount)
		if h.config.BQEnabled {
			h.metricBQVectors.Set(nodeCount)
		}

		dsName := "unknown"
		if h.dataset != nil {
			dsName = h.dataset.Name
		}
		typeStr := h.config.DataType.String()
		metrics.HNSWIngestionThroughputVectorsPerSecond.WithLabelValues(dsName, typeStr).Inc()

		metrics.HNSWInsertLatencyByType.WithLabelValues(typeStr).Observe(duration)
		metrics.HNSWInsertLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)
	}()

	// Make a defensive copy of the vector is avoided by copying into GraphData first.
	// We rely on the copy at line 200 to provide a stable reference.
	dims = int(h.dims.Load())

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

	// Lazy Dimension Initialization
	// Must happen BEFORE acquiring RLock to avoid Deadlock during Grow()
	if dims == 0 {
		inputDims := 0
		switch v := vec.(type) {
		case []float32:
			inputDims = len(v)
		case []float16.Num:
			inputDims = len(v)
		case []complex64:
			inputDims = len(v)
		case []complex128:
			inputDims = len(v)
		case []float64:
			inputDims = len(v)
		case []int8:
			inputDims = len(v)
		case []uint8:
			inputDims = len(v)
		}

		if inputDims > 0 {
			h.initMu.Lock()
			// Double-check under lock
			dims = int(h.dims.Load())
			if dims == 0 {
				newDims := inputDims
				// This acquires growMu.Lock(), which is safe here (we don't hold RLock yet).
				h.Grow(data.Capacity, newDims)

				h.dims.Store(int32(newDims))
				dims = newDims

				data = h.data.Load()
			}
			h.initMu.Unlock()
		}
	}

	// Invariant: If dims > 0, Vectors/SQ8 arrays MUST exist in data.
	if data.Capacity <= int(id) || (dims > 0 && data.Vectors == nil) {
		h.Grow(int(id)+1, dims)
		data = h.data.Load()
	}

	// SQ8 Training: Must be done BEFORE acquiring RLock to avoid recursive Lock/RLock deadlock
	if h.config.SQ8Enabled && dims > 0 {
		if vecF32, ok := vec.([]float32); ok {
			// Use nodeCount as an approximation of total vectors seen including this one
			h.ensureTrained(int(h.nodeCount.Load())+1, [][]float32{vecF32})
		}
	}

	// Acquire Read Lock to prevent concurrent Grow() from swapping GraphData
	// while we are allocating/initializing chunks in the current snapshot.
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	// Lazy Chunk Allocation (Protected by RLock)
	cID := chunkID(id)
	cOff := chunkOffset(id)
	var err error
	data, err = h.ensureChunk(data, cID, cOff, dims)
	if err != nil {
		return err
	}

	metrics.HNSWInsertPoolGetTotal.Inc()
	ctx := h.searchPool.Get()
	ctx.Reset()
	defer func() {
		metrics.HNSWInsertPoolPutTotal.Inc()
		h.searchPool.Put(ctx)
	}()

	levelsChunk := data.GetLevelsChunk(cID)
	// We ensured chunk exists, so levelsChunk should not be nil.
	if levelsChunk != nil {
		levelsChunk[cOff] = uint8(level)
	}

	// Recovery block removed: Invariant guarantees Vectors exist if dims > 0.

	// Store Vector (Copy for L2 locality)
	// Skip if DiskStore is enabled (already written via BatchAppend or will be written)
	// Note: We check data.DiskStore which is populated if offloading is active.
	// FIX: Check if DiskGraph is loaded via atomic pointer, not data.DiskStore if it's nil
	if dims > 0 && h.diskGraph.Load() == nil {
		if err := data.SetVector(id, vec); err != nil {
			return err
		}
	}

	if h.config.BQEnabled && dims > 0 {
		if vecF32, ok := vec.([]float32); ok {
			// Initialize Encoder if needed (lazy)
			if h.bqEncoder == nil {
				h.initMu.Lock()
				if h.bqEncoder == nil {
					h.bqEncoder = NewBQEncoder(dims)
				}
				h.initMu.Unlock()
			}

			encoded := h.bqEncoder.Encode(vecF32)
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
	}

	if h.config.PQEnabled && h.pqEncoder != nil && dims > 0 {
		if vecF32, ok := vec.([]float32); ok {
			code, err := h.pqEncoder.Encode(vecF32)
			if err == nil {
				encodedChunk := data.GetVectorsPQChunk(cID)
				if encodedChunk != nil {
					pqM := data.PQDims()
					if pqM > 0 {
						baseIdx := int(cOff) * pqM
						copy(encodedChunk[baseIdx:baseIdx+pqM], code)
					}
				}
			}
		}
	}

	if h.nodeCount.Load() == 0 {
		lockStart := time.Now()
		h.initMu.Lock()
		h.metricLockWait.WithLabelValues("init").Observe(time.Since(lockStart).Seconds())
		if h.nodeCount.Load() == 0 {
			h.entryPoint.Store(id)
			h.maxLevel.Store(int32(level))
			h.nodeCount.Store(1)
			h.initMu.Unlock()
			return nil
		}
		h.initMu.Unlock()
	}

	// Strategy: Use local buffer for encoding, then copy to shared memory
	// This avoids race conditions from concurrent writes to the same chunk
	var localSQ8Buffer []byte
	sq8Handled := false

	if h.sq8Ready.Load() {
		if vecF32, ok := vec.([]float32); ok {
			if cap(ctx.querySQ8) < dims {
				ctx.querySQ8 = make([]byte, dims)
			}
			ctx.querySQ8 = ctx.querySQ8[:dims]
			h.quantizer.Encode(vecF32, ctx.querySQ8)
			localSQ8Buffer = ctx.querySQ8

			cID := chunkID(id)
			cOff := chunkOffset(id)
			if chunk := data.GetVectorsSQ8Chunk(cID); chunk != nil {
				paddedDims := (dims + 63) & ^63
				offset := int(cOff) * paddedDims
				if offset+dims <= len(chunk) {
					dest := chunk[offset : offset+dims]
					copy(dest, localSQ8Buffer)
				}
			}
			sq8Handled = true
		}
	}

	if !sq8Handled && h.sq8Ready.Load() {
		cID := chunkID(id)
		cOff := chunkOffset(id)

		if chunk := data.GetVectorsSQ8Chunk(cID); chunk != nil {
			if vecF32, ok := vec.([]float32); ok {
				if cap(ctx.querySQ8) < dims {
					ctx.querySQ8 = make([]byte, dims)
				}
				ctx.querySQ8 = ctx.querySQ8[:dims]
				h.quantizer.Encode(vecF32, ctx.querySQ8)
				localSQ8Buffer = ctx.querySQ8

				paddedDims := (dims + 63) & ^63
				offset := int(cOff) * paddedDims
				if offset+dims <= len(chunk) {
					dest := chunk[offset : offset+dims]
					copy(dest, localSQ8Buffer)
				}
			}
		}
	}

	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Fix for Growth Race:
	// If the entry point 'ep' was inserted by another thread that forced a Grow(),
	// our local 'data' snapshot might be too small (stale) to contain 'ep'.
	// We must reload 'data' to avoid out-of-bounds access during search.
	if int(ep) >= data.Capacity {
		data = h.data.Load()

		if int(ep) >= data.Capacity {
			return fmt.Errorf("entry point %d beyond capacity %d even after reload", ep, data.Capacity)
		}

		// Re-ensure OUR chunk exists in this new data view for safety
		// (though likely it does if Grow copied it, or we allocate it again)
		// We need to ensure we can write links to our node later.
		var err error
		data, err = h.ensureChunk(data, cID, cOff, dims)
		if err != nil {
			return err
		}
	}

	for lc := maxL; lc > level; lc-- {
		neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, 1, lc, data)
		if err != nil {
			return err
		}
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}

	ef := int(h.efConstruction.Load())
	if h.config.AdaptiveEf {
		nodeCount := int(h.nodeCount.Load())
		ef = h.getAdaptiveEf(nodeCount)
	}

	for lc := level; lc >= 0; lc-- {
		candidates, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, lc, data)
		if err != nil {
			return err
		}

		m := h.m
		if lc == 0 {
			m = h.m * 2 // Layer 0 usually denser
		}
		maxCap := h.mMax
		if lc > 0 {
			maxCap = h.mMax0
		}
		if m > maxCap {
			m = maxCap
		}

		neighbors := h.selectNeighbors(ctx, candidates, m, data)

		maxConn := h.mMax
		if lc > 0 {
			maxConn = h.mMax0
		}

		// Optimized target (Forward) connections additions using batch to avoid redundant pruning of 'id'
		neighborIDs := make([]uint32, len(neighbors))
		neighborDists := make([]float32, len(neighbors))
		for i, n := range neighbors {
			neighborIDs[i] = n.ID
			neighborDists[i] = n.Dist
		}
		h.AddConnectionsBatch(ctx, data, id, neighborIDs, neighborDists, lc, maxConn)

		// Reverse connections still one-by-one for now to respect sharded locks easily
		for _, neighbor := range neighbors {
			h.AddConnection(ctx, data, neighbor.ID, id, lc, maxConn, neighbor.Dist)
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
