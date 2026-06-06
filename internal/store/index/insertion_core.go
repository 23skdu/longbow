package index

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Insert adds a new vector to the HNSW graph.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	defer h.commitID(id)
	vec, err := h.GetVectorAny(id)
	if err != nil {
		return err
	}
	return h.InsertWithVector(id, vec, level)
}

// InsertWithVector inserts a vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVector(id uint32, vec any, level int) error {
	first := true
	for {
		current := h.data.Load()
		data, err := h.insertInternal(id, vec, level, !first, current)
		if err != nil {
			return err
		}
		if data == nil || h.compareAndSwapData(current, data) {
			h.commitID(id)

			// Update HNSWNodeCount gauge on successful commit
			if !h.disableNodeCountMetric.Load() {
				shouldUpdateAll := metrics.GlobalHotpathSampler.AlwaysSample
				if shouldUpdateAll || id%100 == 0 {
					metrics.HNSWNodeCount.WithLabelValues(h.name, "0").Set(float64(h.nodeCount.Load()))
				}
			}
			return nil
		}
		first = false
		// CAS failed, retry insertion on the new global state
	}
}

func (h *ArrowHNSW) insertInternal(id uint32, vec any, level int, skipSet bool, existingData *types.GraphData) (*types.GraphData, error) {
	meta := h.metadataRegistry.Load()
	if level < 0 {
		level = h.generateLevel()
	}
	data := existingData
	isPrivate := false
	if data == nil {
		data = h.data.Load()
	} else {
		isPrivate = true // We assume existingData provided by the caller is a private copy they own
	}

	// Helper to ensure we are working on a private clone before any modification
	ensurePrivate := func() {
		if !isPrivate {
			data = h.data.Load()
			data = data.Clone()
			isPrivate = true
		}
	}
	start := time.Now()
	var dims int
	// Snapshot nodeCount once for pressure-adaptive sampling decisions below.
	currentNodeCount := meta.NodeCount
	defer func() {
		duration := time.Since(start).Seconds()
		typeStr := h.config.DataType.String()

		// Use pressure-adaptive sampling: backs off from 1ms → 10ms → 100ms intervals
		// as the dataset grows past 50k and 200k nodes respectively. This prevents
		// Prometheus lock contention from dominating the insertion hot path at scale.
		if !skipSet {
			if ok, multiplier := metrics.GlobalHotpathSampler.ShouldSampleUnderPressure(currentNodeCount); ok {
				metrics.HNSWNodesAddedTotal.WithLabelValues(h.name).Add(multiplier)
				metrics.HNSWInsertOpsTotal.WithLabelValues(h.name, typeStr).Add(multiplier)
				metrics.HNSWIngestionThroughputVectorsPerSecond.WithLabelValues(h.name, typeStr).Add(multiplier)
				metrics.HNSWInsertDurationSeconds.Observe(duration)
				metrics.HNSWInsertLatencyByType.WithLabelValues(typeStr).Observe(duration)
				metrics.HNSWInsertLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)
			}
		}
	}()

	if h.insertPool != nil {
		insertCtx := h.insertPool.Get()
		defer h.insertPool.Put(insertCtx)
		// Pool metrics are low-frequency: gate through pressure sampler to avoid
		// unconditional atomic increments on every single insert.
		if ok, _ := metrics.GlobalHotpathSampler.ShouldSampleUnderPressure(currentNodeCount); ok {
			metrics.HNSWInsertPoolGetTotal.Inc()
			metrics.HNSWInsertPoolPutTotal.Inc()
		}
	}

	if h.config.SQ8Enabled {
		if vecF32, ok := vec.([]float32); ok {
			h.ensureTrained(int(meta.NodeCount), [][]float32{vecF32}, h.data.Load())
		}
	}

	if h.config.PQEnabled {
		if vecF32, ok := vec.([]float32); ok {
			h.ensurePQTrained([][]float32{vecF32})
		}
	}

	dims = int(h.dims.Load())
	if dims == 0 || data == nil || int(id) >= data.Capacity {
		h.growMu.Lock()
		// Only load from global if we don't already have a working copy
		if data == nil {
			data = h.data.Load()
		}
		dims = int(h.dims.Load())
		if dims == 0 {
			inputDims := 0
			switch v := vec.(type) {
			case []float16.Num:
				inputDims = len(v)
			case []float32:
				inputDims = len(v)
			case []float64:
				inputDims = len(v)
			case []int8:
				inputDims = len(v)
			case []uint8:
				inputDims = len(v)
			case []int16:
				inputDims = len(v)
			case []uint16:
				inputDims = len(v)
			case []int32:
				inputDims = len(v)
			case []uint32:
				inputDims = len(v)
			case []int64:
				inputDims = len(v)
			case []uint64:
				inputDims = len(v)
			}
			if inputDims > 0 {
				h.dims.Store(int32(inputDims))
				h.config.Dims = inputDims
				dims = inputDims
			}
		}
		if data == nil || int(id) >= data.Capacity {
			newCap := (int(id) + types.ChunkSize) & ^(types.ChunkSize - 1)
			if err := h.growInternal(newCap, dims); err != nil {
				h.growMu.Unlock()
				return nil, err
			}
			// Adopt the new global state
			data = h.data.Load()
			// If we were supposed to be private, clone it again
			if !skipSet || existingData != nil {
				data = data.Clone()
			}
		}
		h.growMu.Unlock()
	}

	cID := types.ChunkID(id)
	if data.NeedsChunk(cID) {
		// Use ensureChunk which handles the COW and publishing of the grown state
		var err error
		data, err = h.ensureChunk(int(cID), int(types.ChunkOffset(id)), dims)
		if err != nil {
			return nil, err
		}
		// If we were supposed to have a private copy, clone it
		if !skipSet || existingData != nil {
			data = h.data.Load()
			data = data.Clone()
		}
	}

	// h.growMu.RLock() - REMOVED: causes deadlocks with promoteNode and redundant with EnsureChunks
	// defer h.growMu.RUnlock()

	if !skipSet {
		ensurePrivate()

		if !h.sharedVectorSpace.Load() {
			oldVer := data.LockNode(0, id)
			err := data.SetVector(id, vec)
			data.UnlockNode(0, id, oldVer)
			if err != nil {
				return nil, err
			}
		}

		if h.config.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
			sq8Chunk := data.GetVectorsSQ8Chunk(cID)
			if sq8Chunk != nil {
				if vf32, ok := vec.([]float32); ok {
					sq8Stride := (dims + 63) & ^63
					startOff := int(types.ChunkOffset(id)) * sq8Stride
					dest := sq8Chunk[startOff : startOff+dims]
					h.quantizer.Encode(vf32, dest)
				}
			}
		}

		if h.config.BQEnabled && h.bqEncoder != nil {
			bqChunk := data.GetVectorsBQChunk(cID)
			if bqChunk != nil {
				if vf32, ok := vec.([]float32); ok {
					code := h.bqEncoder.Encode(vf32)
					numWords := h.bqEncoder.CodeSize()
					dest := bqChunk[int(types.ChunkOffset(id))*numWords : (int(types.ChunkOffset(id))+1)*numWords]
					copy(dest, code)
				}
			}
		}

		// PERSIST LEVEL: Ensure the node's hierarchical level is stored in metadata
		cID := int(id) / types.ChunkSize
		cOff := int(id) % types.ChunkSize
		levelsChunk := data.GetLevelsChunk(cID)
		if levelsChunk != nil {
			safeLevel := level
			if safeLevel > 255 {
				safeLevel = 255
			}
			atomic.StoreUint32(&levelsChunk[cOff], uint32(safeLevel))
		}
	}

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		if int(meta.NodeCount) >= h.config.AdaptiveMThreshold {
			if h.adaptiveMTriggered.CompareAndSwap(false, true) {
				// Read growth factor from env; default 1.5x to cap adjacency memory growth.
				// The 2x default doubles MMax0 from 24→48 at 10k nodes, which costs
				// 100k * 48 * 4B = ~19GB at 100k vectors. Capping at 1.5x keeps it under 15GB.
				growthFactor := 1.5
				if envFactor := os.Getenv("LONGBOW_ADAPTIVE_M_MAX_FACTOR"); envFactor != "" {
					if f, err := strconv.ParseFloat(envFactor, 64); err == nil && f > 1.0 && f <= 4.0 {
						growthFactor = f
					}
				}

				newM := int64(math.Round(float64(h.config.M) * growthFactor))
				newMMax := int64(math.Round(float64(h.config.MMax) * growthFactor))
				newMMax0 := int64(math.Round(float64(h.config.MMax0) * growthFactor))

				// Hard cap on MMax0 via env: prevents adjacency memory explosion.
				if envMax0 := os.Getenv("LONGBOW_MAX_M0"); envMax0 != "" {
					if cap0, err := strconv.ParseInt(envMax0, 10, 32); err == nil && cap0 > 0 {
						if newMMax0 > cap0 {
							newMMax0 = cap0
						}
					}
				}

				if newM > math.MaxInt32 {
					newM = math.MaxInt32
				}
				if newMMax > math.MaxInt32 {
					newMMax = math.MaxInt32
				}
				if newMMax0 > math.MaxInt32 {
					newMMax0 = math.MaxInt32
				}

				// Emit warning with estimated per-vector memory impact so operators
				// can correlate adaptive-M events with memory pressure reports.
				_ = fmt.Sprintf(
					"[%s] AdaptiveM triggered at nodeCount=%d: M %d→%d, MMax0 %d→%d (factor=%.1fx). "+
						"Estimated L0 adjacency overhead per 100k nodes: %.1f GB",
					h.name, meta.NodeCount,
					h.config.M, newM, h.config.MMax0, newMMax0, growthFactor,
					float64(100000)*float64(newMMax0)*4.0/1e9,
				)
				metrics.HNSWAdaptiveMFiredTotal.WithLabelValues(h.name).Inc()

				h.m.Store(int32(newM))         // #nosec G115
				h.mMax.Store(int32(newMMax))   // #nosec G115
				h.mMax0.Store(int32(newMMax0)) // #nosec G115
				h.config.M, h.config.MMax, h.config.MMax0 = int(newM), int(newMMax), int(newMMax0)
				h.levelMultiplier = 1.0 / math.Log(float64(newM))
			}
		}
	}

	if h.config.PQEnabled && h.oopqEncoder != nil {
		if v32, ok := vec.([]float32); ok {
			switch enc := h.oopqEncoder.(type) {
			case *pq.PQEncoder:
				code, err := enc.Encode(v32)
				if err == nil {
					_ = data.SetVectorPQ(id, code)
				}
			case *pq.OPQEncoder:
				code, err := enc.Encode(v32)
				if err == nil {
					_ = data.SetVectorPQ(id, code)
				}
			}
		}
	}

	ctx := h.searchPool.Get()
	ctx.MaxNodeCount = meta.NodeCount
	ctx.MaxGeneration = meta.Generation
	computer := h.resolveHNSWComputer(data, ctx, vec, true, nil)
	defer h.searchPool.PutWithMetrics(ctx, h.config.DataType.String(), strconv.Itoa(dims))
	ctx.Reset()
	ctx.AllowUncommitted = true

	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Spin-wait for the first node (id=0) to commit its entry point
	// to prevent concurrent inserts from becoming disconnected islands.
	if ep == math.MaxUint32 && id > 0 && h.inBulkInsert.Load() == 0 {
		for ep == math.MaxUint32 {
			// Backoff slightly to allow commitID(0) to proceed
			time.Sleep(1 * time.Millisecond)
			ep = h.entryPoint.Load()
		}
		maxL = int(h.maxLevel.Load())
	}

	// Fast path: when TopLayerManager has accumulated enough entry points,
	// we can read the entry point atomically without acquiring epMu.
	// The pool's GetRandom() provides sufficient diversity to avoid hot-node
	// contention even under heavy parallel insertion.
	if maxL >= 0 && ep != math.MaxUint32 && h.topLayerManager.IsMatured(maxL) {
		// Lock-free entry point selection: use a randomised pool entry if available.
		if randomizedEP, ok := h.topLayerManager.entryPoints[maxL].GetRandom(); ok {
			if int64(randomizedEP) < meta.NodeCount {
				ep = randomizedEP
			}
		}

		for l := maxL; l > level; l-- {
			var neighbors []types.Candidate
			var err error
			if compF32, ok := computer.(*float32ToFloat32Computer); ok {
				neighbors, err = h.searchLayerFloat32(context.Background(), compF32, ep, 1, l, ctx, data)
			} else {
				neighbors, err = h.searchLayer(context.Background(), computer, ep, 1, l, ctx, data, vec)
			}
			if err != nil {
				return nil, err
			}
			if len(neighbors) > 0 {
				ep = neighbors[0].ID
			}
		}
	} else if maxL >= 0 && ep != math.MaxUint32 {
		// Slow path: take epMu for the top-layer traversal when pool is not yet matured.
		h.epMu.Lock()
		ep = h.entryPoint.Load()
		maxL = int(h.maxLevel.Load())

		if randomizedEP, ok := h.topLayerManager.entryPoints[maxL].GetRandom(); ok {
			if int64(randomizedEP) < meta.NodeCount {
				ep = randomizedEP
			}
		}
		h.epMu.Unlock()

		for l := maxL; l > level; l-- {
			var neighbors []types.Candidate
			var err error
			if compF32, ok := computer.(*float32ToFloat32Computer); ok {
				neighbors, err = h.searchLayerFloat32(context.Background(), compF32, ep, 1, l, ctx, data)
			} else {
				neighbors, err = h.searchLayer(context.Background(), computer, ep, 1, l, ctx, data, vec)
			}
			if err != nil {
				return nil, err
			}
			if len(neighbors) > 0 {
				ep = neighbors[0].ID
			}
		}
	}

	shard := id % ShardedLockCount
	lockStart := time.Now()
	h.insertMus[shard].Lock()
	metrics.InsertMuWaitDurationSeconds.WithLabelValues(h.name).Observe(time.Since(lockStart).Seconds())
	defer h.insertMus[shard].Unlock()

	// Cache configuration atomics outside the hot insertion loop
	cachedEfConstruction := int(h.efConstruction.Load())
	cachedM := int(h.m.Load())
	cachedMMax := int(h.mMax.Load())
	cachedMMax0 := int(h.mMax0.Load())
	cachedEf := max(cachedEfConstruction, cachedM)

	for l := min(level, maxL+1); l >= 0 && ep != math.MaxUint32; l-- {
		ef := cachedEf
		neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, l, data)
		if err != nil {
			return nil, err
		}

		var filtered []types.Candidate
		for _, nb := range neighbors {
			if nb.ID != id {
				filtered = append(filtered, nb)
			}
		}
		neighbors = filtered

		maxConn := cachedMMax
		if l == 0 {
			maxConn = cachedMMax0
		}

		if len(neighbors) > 0 {
			ensurePrivate()
			for _, nb := range neighbors {
				h.AddConnection(ctx, data, id, nb.ID, l, maxConn, nb.Dist)
				h.AddConnection(ctx, data, nb.ID, id, l, maxConn, nb.Dist)
			}
			ep = neighbors[0].ID
		}
	}

	// If we modified in-place (no clone), we must update metadata and published state
	// If we modified in-place (no clone), we must update metadata for Entry Point promotion
	if !skipSet && existingData == nil {
		h.updateMetadata(func(meta *HNSWMetadata) {
			if level > int(meta.MaxLevel) {
				meta.MaxLevel = int32(level) // #nosec G115
				meta.EntryPoint = id
			} else if meta.EntryPoint == math.MaxUint32 {
				meta.EntryPoint = id
			}
			meta.Generation++
		})
		// We don't return data because it was updated in-place and already published
		return nil, nil
	}

	return data, nil
}
