package core

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its types.VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	defer h.commitID(id)
	// Zero-Copy Ingestion Path
	// Get vector for distance calculations (and caching)
	// We use generic GetVectorAny to support all types.
	vec, err := h.GetVectorAny(id)
	if err != nil {
		return err
	}

	return h.InsertWithVector(id, vec, level)
}

// InsertWithVector inserts a vector that has already been retrieved.
func (h *ArrowHNSW) InsertWithVector(id uint32, vec any, level int) error {
	if level < 0 {
		level = h.generateLevel()
	}
	start := time.Now()
	var dims int
	defer func() {
		duration := time.Since(start).Seconds()
		nodeCount := float64(h.nodeCount.Load())
		// Metrics Sampling: Reduce atomic overhead in hot paths
		typeStr := h.config.DataType.String()
		if int(id)%100 == 0 {
			metrics.HNSWNodesAddedTotal.WithLabelValues(h.name).Add(100)
			metrics.HNSWInsertOpsTotal.WithLabelValues(h.name, typeStr).Add(100)
			metrics.HNSWIngestionThroughputVectorsPerSecond.WithLabelValues(h.name, typeStr).Add(100)
		}

		if int(id)%10 == 0 {
			metrics.HNSWInsertDurationSeconds.Observe(duration)
			metrics.HNSWInsertLatencyByType.WithLabelValues(typeStr).Observe(duration)
			metrics.HNSWInsertLatencyByDim.WithLabelValues(strconv.Itoa(dims)).Observe(duration)
		}

		if !h.disableNodeCountMetric.Load() && int(id)%100 == 0 {
			metrics.HNSWNodeCount.WithLabelValues(h.name, "0").Set(nodeCount)
		}
	}()

	// 1. SQ8 Training (Outside any lock to avoid deadlock with ensureTrained -> growMu.Lock())
	if h.config.SQ8Enabled {
		if vecF32, ok := vec.([]float32); ok {
			h.ensureTrained(int(h.nodeCount.Load()), [][]float32{vecF32})
		}
	}

	// Acquire Read Lock to protect initial data/dims access and prevent structural races
	h.growMu.RLock()
	data := h.data.Load()
	dims = int(h.dims.Load())

	// Fix for race where we see new dims but old data pointer
	if data != nil && dims > 0 && data.Dims != dims {
		data = h.data.Load()
	}

	// Double-check if we need growth or and lazy initialization under lock
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	// Use Arena-aware check for structural changes
	needsStructuralChange := data == nil || int(id) >= data.Capacity || dims == 0 || data.NeedsChunk(int(cID))

	if needsStructuralChange {
		h.growMu.RUnlock()
		if dims == 0 {
			inputDims := 0
			switch v := vec.(type) {
			case []float32: inputDims = len(v)
			case []float16.Num: inputDims = len(v)
			case []complex64: inputDims = len(v)
			case []complex128: inputDims = len(v)
			case []float64: inputDims = len(v)
			case []int8: inputDims = len(v)
			case []uint8: inputDims = len(v)
			}
			if inputDims > 0 {
				h.initMu.Lock()
				if h.dims.Load() == 0 {
					capacity := 1024
					data := h.data.Load()
					if data != nil { capacity = data.Capacity }
					if err := h.Grow(capacity, inputDims); err != nil {
						h.initMu.Unlock()
						return fmt.Errorf("failed to grow during initial resize: %w", err)
					}
					h.dims.Store(int32(inputDims))
					h.config.Dims = inputDims
				}
				h.initMu.Unlock()
			}
		}

		// Strictly serialized growth/chunk allocation
		h.growMu.Lock()
		currData := h.data.Load()
		currDims := int(h.dims.Load())

		if currData == nil || int(id) >= currData.Capacity {
			newCap := int(id) + 1
			if currData != nil && currData.Capacity > 0 {
				newCap = max(int(id)+1, currData.Capacity*2)
				newCap = (newCap + types.ChunkSize - 1) & ^(types.ChunkSize - 1)
			}
			if err := h.growInternal(newCap, currDims); err != nil {
				h.growMu.Unlock()
				return fmt.Errorf("failed to grow for ID %d: %w", id, err)
			}
		}

		var err error
		_, err = h.ensureChunkInternal(cID, cOff, currDims)
		if err != nil {
			h.growMu.Unlock()
			return err
		}
		h.growMu.Unlock()

		// Reload for the next phase
		data = h.data.Load()
		dims = int(h.dims.Load())
	} else {
		// Already have RLock from line 66, and no growth needed.
		h.growMu.RUnlock()
	}

	// Ensure nodeCount progress even on failure/panic
	defer h.commitID(id)

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 2048
		}
		if count >= threshold {
			h.adjustMParameter(data, threshold)
		}
	}

	metrics.HNSWInsertPoolGetTotal.Inc()
	ctx := h.searchPool.Get()
	ctx.Reset()
	ctx.diskGraph = h.diskGraph.Load()
	defer func() {
		metrics.HNSWInsertPoolPutTotal.Inc()
		h.searchPool.PutWithMetrics(ctx, h.config.DataType.String(), strconv.Itoa(int(h.dims.Load())))
	}()

	levelsChunk := data.GetLevelsChunk(cID)
	if levelsChunk != nil {
		levelsChunk[cOff] = uint8(level) // #nosec G115
	}

	// Store Vector
	if err := data.SetVector(id, vec); err != nil {
		return err
	}

	// Quantization paths
	if h.config.SQ8Enabled && h.sq8Ready.Load() {
		if v32, ok := vec.([]float32); ok {
			paddedDims := (dims + 63) & ^63
			encoded := make([]byte, paddedDims)
			h.quantizer.Encode(v32, encoded[:dims])
			_ = data.SetVector(id, encoded)
		}
	}

	if h.config.BQEnabled {
		if v32, ok := vec.([]float32); ok {
			bqVec := encodeBQ(v32)
			_ = data.SetVectorBQ(id, bqVec)
		}
	}

	if h.config.PQEnabled && h.pqEncoder != nil {
		if v32, ok := vec.([]float32); ok {
			code, err := h.pqEncoder.Encode(v32)
			if err == nil { _ = data.SetVectorPQ(id, code) }
		}
	}

	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Phase 1: Descend layers
	if maxL >= 0 {
		for l := maxL; l > level; l-- {
			// Reload data to see latest view during descent
			data = h.data.Load()
			neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, 1, l, data)
			if err != nil { return err }
			if len(neighbors) > 0 { ep = neighbors[0].ID }
			h.putCandidateSlice(neighbors)
		}
	}

	// Phase 2: Link layers
	if maxL < 0 {
		// First node
		h.maxLevel.Store(int32(level)) // #nosec G115
		h.entryPoint.Store(id)
		return nil
	}

	startL := min(level, maxL)
	for l := startL; l >= 0; l-- {
		data = h.data.Load()
		ef := int(h.efConstruction.Load())
		if h.config.AdaptiveEf {
			ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
		}

		candidates, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, l, data)
		if err != nil { return err }

		neighbors := h.selectNeighbors(ctx, candidates, h.m, data)
		maxConn := h.mMax
		if l == 0 { maxConn = h.mMax0 }

		for _, nb := range neighbors {
			// AddConnection handles its own COW and internal locking
			data = h.AddConnection(ctx, data, id, nb.ID, l, maxConn, nb.Dist)
			data = h.AddConnection(ctx, data, nb.ID, id, l, maxConn, nb.Dist)
		}

		if len(neighbors) > 0 { ep = neighbors[0].ID }
		h.putCandidateSlice(candidates)
	}

	if level > maxL {
		h.maxLevel.Store(int32(level)) // #nosec G115
		h.entryPoint.Store(id)
	}

	return nil
}

func encodeBQ(vec []float32) []uint64 {
	length := len(vec)
	padded := (length + 63) & ^63
	numWords := padded / 64
	encoded := make([]uint64, numWords)

	for i := 0; i < length; i++ {
		if vec[i] > 0 {
			wordIdx := i / 64
			bitIdx := i % 64
			encoded[wordIdx] |= (1 << bitIdx)
		}
	}
	return encoded
}
