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
	h.growMu.Lock()
	defer h.growMu.Unlock()

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

	if level < 0 {
		level = h.generateLevel()
	}

	data := h.data.Load()
	dims = int(h.dims.Load())

	// Phase 0: Initial Dimension Setup & Growth (Strictly Serialized)
	if dims == 0 || data == nil || int(id) >= data.Capacity {
		h.growMu.Lock()
		// Re-check under lock
		data = h.data.Load()
		dims = int(h.dims.Load())
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
				h.dims.Store(int32(inputDims))
				h.config.Dims = inputDims
				dims = inputDims
			}
		}

		if data == nil || int(id) >= data.Capacity {
			newCap := int(id) + 1
			if data != nil && data.Capacity > 0 {
				newCap = max(int(id)+1, data.Capacity*2)
			}
			newCap = (newCap + types.ChunkSize - 1) & ^(types.ChunkSize - 1)
			
			if err := h.growInternal(newCap, dims); err != nil {
				h.growMu.Unlock()
				return fmt.Errorf("failed to grow for ID %d: %w", id, err)
			}
			data = h.data.Load()
		}
		h.growMu.Unlock()
	}

	// Phase 1: Ensure Chunk Allocation (Strictly Serialized)
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	h.growMu.Lock()
	if _, err := h.ensureChunkInternal(cID, cOff, dims); err != nil {
		h.growMu.Unlock()
		return err
	}
	h.growMu.Unlock()
	data = h.data.Load()

	// Ensure nodeCount progress even on failure/panic
	defer h.nodeCount.Add(1)

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 2048
		}
		if count >= threshold {
			if h.adaptiveMTriggered.CompareAndSwap(false, true) {
				h.m = h.config.M * 2
				h.mMax = h.config.MMax * 2
				h.mMax0 = h.config.MMax0 * 2
			}
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
			neighbors, err := h.searchLayer(context.Background(), nil, ep, 1, l, nil, data, vec)
			if err != nil {
				return err
			}
			if len(neighbors) > 0 {
				ep = neighbors[0].ID
			}
		}
	}

	ctx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(ctx, h.config.DataType.String(), strconv.Itoa(dims))
	ctx.Reset()

	// Phase 2: Layer-by-Layer Insertion
	for l := min(level, maxL+1); l >= 0; l-- {
		ef := int(h.efConstruction.Load())
		m := h.m
		if ef < m {
			ef = m
		}

		neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, l, data)
		if err != nil {
			return err
		}

		maxConn := h.mMax
		if l == 0 { maxConn = h.mMax0 }

		for _, nb := range neighbors {
			// AddConnection handles its own COW and internal locking
			data = h.AddConnection(ctx, data, id, nb.ID, l, maxConn, nb.Dist)
			data = h.AddConnection(ctx, data, nb.ID, id, l, maxConn, nb.Dist)
		}

		if len(neighbors) > 0 { ep = neighbors[0].ID }
	}

	maxL = int(h.maxLevel.Load())
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
