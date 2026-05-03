package core

import (
	"context"
	"strconv"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
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
	data, err := h.insertInternal(id, vec, level, false, nil)
	if err == nil && data != nil {
		h.compareAndSwapData(data)
	}
	return err
}

// insertInternal is the core HNSW insertion logic.
// It accepts an optional 'data' snapshot to optimize batch operations.
func (h *ArrowHNSW) insertInternal(id uint32, vec any, level int, skipSet bool, data *types.GraphData) (*types.GraphData, error) {
	if level < 0 {
		level = h.generateLevel()
	}
	if data == nil {
		data = h.data.Load()
	}
	start := time.Now()
	var dims int
	defer func() {
		duration := time.Since(start).Seconds()
		nodeCount := float64(h.nodeCount.Load())
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

	if h.insertPool != nil {
		insertCtx := h.insertPool.Get()
		defer h.insertPool.Put(insertCtx)
		metrics.HNSWInsertPoolGetTotal.Inc()
		metrics.HNSWInsertPoolPutTotal.Inc()
	}

	if h.config.SQ8Enabled {
		if vecF32, ok := vec.([]float32); ok {
			h.ensureTrained(int(h.nodeCount.Load()), [][]float32{vecF32})
		}
	}

	dims = int(h.dims.Load())
	if dims == 0 || data == nil || int(id) >= data.Capacity {
		h.growMu.Lock()
		data = h.data.Load()
		dims = int(h.dims.Load())
		if dims == 0 {
			inputDims := 0
			switch v := vec.(type) {
			case []float32: inputDims = len(v)
			case []float64: inputDims = len(v)
			case []int8: inputDims = len(v)
			case []uint8: inputDims = len(v)
			case []int16: inputDims = len(v)
			case []uint16: inputDims = len(v)
			case []int32: inputDims = len(v)
			case []uint32: inputDims = len(v)
			case []int64: inputDims = len(v)
			case []uint64: inputDims = len(v)
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
			data = h.data.Load()
		}
		h.growMu.Unlock()
	}

	cID := types.ChunkID(id)
	if data.NeedsChunk(cID) {
		h.growMu.Lock()
		if _, err := h.ensureChunkInternal(int(cID), int(types.ChunkOffset(id)), dims); err != nil {
			h.growMu.Unlock()
			return nil, err
		}
		h.growMu.Unlock()
		data = h.data.Load()
	}

	if !skipSet {
		oldVer := data.LockNode(0, id)
		err := data.SetVector(id, vec)
		data.UnlockNode(0, id, oldVer)
		if err != nil {
			return nil, err
		}
	}

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		if int(h.nodeCount.Load()) >= h.config.AdaptiveMThreshold {
			if h.adaptiveMTriggered.CompareAndSwap(false, true) {
				h.m = h.config.M * 2
				h.mMax = h.config.MMax * 2
				h.mMax0 = h.config.MMax0 * 2
				h.config.M, h.config.MMax, h.config.MMax0 = h.m, h.mMax, h.mMax0
			}
		}
	}

	if h.config.PQEnabled && h.oopqEncoder != nil {
		if v32, ok := vec.([]float32); ok {
			switch enc := h.oopqEncoder.(type) {
			case *pq.PQEncoder:
				code, err := enc.Encode(v32)
				if err == nil { _ = data.SetVectorPQ(id, code) }
			case *pq.OPQEncoder:
				code, err := enc.Encode(v32)
				if err == nil { _ = data.SetVectorPQ(id, code) }
			}
		}
	}

	ctx := h.searchPool.Get()
	defer h.searchPool.PutWithMetrics(ctx, h.config.DataType.String(), strconv.Itoa(dims))
	ctx.Reset()

	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	if maxL >= 0 {
		for l := maxL; l > level; l-- {
			neighbors, err := h.searchLayer(context.Background(), nil, ep, 1, l, ctx, data, vec)
			if err != nil { return nil, err }
			if len(neighbors) > 0 { ep = neighbors[0].ID }
		}
	}

	for l := min(level, maxL+1); l >= 0; l-- {
		ef := max(int(h.efConstruction.Load()), h.m)
		neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, l, data)
		if err != nil { return nil, err }
		
		var filtered []types.Candidate
		for _, nb := range neighbors { if nb.ID != id { filtered = append(filtered, nb) } }
		neighbors = filtered

		maxConn := h.mMax
		if l == 0 { maxConn = h.mMax0 }
		for _, nb := range neighbors {
			data = h.AddConnection(ctx, data, id, nb.ID, l, maxConn, nb.Dist)
			data = h.AddConnection(ctx, data, nb.ID, id, l, maxConn, nb.Dist)
		}
		if len(neighbors) > 0 { ep = neighbors[0].ID }
	}

	if level > int(h.maxLevel.Load()) {
		h.epMu.Lock()
		if level > int(h.maxLevel.Load()) {
			h.maxLevel.Store(int32(level)) // #nosec G115
			h.entryPoint.Store(id)
		}
		h.epMu.Unlock()
	}

	return data, nil
}
