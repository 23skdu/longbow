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
	cID := chunkID(id)
	cOff := chunkOffset(id)
	needsStructuralChange := data == nil || int(id) >= data.Capacity || dims == 0 || (dims > 0 && data.Vectors == nil) || cID >= len(data.Vectors) || data.Vectors[cID] == nil

	if needsStructuralChange {
		h.growMu.RUnlock()
		// Re-initialize/Grow without holding RLock to allow write lock acquisition
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
				if h.dims.Load() == 0 {
					if err := h.Grow(h.data.Load().Capacity, inputDims); err != nil {
						h.initMu.Unlock()
						return fmt.Errorf("failed to grow during initial resize: %w", err)
					}
					h.dims.Store(int32(inputDims))
				}
				h.initMu.Unlock()
			}
		}

		// Ensure capacity for the specific ID
		currData := h.data.Load()
		currDims := int(h.dims.Load())
		if currData == nil || int(id) >= currData.Capacity {
			newCap := int(id) + 1
			if currData != nil && currData.Capacity > 0 {
				newCap = max(int(id)+1, currData.Capacity*2)
			}
			if err := h.Grow(newCap, currDims); err != nil {
				return fmt.Errorf("failed to grow for ID %d: %w", id, err)
			}
		}

		// Allocate chunk
		var err error
		_, err = h.ensureChunk(h.data.Load(), cID, cOff, currDims)
		if err != nil {
			return err
		}

		// Re-acquire RLock for the rest of the operation
		h.growMu.RLock()
		data = h.data.Load() // Use latest snapshot
		dims = int(h.dims.Load())
	}
	defer h.growMu.RUnlock()

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

	// Store Vector (Copy for L2 locality)
	if err := data.SetVector(id, vec); err != nil {
		return err
	}

	// If SQ8 is ready, encode and store in scalar chunk as well
	if h.config.SQ8Enabled && h.sq8Ready.Load() {
		if v32, ok := vec.([]float32); ok {
			paddedDims := (dims + 63) & ^63
			encoded := make([]byte, paddedDims)
			h.quantizer.Encode(v32, encoded[:dims])
			// Store in SQ8 storage
			_ = data.SetVector(id, encoded)
		}
	}

	// -------------------------------------------------------------------------
	// Insertion Logic (Layer Search and Linking)
	// -------------------------------------------------------------------------

	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())

	// Phase 1: Search from maxL down to level + 1
	if maxL >= 0 {
		for l := maxL; l > level; l-- {
			neighbors, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, 1, l, data)
			if err != nil {
				return err
			}
			if len(neighbors) > 0 {
				ep = neighbors[0].ID
			}
		}
	}

	// Phase 2: Search and link from level down to 0
	// We start from min(level, max(0, maxL)) to ensure we link even if graph was empty.
	// Actually, if graph was empty, we just skip search and set entry point.
	startL := level
	if maxL >= 0 {
		startL = min(level, maxL)
	} else {
		// First node ever
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
		h.nodeCount.Add(1)
		return nil // No one to link to
	}

	for l := startL; l >= 0; l-- {
		ef := int(h.config.EfConstruction)
		if h.config.AdaptiveEf {
			ef = h.getAdaptiveEf(int(h.nodeCount.Load()))
		}

		candidates, err := h.searchLayerForInsert(context.Background(), ctx, vec, ep, ef, l, data)
		if err != nil {
			return err
		}

		// Select M neighbors for the new node
		m := h.m
		if l == 0 {
			// For layer 0, we can use a higher M if configured, but typically it follows h.m
			// HNSW paper suggests M neighbors for all layers during construction,
			// but M_max0 can be higher.
			m = h.m
		}

		neighbors := h.selectNeighbors(ctx, candidates, m, data)

		// Determine pruning limits for this layer
		maxConn := h.mMax
		if l == 0 {
			maxConn = h.mMax0
		}

		// Create bidirectional connections
		for _, nb := range neighbors {
			// Add connection from new node to neighbor
			h.AddConnection(ctx, data, id, nb.ID, l, maxConn, nb.Dist)
			// Add connection from neighbor back to new node (and prune if neighbor exceeds maxConn)
			h.AddConnection(ctx, data, nb.ID, id, l, maxConn, nb.Dist)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
