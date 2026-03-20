package store

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
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
	err = h.InsertWithVector(id, vec, level)
	return err
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
		metrics.HNSWInsertDurationSeconds.Observe(duration)
		nodeCount := float64(h.nodeCount.Load())
		metrics.HNSWNodesAddedTotal.WithLabelValues(h.name).Inc()
		metrics.HNSWNodeCount.WithLabelValues(h.name).Set(nodeCount)

		typeStr := h.config.DataType.String()
		metrics.HNSWInsertOpsTotal.WithLabelValues(h.name, typeStr).Inc()

		metrics.HNSWIngestionThroughputVectorsPerSecond.WithLabelValues(h.name, typeStr).Inc()

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
	} else {
		h.growMu.RUnlock()
		currentData := h.data.Load()
		if currentData != data {
			data = currentData
			dims = int(h.dims.Load())
			needsStructuralChange = data == nil || int(id) >= data.Capacity || dims == 0 || (dims > 0 && data.Vectors == nil) || cID >= len(data.Vectors) || data.Vectors[cID] == nil
			if needsStructuralChange {
				h.growMu.Lock()
				defer h.growMu.Unlock()
				goto do_grow
			}
		}
	}

do_grow:
	if needsStructuralChange {
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
					data := h.data.Load()
					capacity := 1024
					if data != nil {
						capacity = data.Capacity
					}
					if err := h.Grow(capacity, inputDims); err != nil {
						h.initMu.Unlock()
						return fmt.Errorf("failed to grow during initial resize: %w", err)
					}
					h.dims.Store(int32(inputDims))
				}
				h.initMu.Unlock()
			}
		}

		currData := h.data.Load()
		currDims := int(h.dims.Load())
		if currData == nil || int(id) >= currData.Capacity {
			newCap := int(id) + 1
			if currData != nil && currData.Capacity > 0 {
				newCap = max(int(id)+1, currData.Capacity*2)
				// Align capacity to optimal chunk sizes to prevent fragmentation
				newCap = (newCap + ChunkSize - 1) & ^(ChunkSize - 1)
			}
			if err := h.Grow(newCap, currDims); err != nil {
				return fmt.Errorf("failed to grow for ID %d: %w", id, err)
			}
		}

		var err error
		_, err = h.ensureChunk(h.data.Load(), cID, cOff, currDims)
		if err != nil {
			return err
		}

		data = h.data.Load()
		dims = int(h.dims.Load())
	}

	currentData := h.data.Load()
	if currentData != data {
		data = currentData
		dims = int(h.dims.Load())
	}

	// From here on, we use 'data' snapshot. Internal operations (AddConnection)
	// will handle their own locking and data reloads if needed.

	if h.config.AdaptiveMEnabled && !h.adaptiveMTriggered.Load() {
		count := int(h.nodeCount.Load())
		threshold := h.config.AdaptiveMThreshold
		if threshold <= 0 {
			threshold = 2000
			if data != nil && data.Capacity >= 50000 {
				threshold = 10000
			} else if data != nil && data.Capacity >= 10000 {
				threshold = 5000
			}
		}

		if count == threshold {
			h.adjustMParameter(data, threshold)
		}
	}

	// Dynamic HNSW Dimension Index Optimization for Float32/Float64 at Scale
	// If index wasn't initialized with high InitialCapacity but grew past 10k nodes,
	// apply the same M/MMax/MMax0 adjustments as the init-time optimization.
	// This prevents Float32 QPS collapse from suboptimal graph connectivity.
	currentCount := int(h.nodeCount.Load())
	currentM := h.m
	currentDims := int(h.dims.Load())

	if currentCount >= 10000 && currentDims >= 384 &&
		(h.config.DataType == types.VectorTypeFloat32 || h.config.DataType == types.VectorTypeFloat64) &&
		!h.adaptiveMTriggered.Load() && currentM < 24 {
		newM := 24
		newMMax := 48
		if currentM < newM {
			h.m = newM
			h.mMax = newMMax
			h.mMax0 = newMMax * 2
			h.levelMultiplier = 1.0 / math.Log(float64(newM))
			h.adaptiveMTriggered.Store(true)
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

	// If BQ is enabled, encode and store
	if h.config.BQEnabled {
		if v32, ok := vec.([]float32); ok {
			bqVec := encodeBQ(v32)
			if err := data.SetVectorBQ(id, bqVec); err != nil {
				return fmt.Errorf("failed to set BQ vector: %w", err)
			}
		}
	}

	// If PQ is enabled and ready, encode and store
	if h.config.PQEnabled && h.pqEncoder != nil {
		if v32, ok := vec.([]float32); ok {
			code, err := h.pqEncoder.Encode(v32)
			if err == nil {
				if err := data.SetVectorPQ(id, code); err != nil {
					return fmt.Errorf("failed to set PQ vector: %w", err)
				}
			}
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
	// Phase 2: Search and link from level down to 0
	// We start from min(level, max(0, maxL)) to ensure we link even if graph was empty.
	// Actually, if graph was empty, we just skip search and set entry point.
	if maxL < 0 {
		// First node ever
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)

		// Update nodeCount if it hasn't been incremented by caller
		for {
			current := h.nodeCount.Load()
			if int64(id) < current {
				break
			}
			if h.nodeCount.CompareAndSwap(current, int64(id+1)) {
				break
			}
		}

		return nil // No one to link to
	}
	startL := min(level, maxL)

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

	// Removed: h.nodeCount.Add(1) - incremented in caller

	// Update nodeCount if we're adding a new node at the end
	for {
		current := h.nodeCount.Load()
		if int64(id) < current {
			break
		}
		if h.nodeCount.CompareAndSwap(current, int64(id+1)) {
			break
		}
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
