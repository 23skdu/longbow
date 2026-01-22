package store

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// HNSWCompactionStats tracks the results of a graph compaction.
type HNSWCompactionStats struct {
	OldNodeCount   int
	NewNodeCount   int
	NodesRemoved   int
	BytesReclaimed int64
	Duration       time.Duration
}

// CompactGraph rebuilds the HNSW graph to reclaim space from deleted nodes.
// It remaps all live nodes to a contiguous ID space and consolidates memory.
func (h *ArrowHNSW) CompactGraph(ctx context.Context) (*HNSWCompactionStats, error) {
	start := time.Now()

	// 1. Pause writes and pause background maintenance
	h.growMu.Lock()
	defer h.growMu.Unlock()

	oldSize := int(h.nodeCount.Load())
	if oldSize == 0 {
		return &HNSWCompactionStats{}, nil
	}

	// 2. Identify live nodes and create mapping
	liveIDs := make([]uint32, 0, oldSize)
	oldToNew := make(map[uint32]uint32)
	liveLocations := make([]Location, 0, oldSize)

	for i := 0; i < oldSize; i++ {
		// Context check every 1024 nodes
		if i&1023 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		id := uint32(i)
		if !h.deleted.Contains(int(id)) {
			oldToNew[id] = uint32(len(liveIDs))
			liveIDs = append(liveIDs, id)

			// Extract location for re-population
			loc, ok := h.locationStore.Get(VectorID(id))
			if ok {
				liveLocations = append(liveLocations, loc)
			} else {
				liveLocations = append(liveLocations, Location{BatchIdx: -1, RowIdx: -1})
			}
		}
	}

	newNodeCount := len(liveIDs)
	if newNodeCount == oldSize {
		return &HNSWCompactionStats{
			OldNodeCount: oldSize,
			NewNodeCount: oldSize,
			Duration:     time.Since(start),
		}, nil
	}

	// 3. Allocate new GraphData
	newData := NewGraphData(newNodeCount, int(h.dims.Load()), h.config.SQ8Enabled, h.config.PQEnabled, h.config.PQM, h.config.BQEnabled, h.config.Float16Enabled, h.config.PackedAdjacencyEnabled, h.config.DataType)
	oldData := h.data.Load()

	// 4. Copy Node Data (Vectors, Levels, Neighbors)
	for newIDIdx, oldID := range liveIDs {
		// Context check every 1024 nodes
		if newIDIdx&1023 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		newID := uint32(newIDIdx)

		// Ensure chunk exists in new data
		cID := chunkID(newID)
		cOff := chunkOffset(newID)
		newData = h.ensureChunk(newData, cID, cOff, newData.Dims)

		// Copy Level
		lvl := oldData.GetLevel(oldID)
		if lvl >= 0 {
			newLevelChunk := newData.GetLevelsChunk(cID)
			if newLevelChunk != nil {
				newLevelChunk[cOff] = uint8(lvl)
			}
		}

		// Copy Vector
		h.copyVector(oldData, newData, oldID, newID)

		// Copy Neighbors (remapped)
		for l := 0; l <= lvl && l < ArrowMaxLayers; l++ {
			if h.config.PackedAdjacencyEnabled {
				h.copyPackedNeighbors(oldData, newData, oldID, newID, l, oldToNew)
			} else {
				h.copyNeighbors(oldData, newData, oldID, newID, l, oldToNew)
			}
		}
	}

	// 5. Rebuild LocationStore mapping
	h.locationStore.Reset()
	h.locationStore.BatchAppend(liveLocations)

	// 6. Finalize Metadata
	// Find the new max level and a suitable entry point from the highest layer
	newMaxLevel := -1
	newEntryPoint := uint32(0)
	for newIDIdx := range liveIDs {
		// Context check every 1024 nodes
		if newIDIdx&1023 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		newID := uint32(newIDIdx)
		lvl := newData.GetLevel(newID)
		if lvl > newMaxLevel {
			newMaxLevel = lvl
			newEntryPoint = newID
		}
	}

	if newNodeCount > 0 {
		h.maxLevel.Store(int32(newMaxLevel))
		h.entryPoint.Store(newEntryPoint)
	} else {
		h.maxLevel.Store(-1)
		h.entryPoint.Store(0)
	}

	// 7. Atomic Swap
	h.deleted.Reset() // Clear bits BEFORE swap
	h.data.Store(newData)
	h.backend.Store(newData)
	h.nodeCount.Store(int64(newNodeCount))

	// 8. Statistics
	oldCap := int64(oldData.Capacity)
	newCap := int64(newData.Capacity)
	avgBytesPerNode := int64(h.config.DataType.ElementSize())*int64(h.config.Dims) + int64(ArrowMaxLayers*MaxNeighbors*4)
	bytesReclaimed := (oldCap - newCap) * avgBytesPerNode

	stats := &HNSWCompactionStats{
		OldNodeCount:   oldSize,
		NewNodeCount:   newNodeCount,
		NodesRemoved:   oldSize - newNodeCount,
		BytesReclaimed: bytesReclaimed,
		Duration:       time.Since(start),
	}

	// Update Metrics
	metrics.HNSWNodesTotal.WithLabelValues(h.dataset.Name).Set(float64(newNodeCount))

	return stats, nil
}

// copyVector copies vector from old graph to target graph
func (h *ArrowHNSW) copyVector(old, target *GraphData, oldID, newID uint32) {
	cID_old := chunkID(oldID)
	cOff_old := chunkOffset(oldID)
	cID_new := chunkID(newID)
	cOff_new := chunkOffset(newID)

	// Primary Vectors
	switch target.Type {
	case VectorTypeFloat32:
		copyVectorTyped(old.GetVectorsChunk(cID_old), target.GetVectorsChunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeFloat16:
		copyVectorTyped(old.GetVectorsF16Chunk(cID_old), target.GetVectorsF16Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeFloat64:
		copyVectorTyped(old.GetVectorsFloat64Chunk(cID_old), target.GetVectorsFloat64Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeInt8:
		copyVectorTyped(old.GetVectorsInt8Chunk(cID_old), target.GetVectorsInt8Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeUint8:
		copyVectorTyped(old.GetVectorsUint8Chunk(cID_old), target.GetVectorsUint8Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeInt16:
		copyVectorTyped(old.GetVectorsInt16Chunk(cID_old), target.GetVectorsInt16Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeUint16:
		copyVectorTyped(old.GetVectorsUint16Chunk(cID_old), target.GetVectorsUint16Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeInt32:
		copyVectorTyped(old.GetVectorsInt32Chunk(cID_old), target.GetVectorsInt32Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeUint32:
		copyVectorTyped(old.GetVectorsUint32Chunk(cID_old), target.GetVectorsUint32Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeInt64:
		copyVectorTyped(old.GetVectorsInt64Chunk(cID_old), target.GetVectorsInt64Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeUint64:
		copyVectorTyped(old.GetVectorsUint64Chunk(cID_old), target.GetVectorsUint64Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeComplex64:
		copyVectorTyped(old.GetVectorsComplex64Chunk(cID_old), target.GetVectorsComplex64Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	case VectorTypeComplex128:
		copyVectorTyped(old.GetVectorsComplex128Chunk(cID_old), target.GetVectorsComplex128Chunk(cID_new), cOff_old, cOff_new, target.PaddedDims)
	}

	// Copy Quantized/Encoded vectors if enabled
	if h.config.SQ8Enabled {
		paddedSQ8 := (target.Dims + 63) & ^63
		copyVectorTyped(old.GetVectorsSQ8Chunk(cID_old), target.GetVectorsSQ8Chunk(cID_new), cOff_old, cOff_new, paddedSQ8)
	}
	if h.config.PQEnabled {
		copyVectorTyped(old.GetVectorsPQChunk(cID_old), target.GetVectorsPQChunk(cID_new), cOff_old, cOff_new, target.PQDims)
	}
	if h.config.BQEnabled {
		numWords := (target.Dims + 63) / 64
		copyVectorTyped(old.GetVectorsBQChunk(cID_old), target.GetVectorsBQChunk(cID_new), cOff_old, cOff_new, numWords)
	}
}

func copyVectorTyped[T any](src, dest []T, offSrc, offDest uint32, stride int) {
	if src == nil || dest == nil || stride <= 0 {
		return
	}
	startS := int(offSrc) * stride
	startD := int(offDest) * stride
	if startS+stride <= len(src) && startD+stride <= len(dest) {
		copy(dest[startD:startD+stride], src[startS:startS+stride])
	}
}

// copyNeighbors copies and remaps neighbors from old graph to new graph
func (h *ArrowHNSW) copyNeighbors(old, target *GraphData, oldID, newID uint32, layer int, oldToNew map[uint32]uint32) {
	cID_old := chunkID(oldID)
	cOff_old := chunkOffset(oldID)
	cID_new := chunkID(newID)
	cOff_new := chunkOffset(newID)

	oldCounts := old.GetCountsChunk(layer, cID_old)
	oldNeighbors := old.GetNeighborsChunk(layer, cID_old)

	if oldCounts == nil || oldNeighbors == nil {
		return
	}

	count := int(atomic.LoadInt32(&oldCounts[cOff_old]))
	if count == 0 {
		return
	}

	newCounts := target.GetCountsChunk(layer, cID_new)
	newNeighbors := target.GetNeighborsChunk(layer, cID_new)
	if newCounts == nil || newNeighbors == nil {
		return
	}

	baseOld := int(cOff_old) * MaxNeighbors
	baseNew := int(cOff_new) * MaxNeighbors

	actualCount := 0
	for i := 0; i < count; i++ {
		neighborID := oldNeighbors[baseOld+i]
		if newNeighborID, ok := oldToNew[neighborID]; ok {
			newNeighbors[baseNew+actualCount] = newNeighborID
			actualCount++
		}
	}

	atomic.StoreInt32(&newCounts[cOff_new], int32(actualCount))
}

func (h *ArrowHNSW) copyPackedNeighbors(old, target *GraphData, oldID, newID uint32, layer int, oldToNew map[uint32]uint32) {
	if old.PackedNeighbors[layer] == nil || target.PackedNeighbors[layer] == nil {
		return
	}

	neighbors, ok := old.PackedNeighbors[layer].GetNeighbors(oldID)
	if !ok || len(neighbors) == 0 {
		return
	}

	newNeighbors := make([]uint32, 0, len(neighbors))
	for _, nid := range neighbors {
		if nnid, ok := oldToNew[nid]; ok {
			newNeighbors = append(newNeighbors, nnid)
		}
	}

	if len(newNeighbors) > 0 {
		_ = target.PackedNeighbors[layer].SetNeighbors(newID, newNeighbors)
	}
}
