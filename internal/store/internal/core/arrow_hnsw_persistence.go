package core

import (
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
)

// SnapshotHNSW exports the current graph state for persistence.
func (h *ArrowHNSW) SnapshotHNSW() error {
	return nil
}

// promoteNode ensures that a node's neighbor list is present in the mutable types.GraphData.
// If the node is currently only in DiskGraph, it copies the neighbors to types.GraphData.
// This implements Copy-On-Write for the graph structure.
// Returns the chunk and offset in types.GraphData.
func (h *ArrowHNSW) promoteNode(data *types.GraphData, id uint32) *types.GraphData {
	if int(id) >= data.Capacity {
		return data
	}

	cID := types.ChunkID(id)
	
	// Optimized check: verify chunk exists in memory before promoting
	if data.GetNeighborsChunk(0, cID) != nil {
		return data
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	// Re-load data pointer under lock to avoid races
	data = h.data.Load()
	return h.promoteNodeLocked(data, id)
}

// promoteNodeLocked is like promoteNode but assumes growMu.Lock() is already held.
func (h *ArrowHNSW) promoteNodeLocked(data *types.GraphData, id uint32) *types.GraphData {
	if int(id) >= data.Capacity {
		return data
	}

	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	// If already in memory, no need to promote from disk
	if data.GetNeighborsChunk(0, cID) != nil {
		return data
	}

	dg := h.diskGraph.Load()
	if dg == nil {
		return data
	}

	// Ensure chunk in Mutable Data (L0-LMax)
	dims := int(h.dims.Load())
	var err error
	data, err = h.ensureChunkInternal(cID, cOff, dims)
	if err != nil {
		return data
	}

	// Copy neighbors from disk for all layers
	for l := 0; l < types.ArrowMaxLayers; l++ {
		diskNeighbors := dg.GetNeighbors(l, id, nil)
		if len(diskNeighbors) == 0 {
			continue
		}

		countsChunk := data.GetCountsChunk(l, cID)
		neighborsChunk := data.GetNeighborsChunk(l, cID)
		if countsChunk == nil || neighborsChunk == nil {
			continue
		}

		// Copy neighbors to chunk
		limit := h.mMax
		if l == 0 {
			limit = h.mMax0
		}
		
		start := int(cOff) * limit
		for i, nID := range diskNeighbors {
			if i < limit {
				neighborsChunk[start+i] = nID
			}
		}
		atomic.StoreInt32(&countsChunk[cOff], int32(min(len(diskNeighbors), limit))) // #nosec G115
	}

	return data
}
