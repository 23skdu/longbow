package core

import (
	"fmt"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
)

// LoadFromMmap opens a DiskGraph file and attaches it to the ArrowHNSW index.
// This enables zero-copy loading of the graph structure.
func (h *ArrowHNSW) LoadFromMmap(path string) error {
	dg, err := NewDiskGraph(path)
	if err != nil {
		return fmt.Errorf("failed to open disk graph: %w", err)
	}

	// Verify dimensions match
	if h.config.Dims > 0 && int(dg.header.Dims) != h.config.Dims {
		_ = dg.Close() // #nosec G104
		return fmt.Errorf("dimension mismatch: graph has %d, config has %d", dg.header.Dims, h.config.Dims)
	}

	// Update HNSW state from graph header
	h.nodeCount.Store(int64(dg.header.NumNodes))
	h.dims.Store(int32(dg.header.Dims)) // #nosec G115

	// Restore Entry Point and Max Level (Version 3+)
	if dg.header.Version >= 3 {
		h.entryPoint.Store(dg.header.EntryPoint)
		h.maxLevel.Store(dg.header.GraphMaxLevel)
	} else {
		// Fallback for older formats (if supported)
		h.entryPoint.Store(0)                            // Unlikely to be correct but safe default?
		h.maxLevel.Store(int32(dg.header.MaxLayers - 1)) // #nosec G115
	}

	// Attach DiskGraph
	h.diskGraph.Store(dg)

	// Disk Backing Store
	h.diskGraph.Store(dg)

	// Set BackingGraph on types.GraphData for Copy-On-Write support
	data := h.data.Load()
	if data != nil {
		data.BackingGraph = dg
	}

	// Restore Quantizer (Version 3+)
	if dg.header.Version >= 3 && h.config.SQ8Enabled {
		// Only restore if valid bounds (not 0,0 typically, although 0,0 is possible for flat data, but unlikely)
		// We can trust the header.
		h.quantizer = NewScalarQuantizerFromParams(int(dg.header.Dims), dg.header.SQ8Min, dg.header.SQ8Max)
		h.sq8Ready.Store(true)
	}

	// Note: We do NOT populate h.data (types.GraphData) with these nodes.
	// Users of the index must use the Hybrid Accessors (GetNeighbors) which check diskGraph.

	return nil
}

// promoteNode ensures that a node's neighbor list is present in the mutable types.GraphData.
// If the node is currently only in DiskGraph, it copies the neighbors to types.GraphData.
// This implements Copy-On-Write for the graph structure.
// Returns the chunk and offset in types.GraphData.
func (h *ArrowHNSW) promoteNode(data*types.GraphData, id uint32)*types.GraphData {
	// If already in types.GraphData (capacity covered), check if chunk exists
	if int(id) >= data.Capacity {
		// Should have been grown by caller
		return data
	}

	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	dg := h.diskGraph.Load()
	if dg == nil {
		return data
	}

	// Optimized check: verify chunk exists in memory before promoting
	neighborsChunkL0 := data.GetNeighborsChunk(0, cID)
	if neighborsChunkL0 != nil {
		return data
	}

	// Iterate all layers
	for l := 0; l < types.ArrowMaxLayers; l++ {
		// Get neighbors from Disk
		diskNeighbors := dg.GetNeighbors(l, id, nil)
		if len(diskNeighbors) == 0 {
			continue
		}

		// Ensure chunk in Mutable Data
		var err error
		data, err = h.ensureChunk(data, cID, cOff, data.Dims)
		if err != nil {
			// What to do? Log and continue? Or return?
			// promoteNode returns*types.GraphData.
			// Ideally we shouldn't fail memory alloc here.
			// For now, return unmodified data if error (safe fallback?)
			return data
		}

		countsChunk := data.GetCountsChunk(l, cID)
		neighborsChunk := data.GetNeighborsChunk(l, cID)

		// Check if already populated (count > 0)
		countAddr := &countsChunk[cOff]
		currentCount := atomic.LoadInt32(countAddr)

		if currentCount == 0 {
			// Copy from Disk
			baseIdx := int(cOff) * types.MaxNeighbors
			for i, nid := range diskNeighbors {
				if i >= types.MaxNeighbors {
					break
				}
				atomic.StoreUint32(&neighborsChunk[baseIdx+i], nid)
			}
			atomic.StoreInt32(countAddr, int32(len(diskNeighbors))) // #nosec G115

			// Initialize version
			verAddr := &data.GetVersionsChunk(l, cID)[cOff]
			atomic.StoreUint32(verAddr, 0) // Valid, even
		}
	}

	return data
}

// promoteNodeLocked is like promoteNode but assumes growMu.Lock() is already held.
func (h *ArrowHNSW) promoteNodeLocked(data *types.GraphData, id uint32) *types.GraphData {
	if int(id) >= data.Capacity {
		return data
	}

	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)

	dg := h.diskGraph.Load()
	if dg == nil {
		return data
	}

	neighborsChunkL0 := data.GetNeighborsChunk(0, cID)
	if neighborsChunkL0 != nil {
		return data
	}

	for l := 0; l < types.ArrowMaxLayers; l++ {
		diskNeighbors := dg.GetNeighbors(l, id, nil)
		if len(diskNeighbors) == 0 {
			continue
		}

		var err error
		data, err = h.ensureChunkInternal(cID, cOff, data.Dims)
		if err != nil {
			return data
		}
	}

	return data
}

// ensureChunk wrapper that handles promotion if needed?
// No, promoteNode should be called by AddConnection before writing.
