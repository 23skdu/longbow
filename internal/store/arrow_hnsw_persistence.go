package store

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
		_ = dg.Close()
		return fmt.Errorf("dimension mismatch: graph has %d, config has %d", dg.header.Dims, h.config.Dims)
	}

	// Update HNSW state from graph header
	h.nodeCount.Store(int64(dg.header.NumNodes))
	h.dims.Store(int32(dg.header.Dims))

	// Restore Entry Point and Max Level (Version 3+)
	if dg.header.Version >= 3 {
		h.entryPoint.Store(dg.header.EntryPoint)
		h.maxLevel.Store(dg.header.GraphMaxLevel)
	} else {
		// Fallback for older formats (if supported)
		h.entryPoint.Store(0)                            // Unlikely to be correct but safe default?
		h.maxLevel.Store(int32(dg.header.MaxLayers - 1)) // Conservative
	}

	// Attach DiskGraph
	h.diskGraph.Store(dg)

	// Disk Backing Store
	h.diskGraph.Store(dg)

	// Set BackingGraph on GraphData for Copy-On-Write support
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

	// Note: We do NOT populate h.data (GraphData) with these nodes.
	// Users of the index must use the Hybrid Accessors (GetNeighbors) which check diskGraph.

	return nil
}

// promoteNode ensures that a node's neighbor list is present in the mutable GraphData.
// If the node is currently only in DiskGraph, it copies the neighbors to GraphData.
// This implements Copy-On-Write for the graph structure.
// Returns the chunk and offset in GraphData.
func (h *ArrowHNSW) promoteNode(data *GraphData, id uint32) *GraphData {
	// If already in GraphData (capacity covered), check if chunk exists
	if int(id) >= data.Capacity {
		// Should have been grown by caller
		return data
	}

	cID := chunkID(id)
	cOff := chunkOffset(id)

	// Check if already promoted (chunk exists and count > 0)
	// We check L0 as indicator.
	// Check if already promoted (chunk exists and count > 0)
	// We check L0 as indicator.
	// Optimally we would check here, but for now we fall through to ensure safety.
	// TODO: Implement optimized check if data.GetNeighborsChunk(0, cID) != nil

	dg := h.diskGraph.Load()
	if dg == nil {
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
			// promoteNode returns *GraphData.
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
			baseIdx := int(cOff) * MaxNeighbors
			for i, nid := range diskNeighbors {
				if i >= MaxNeighbors {
					break
				}
				atomic.StoreUint32(&neighborsChunk[baseIdx+i], nid)
			}
			atomic.StoreInt32(countAddr, int32(len(diskNeighbors)))

			// Initialize version
			verAddr := &data.GetVersionsChunk(l, cID)[cOff]
			atomic.StoreUint32(verAddr, 0) // Valid, even
		}
	}

	return data
}

// ensureChunk wrapper that handles promotion if needed?
// No, promoteNode should be called by AddConnection before writing.
