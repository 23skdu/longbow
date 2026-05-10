package core

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"sync/atomic"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

// SnapshotHNSW exports the current graph state for persistence.
func (h *ArrowHNSW) SnapshotHNSW() error {
	return nil
}

// ExportState implements VectorIndex.
func (h *ArrowHNSW) ExportState() ([]byte, error) {
	var buf bytes.Buffer
	if err := h.ExportGraph(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportState implements VectorIndex.
func (h *ArrowHNSW) ImportState(data []byte) error {
	return h.ImportGraph(bytes.NewReader(data))
}

// ExportGraph implements VectorIndex.
func (h *ArrowHNSW) ExportGraph(w io.Writer) error {
	h.growMu.Lock() // Use Write Lock to ensure consistent snapshot against concurrent insertions
	defer h.growMu.Unlock()

	// 1. Capture Snapshot + Metadata
	var snapshot *types.GraphData
	locs := make([]types.Location, 0, h.locationStore.Len())
	dims := int(h.dims.Load())

	if data := h.data.Load(); data != nil {
		snapshot = data.CloneForSnapshot()
		// Capture PackedNeighbors state into legacy slices for serialization
		for l, pn := range snapshot.PackedNeighbors {
			if pn == nil { continue }
			for i := uint32(0); i < uint32(snapshot.Capacity); i++ { // #nosec G115
				if neighbors, ok := pn.GetNeighbors(i); ok {
					_ = snapshot.SetNeighborsAtLayer(l, i, neighbors)
				}
			}
		}
	}

	size := h.locationStore.Len()
	for i := 0; i < size; i++ {
		loc, ok := h.locationStore.Get(types.VectorID(i))
		if ok {
			locs = append(locs, loc)
		} else {
			locs = append(locs, types.Location{})
		}
	}

	if snapshot == nil {
		return fmt.Errorf("no graph data to export")
	}

	meta := h.GetMetadataSnapshot()
	state := types.SyncState{
		Version:    1,
		Dims:       dims,
		EntryPoint: meta.EntryPoint,
		MaxLevel:   meta.MaxLevel,
		Generation: meta.Generation,
		Locations:  locs,
	}

	// Use temporary buffer for metadata part
	var metaBuf bytes.Buffer
	if err := gob.NewEncoder(&metaBuf).Encode(state); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}
	metaBytes := metaBuf.Bytes()

	// Write Metadata Length + Bytes
	if err := binary.Write(w, binary.LittleEndian, uint32(len(metaBytes))); err != nil { // #nosec G115
		return err
	}
	if _, err := w.Write(metaBytes); err != nil {
		return err
	}

	// 3. Export Snapshot types.GraphData
	return snapshot.Serialize(w)
}

// ImportGraph implements VectorIndex.
func (h *ArrowHNSW) ImportGraph(r io.Reader) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	// 1. Read Metadata
	var metaLen uint32
	if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
		return fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaBytes := make([]byte, metaLen)
	if _, err := io.ReadFull(r, metaBytes); err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var state types.SyncState
	if err := gob.NewDecoder(bytes.NewReader(metaBytes)).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	// Apply Metadata
	if state.Dims > math.MaxInt32 {
		return fmt.Errorf("state dimensions %d exceed MaxInt32", state.Dims)
	}
	h.dims.Store(int32(state.Dims)) // #nosec G115
	h.updateMetadata(func(meta *HNSWMetadata) {
		meta.EntryPoint = state.EntryPoint
		meta.MaxLevel = state.MaxLevel
		meta.Generation = state.Generation
		meta.NodeCount = int64(len(state.Locations))
	})
	h.locationStore.Reset()
	for _, loc := range state.Locations {
		h.locationStore.Append(loc)
	}

	// 2. Read types.GraphData
	data, err := types.DeserializeGraphData(r)
	if err != nil {
		return fmt.Errorf("failed to deserialize graph data: %w", err)
	}

	// Recalculate EntryPoint and MaxLevel if they are missing or uninitialized
	meta := h.GetMetadataSnapshot()
	if meta.EntryPoint == math.MaxUint32 && len(state.Locations) > 0 {
		var bestID uint32 = 0
		var maxL int8 = -1
		// Scan levels to find entry point
		for i := 0; i < len(state.Locations); i++ {
			cID := i / types.ChunkSize
			cOff := i % types.ChunkSize
			levels := data.GetLevelsChunk(cID)
			if levels != nil {
				l := int8(atomic.LoadUint32(&levels[cOff])) // #nosec G115
				if l > maxL {
					maxL = l
					bestID = uint32(i)
				}
			}
		}
		if maxL >= 0 {
			h.entryPoint.Store(bestID)
			h.maxLevel.Store(int32(maxL))
		}
	}

	// Swap data
	h.data.Store(data)

	// Restore configuration flags
	h.config.BQEnabled = data.BQEnabled
	h.config.PQEnabled = data.PQEnabled
	h.config.PQM = data.PQM
	h.config.SQ8Enabled = data.SQ8Enabled
	// Restore node count from metadata (number of valid locations)
	h.nodeCount.Store(int64(len(state.Locations)))

	// Reset runtime structures
	if h.searchPool == nil {
		h.searchPool = NewArrowSearchContextPool()
	}

	return nil
}

// ExportDelta implements VectorIndex.
func (h *ArrowHNSW) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	currentLen := h.locationStore.Len()
	// Export locations starting from fromVersion up to currentLen
	startIdx := int(fromVersion) // #nosec G115
	if startIdx >= currentLen {
		return &types.DeltaSync{
			FromVersion:  fromVersion,
			ToVersion:    uint64(currentLen), // #nosec G115
			NewLocations: nil,
			StartIndex:   startIdx,
		}, nil
	}

	newLocs := make([]types.Location, 0, currentLen-startIdx)
	idx := 0
	h.locationStore.IterateMutable(func(_ types.VectorID, val *atomic.Uint64) {
		if idx >= startIdx {
			loc := basecore.UnpackLocation(val.Load())
			newLocs = append(newLocs, loc)
		}
		idx++
	})

	return &types.DeltaSync{
		FromVersion:  fromVersion,
		ToVersion:    uint64(currentLen), // #nosec G115
		NewLocations: newLocs,
		StartIndex:   startIdx,
	}, nil
}

// ApplyDelta implements VectorIndex.
func (h *ArrowHNSW) ApplyDelta(delta *types.DeltaSync) error {
	if delta == nil || len(delta.NewLocations) == 0 {
		return nil
	}

	h.growMu.Lock()
	defer h.growMu.Unlock()

	for i, loc := range delta.NewLocations {
		globalID := types.VectorID(delta.StartIndex + i) // #nosec G115
		h.locationStore.EnsureCapacity(globalID)
		h.locationStore.Set(globalID, loc)
	}

	h.locationStore.UpdateSize(types.VectorID(delta.StartIndex + len(delta.NewLocations) - 1)) // #nosec G115

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

	// If chunk is already in memory, no need to clone or promote.
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

	cID := int(id) / types.ChunkSize  // #nosec G115
	cOff := int(id) % types.ChunkSize // #nosec G115

	// If already in memory, no need to promote from disk
	if data.GetNeighborsChunk(0, cID) != nil {
		return data
	}

	dg := h.diskGraph.Load()
	if dg == nil {
		return data
	}

	// Ensure chunk in Mutable Data (L0-LMax) using private locked version
	dims := int(h.dims.Load())
	newData, cloned, err := h.ensureChunkInternalLocked(cID, cOff, dims)
	if err != nil {
		return data
	}

	// If ensureChunkInternalLocked didn't clone, we MUST clone now because we are going to modify neighbors.
	// We MUST NOT modify any GraphData that was ever published (i.e. currently in h.data).
	if !cloned {
		newData = newData.Clone()
		cloned = true
	}

	// Copy neighbors from disk for all layers
	for l := 0; l < types.ArrowMaxLayers; l++ {
		diskNeighbors := dg.GetNeighbors(l, id, nil)
		if len(diskNeighbors) == 0 {
			continue
		}
		countsChunk := newData.GetCountsChunk(l, cID)
		neighborsChunk := newData.GetNeighborsChunk(l, cID)
		if countsChunk == nil || neighborsChunk == nil {
			continue
		}

		// Copy neighbors to chunk
		limit := h.mMax.Load()
		if l == 0 {
			limit = h.mMax0.Load()
		}

		start := int(cOff) * types.MaxNeighbors
		for i, nID := range diskNeighbors {
			if i < int(limit) {
				neighborsChunk[start+i] = nID
			}
		}
		atomic.StoreInt32(&countsChunk[cOff], int32(min(len(diskNeighbors), int(limit)))) // #nosec G115
	}

	// Publish the newly consistent data structure using CAS
	h.compareAndSwapData(h.data.Load(), newData)
	return newData
}

// Close cleans up resources associated with the index.
func (h *ArrowHNSW) Close() error {
	if h == nil {
		return nil
	}
	if h.navigator != nil {
		if err := h.navigator.Close(); err != nil {
			return err
		}
	}
	// Atomically swap in nil to stop new operations
	data := h.data.Swap(nil)
	if data != nil {
		data.Release()
	}
	// We do NOT nil locationStore, searchPool, etc. here because concurrent 
	// background tasks (like indexing workers or migration) might still 
	// be accessing them. The memory will be reclaimed when the ArrowHNSW 
	// object itself is no longer referenced.
	return nil
}

// SnapshotGraph captures the current graph state for serialization.
func (h *ArrowHNSW) SnapshotGraph() (*types.GraphData, *types.SyncState, error) {
	h.growMu.RLock()
	defer h.growMu.RUnlock()

	data := h.data.Load()
	if data == nil {
		return nil, nil, fmt.Errorf("no graph data to snapshot")
	}

	snap := data.CloneForSnapshot()

	locs := make([]types.Location, 0, h.locationStore.Len())
	h.locationStore.IterateMutable(func(_ types.VectorID, val *atomic.Uint64) {
		loc := basecore.UnpackLocation(val.Load())
		locs = append(locs, loc)
	})

	// Capture PackedNeighbors state
	for l, pn := range data.PackedNeighbors {
		if pn == nil { continue }
		// Ensure legacy slices are in sync for this snapshot
		// This is a trade-off: snapshots become slightly more expensive,
		// but we maintain compatibility with the existing serialization format.
		for i := uint32(0); i < uint32(data.Capacity); i++ { // #nosec G115
			if neighbors, ok := pn.GetNeighbors(i); ok {
				_ = data.SetNeighborsAtLayer(l, i, neighbors)
			}
		}
	}

	state := &types.SyncState{
		Version:   1,
		Dims:      int(h.dims.Load()),
		Locations: locs,
	}

	return snap, state, nil
}
