package index

import (
	"fmt"
	"math"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
)

func (h *ArrowHNSW) ensureChunk(cID, cOff, dims int) (*types.GraphData, error) {
	h.growMu.Lock()
	defer h.growMu.Unlock()

	newData, _, err := h.ensureChunkInternalLocked(cID, cOff, dims)
	return newData, err
}

func (h *ArrowHNSW) ensureChunkInternalLocked(cID, cOff, dims int) (newData *types.GraphData, cloned bool, err error) {
	data := h.data.Load()
	if data == nil {
		// First chunk ever
		capacity := h.config.InitialCapacity
		if capacity < (cID+1)*types.ChunkSize {
			capacity = (cID + 1) * types.ChunkSize
		}
		if err := h.growInternal(capacity, dims); err != nil {
			return nil, false, err
		}
		data = h.data.Load()
		return data, true, nil
	}

	if !data.NeedsChunk(cID) {
		return data, false, nil
	}

	// Grow capacity if needed
	if (cID+1)*types.ChunkSize > data.Capacity {
		newCap := (cID + 1) * types.ChunkSize
		if newCap < data.Capacity*2 {
			newCap = data.Capacity * 2
		}
		if err := h.growInternal(newCap, dims); err != nil {
			return nil, false, err
		}
		data = h.data.Load()
		return data, true, nil
	}

	// Just allocate the chunk within existing capacity IN-PLACE (Lock-Free atomic publishing)
	if err := data.EnsureChunk(cID, cOff, dims); err != nil {
		return nil, false, err
	}
	return data, false, nil
}

// Grow expands the index capacity to the specified size.
func (h *ArrowHNSW) Grow(capacity, dims int) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	return h.growInternal(capacity, dims)
}

func (h *ArrowHNSW) growInternal(capacity, dims int) error {
	if dims > math.MaxInt32 {
		return fmt.Errorf("dimensions %d exceed MaxInt32", dims)
	}

	start := time.Now()
	defer func() {
		metrics.HNSWIndexGrowthDuration.Observe(time.Since(start).Seconds())
	}()

	oldData := h.data.Load()
	if oldData != nil && capacity <= oldData.Capacity && dims == oldData.Dims &&
		oldData.PQEnabled == h.config.PQEnabled &&
		oldData.SQ8Enabled == h.config.SQ8Enabled &&
		oldData.BQEnabled == h.config.BQEnabled {
		return nil
	}

	var newData *types.GraphData
	if oldData == nil {
		newData = types.NewGraphData(
			capacity,
			dims,
			false, // mmap
			false, // useDisk
			0,     // fd
			h.config.Quantization,
			h.config.SQ8Enabled,
			false, // persistent
			h.config.DataType,
			h.config.BQEnabled,
			h.config.PQEnabled,
			h.config.TurboQuantEnabled,
			h.config.TurboQuantBits,
			h.name,
			nil, // allocator
			h.sharedVectorSpace.Load(),
		)
	} else {
		newData = oldData.Clone()
		newData.Capacity = capacity
		newData.Dims = dims
		// DEBUG: log PackedAdjacency sharing
		for l := 0; l < types.ArrowMaxLayers && l < len(oldData.PackedNeighbors) && l < len(newData.PackedNeighbors); l++ {
			if oldData.PackedNeighbors[l] != newData.PackedNeighbors[l] {
			}
		}
	}

	newData.PQEnabled = h.config.PQEnabled
	if newData.PQEnabled {
		if h.oopqEncoder != nil {
			switch enc := h.oopqEncoder.(type) {
			case *pq.PQEncoder:
				newData.PQM = enc.CodeSize()
			case *pq.OPQEncoder:
				newData.PQM = enc.CodeSize()
			}
		} else {
			newData.PQM = h.config.PQM
		}
	}
	newData.SQ8Enabled = h.config.SQ8Enabled
	newData.BQEnabled = h.config.BQEnabled
	newData.TurboQuantEnabled = h.config.TurboQuantEnabled
	newData.TurboQuantBits = h.config.TurboQuantBits

	// Ensure metadata slices are appropriately sized
	numChunks := (capacity + types.ChunkSize - 1) / types.ChunkSize
	if numChunks <= 0 {
		numChunks = 1
	}
	newData.GrowMetadataSlices(numChunks)

	// Ensure structural allocation
	if err := newData.PreAllocate(capacity); err != nil {
		return err
	}

	h.compareAndSwapData(oldData, newData)

	// If UseDisk and disk hasn't been flushed yet, flush after growth
	if h.config.UseDisk && !h.diskFlushed.Load() {
		meta := h.metadataRegistry.Load()
		if meta != nil && meta.NodeCount > 1000 {
			_ = h.FlushToDisk()
		}
	}

	return nil
}

func (h *ArrowHNSW) growNoLock(capacity, dims int) error {
	return h.growInternal(capacity, dims)
}

// EnsureChunks guarantees that the specified range of chunks is allocated and ready for ingestion.
// It handles thread-safe growth if the current capacity is insufficient.
func (h *ArrowHNSW) EnsureChunks(startCID, endCID int, dims int) (*types.GraphData, error) {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	return h.ensureChunksLocked(startCID, endCID, dims)
}

func (h *ArrowHNSW) ensureChunksLocked(startCID, endCID int, dims int) (*types.GraphData, error) {
	data := h.data.Load()
	needsGrow := false
	if data == nil || (endCID+1)*types.ChunkSize > data.Capacity {
		needsGrow = true
	}

	if needsGrow {
		newCap := (endCID + 1) * types.ChunkSize
		if data != nil && newCap < data.Capacity*2 {
			newCap = data.Capacity * 2
		}
		if err := h.growInternal(newCap, dims); err != nil {
			return nil, err
		}
		data = h.data.Load()
	}

	// Ensure all chunks are allocated IN-PLACE
	for cID := startCID; cID <= endCID; cID++ {
		if data.NeedsChunk(cID) {
			if err := data.EnsureChunk(cID, 0, dims); err != nil {
				return nil, err
			}
		}
	}

	return data, nil
}

func (h *ArrowHNSW) compareAndSwapData(current, newData *types.GraphData) bool {
	if current == newData {
		return true
	}
	if current != nil && newData != nil && newData.Capacity < current.Capacity {
		return false
	}
	if newData != nil {
		newData.OnEvict = func(layer int) {
			if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
				h.neighborCache[layer].Clear()
			}
		}
		// Automatically register or swap newData with the eviction manager if present in the dataset
		if h.dataset != nil {
			if provider, ok := h.dataset.(interface {
				GetEvictionManager() any
			}); ok {
				if evMgrAny := provider.GetEvictionManager(); evMgrAny != nil {
					if evMgr, ok := evMgrAny.(*GraphLayerEvictionManager); ok && evMgr != nil {
						evMgr.SwapTarget(current, newData)
					}
				}
			}
		}
	}
	swapped := h.data.CompareAndSwap(current, newData)
	if swapped {
		for i := 0; i < len(h.neighborCache); i++ {
			if h.neighborCache[i] != nil {
				h.neighborCache[i].Clear()
			}
		}
		if current != nil {
			current.Release()
		}
	}
	return swapped
}
