package index

// GraphLayerEvictionManager evicts cold upper HNSW graph layers (layers ≥ 1) to disk
// when memory pressure exceeds a configurable threshold, and transparently restores
// them on the next access.
//
// # Background
//
// An HNSW graph has a layered structure:
//   - Layer 0: O(N·M) neighbor entries. Usually too large for RAM at scale. Eligible for eviction.
//   - Layers ≥ 1: O(N·log(N)) entries. Small footprint, critical for entry-point traversal. Pinned in memory.
//
// At 500K float32 dim=384 vectors, Layer 0 holds ~1GB of neighbor
// data. Evicting it to a temp file frees this memory without
// any correctness impact — they are transparently restored on cache miss.
//
// # Integration
//
// After autoshard migration completes, wire this into the ShardedHNSW:
//
//	evMgr := index.NewGraphLayerEvictionManager(0.75, logger)
//	evMgr.Register(shardedHNSW)
//
// The manager runs a background goroutine checking memory pressure every 30 seconds.

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/rs/zerolog"
)

// GraphLayerEvictionManager monitors memory utilization and evicts cold HNSW
// upper-layer neighbor data to disk when the heap exceeds a threshold.
type GraphLayerEvictionManager struct {
	mu        sync.Mutex
	threshold float64 // utilization ratio to trigger eviction (e.g. 0.75)
	logger    zerolog.Logger
	targets   []*evictionTarget

	stopCh   chan struct{}
	stopOnce sync.Once
}

// evictionTarget tracks the eviction state for one GraphData (one ArrowHNSW shard).
type evictionTarget struct {
	mu            sync.RWMutex
	gd            *types.GraphData
	evictedLayers map[int]*layerDiskRecord // layer → disk record
}

// layerDiskRecord holds the path and sizes of an evicted layer's neighbor chunks on disk.
type layerDiskRecord struct {
	path       string // path to the temp file
	chunkSizes []int  // number of uint32 elements per chunk (for restore sizing)
	numChunks  int
}

// NewGraphLayerEvictionManager creates a new manager.
// threshold is the heap utilization ratio (0.0–1.0) at which eviction triggers.
func NewGraphLayerEvictionManager(threshold float64, logger zerolog.Logger) *GraphLayerEvictionManager {
	return &GraphLayerEvictionManager{
		threshold: threshold,
		logger:    logger.With().Str("component", "layer-eviction-manager").Logger(),
		stopCh:    make(chan struct{}),
	}
}

// Register adds a GraphData to the set of candidates for upper-layer eviction.
func (m *GraphLayerEvictionManager) Register(gd *types.GraphData) {
	if gd == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Avoid duplicate registration
	for _, t := range m.targets {
		if t.gd == gd {
			return
		}
	}

	target := &evictionTarget{
		gd:            gd,
		evictedLayers: make(map[int]*layerDiskRecord),
	}
	m.targets = append(m.targets, target)

	// Set the callback on GraphData to transparently restore when needed!
	gd.OnNeighborsMiss = func(layer int) error {
		return m.RestoreLayer(target, layer)
	}

	// Wire PackedNeighbors FlatAdjacency instances with the same restore callback.
	for l := range gd.PackedNeighbors {
		if fa, ok := gd.PackedNeighbors[l].(*FlatAdjacency); ok && fa != nil {
			fa.MissCallback = gd.OnNeighborsMiss
		}
	}
}

// SwapTarget updates the registered GraphData reference when HNSW grows/swaps its internal data structure,
// preserving the eviction state and binding the cache-miss restore callback.
func (m *GraphLayerEvictionManager) SwapTarget(oldGD, newGD *types.GraphData) {
	if oldGD == newGD {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// If oldGD is nil but newGD is not, we might be doing lazy initialization, so we can treat it as Register!
	if oldGD == nil && newGD != nil {
		// Check if already registered first to avoid duplicates
		for _, t := range m.targets {
			if t.gd == newGD {
				return
			}
		}
		target := &evictionTarget{
			gd:            newGD,
			evictedLayers: make(map[int]*layerDiskRecord),
		}
		m.targets = append(m.targets, target)
		newGD.OnNeighborsMiss = func(layer int) error {
			return m.RestoreLayer(target, layer)
		}
		for l := range newGD.PackedNeighbors {
			if fa, ok := newGD.PackedNeighbors[l].(*FlatAdjacency); ok && fa != nil {
				fa.MissCallback = newGD.OnNeighborsMiss
			}
		}
		return
	}

	for _, t := range m.targets {
		if t.gd == oldGD {
			t.gd = newGD
			if newGD != nil {
				newGD.OnNeighborsMiss = func(layer int) error {
					return m.RestoreLayer(t, layer)
				}
				for l := range newGD.PackedNeighbors {
					if fa, ok := newGD.PackedNeighbors[l].(*FlatAdjacency); ok && fa != nil {
						fa.MissCallback = newGD.OnNeighborsMiss
					}
				}
			}
			break
		}
	}
}

// Start launches the background pressure monitor.
func (m *GraphLayerEvictionManager) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-ticker.C:
				m.maybeEvictAll()
			}
		}
	}()
}

// Unregister removes a GraphData and cleans up any on-disk evicted files for it.
func (m *GraphLayerEvictionManager) Unregister(gd *types.GraphData) {
	if gd == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, t := range m.targets {
		if t.gd == gd {
			t.mu.Lock()
			for _, rec := range t.evictedLayers {
				if rec != nil && rec.path != "" {
					_ = os.Remove(rec.path)
				}
			}
			t.evictedLayers = make(map[int]*layerDiskRecord)
			t.mu.Unlock()
			m.targets = append(m.targets[:i], m.targets[i+1:]...)
			break
		}
	}
}

// Stop halts the background goroutine and cleans up any on-disk evicted files.
func (m *GraphLayerEvictionManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, target := range m.targets {
			target.mu.Lock()
			for _, rec := range target.evictedLayers {
				if rec != nil && rec.path != "" {
					_ = os.Remove(rec.path)
				}
			}
			target.evictedLayers = make(map[int]*layerDiskRecord)
			target.mu.Unlock()
		}
		m.targets = nil
	})
}

// utilization returns the current heap-in-use / GOGC soft target ratio (approximate).
func currentHeapUtilization() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	// Use HeapSys as the upper bound for a stable utilization signal.
	if ms.HeapSys == 0 {
		return 0
	}
	return float64(ms.HeapInuse) / float64(ms.HeapSys)
}

func (m *GraphLayerEvictionManager) maybeEvictAll() {
	util := currentHeapUtilization()
	if util < m.threshold {
		return
	}
	m.logger.Warn().
		Float64("heap_utilization", util).
		Float64("threshold", m.threshold).
		Msg("Heap above eviction threshold; evicting Layer 0 neighbors")

	m.mu.Lock()
	targets := make([]*evictionTarget, len(m.targets))
	copy(targets, m.targets)
	m.mu.Unlock()

	for _, t := range targets {
		if err := m.evictTarget(t); err != nil {
			m.logger.Error().Err(err).Msg("Layer eviction failed for a target")
		}
	}
}

// evictTarget evicts neighbor data from a target, starting with upper
// layers (via PackedNeighbors) then falling back to layer 0 (via gd.Neighbors).
func (m *GraphLayerEvictionManager) evictTarget(t *evictionTarget) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	gd := t.gd
	if gd == nil {
		return nil
	}

	var totalFreedBytes int64

	// Evict layers from highest to lowest — upper layers first.
	// Upper layers use PackedNeighbors; layer 0 uses gd.Neighbors chunks.
	maxLayer := len(gd.PackedNeighbors)
	if maxLayer == 0 {
		maxLayer = len(gd.Neighbors)
	}

	for layer := maxLayer - 1; layer >= 0; layer-- {
		if _, alreadyEvicted := t.evictedLayers[layer]; alreadyEvicted {
			continue
		}

		// Try PackedNeighbors path first (upper layers use FlatAdjacency/PackedAdjacency)
		if layer < len(gd.PackedNeighbors) && gd.PackedNeighbors[layer] != nil {
			pn := gd.PackedNeighbors[layer]

			f, err := os.CreateTemp("", fmt.Sprintf("longbow_hnsw_packed_layer%d_*.bin", layer))
			if err != nil {
				m.logger.Warn().Err(err).Int("layer", layer).Msg("Failed to create temp file for packed layer eviction")
				continue
			}

			// Determine the number of chunks from the FlatAdjacency/PackedAdjacency.
			// We need to read chunks from the interface, but EvictToDisk needs chunkSizes.
			// Build the chunkSizes array lazily — start with a reasonable cap and let
			// EvictToDisk grow it as needed.
			chunkSizes := make([]int, 64)

			nChunks, chunkSizes, bytesWritten, evictErr := pn.EvictToDisk(gd, layer, chunkSizes, f)
			if evictErr != nil {
				_ = f.Close()
				_ = os.Remove(f.Name())
				m.logger.Warn().Err(evictErr).Int("layer", layer).Msg("Failed to evict packed layer")
				continue
			}
			_ = f.Close()

			if nChunks > 0 {
				rec := &layerDiskRecord{
					path:       f.Name(),
					chunkSizes: chunkSizes,
					numChunks:  nChunks,
				}
				t.evictedLayers[layer] = rec
				totalFreedBytes += bytesWritten
			} else {
				_ = os.Remove(f.Name())
			}

			continue
		}

		// Legacy path: evict from gd.Neighbors[layer] arena chunks
		if layer < len(gd.Neighbors) && len(gd.Neighbors[layer]) > 0 {
			rec, freedBytes, err := evictLayer(gd, layer)
			if err != nil {
				m.logger.Warn().Err(err).Int("layer", layer).Msg("Failed to evict HNSW layer")
			} else if rec != nil {
				t.evictedLayers[layer] = rec
				totalFreedBytes += freedBytes
			}
		}
	}

	if totalFreedBytes > 0 {
		m.logger.Info().
			Int64("freed_bytes", totalFreedBytes).
			Int64("freed_mb", totalFreedBytes/(1024*1024)).
			Str("dataset", gd.Name).
			Msg("HNSW layers evicted to disk")
	}
	return nil
}

// evictLayer serializes the Neighbors chunks for a single layer to a temp file,
// zeros the in-memory offsets to allow GC, and returns the disk record.
func evictLayer(gd *types.GraphData, layer int) (rec *layerDiskRecord, freedBytes int64, err error) {
	chunks := gd.Neighbors[layer]
	if len(chunks) == 0 {
		return nil, 0, nil
	}

	f, err := os.CreateTemp("", fmt.Sprintf("longbow_hnsw_layer%d_*.bin", layer))
	if err != nil {
		return nil, 0, fmt.Errorf("create temp file: %w", err)
	}
	defer f.Close()

	rec = &layerDiskRecord{
		path:       f.Name(),
		numChunks:  len(chunks),
		chunkSizes: make([]int, len(chunks)),
	}

	for cID := range chunks {
		offset := atomic.LoadUint64(&gd.Neighbors[layer][cID])
		if offset == 0 {
			rec.chunkSizes[cID] = 0
			continue
		}
		chunk := gd.Uint32Arena.Get(memory.SliceRef{
			Offset: offset,
			Len:    uint32(types.ChunkSize * types.MaxNeighbors),
			Cap:    uint32(types.ChunkSize * types.MaxNeighbors),
		})
		rec.chunkSizes[cID] = len(chunk)

		// Write as raw little-endian uint32 array
		if len(chunk) > 0 {
			byteSlice := unsafe.Slice((*byte)(unsafe.Pointer(&chunk[0])), len(chunk)*4) // #nosec G103
			if _, werr := f.Write(byteSlice); werr != nil {
				_ = os.Remove(f.Name())
				return nil, 0, fmt.Errorf("write chunk layer=%d chunk=%d: %w", layer, cID, werr)
			}
		}
		freedBytes += int64(len(chunk)) * 4 // 4 bytes per uint32

		// Zero the offset so the arena slab can be reclaimed by GC
		atomic.StoreUint64(&chunks[cID], 0)
	}

	// Call OnEvict if registered to clear any high-level caches (like HNSW neighborCache)
	if gd.OnEvict != nil {
		gd.OnEvict(layer)
	}

	return rec, freedBytes, nil
}

// RestoreLayer reads layer neighbor data back from disk into the arena.
// Called by GetNeighborsChunkFast on cache miss for an evicted layer.
func (m *GraphLayerEvictionManager) RestoreLayer(t *evictionTarget, layer int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.evictedLayers[layer]
	if !ok {
		return nil // Not evicted
	}

	gd := t.gd
	if gd == nil {
		return fmt.Errorf("cannot restore layer %d: nil GraphData", layer)
	}

	f, err := os.Open(rec.path)
	if err != nil {
		return fmt.Errorf("open evicted layer file: %w", err)
	}
	defer f.Close()

	// Restore into PackedNeighbors first (upper layers use FlatAdjacency)
	if layer < len(gd.PackedNeighbors) && gd.PackedNeighbors[layer] != nil {
		if fa, ok := gd.PackedNeighbors[layer].(*FlatAdjacency); ok {
			if err := fa.RestoreFromDisk(gd, layer, rec.chunkSizes, f); err != nil {
				return fmt.Errorf("restore packed layer %d: %w", layer, err)
			}
		}
	} else if layer < len(gd.Neighbors) && len(gd.Neighbors[layer]) > 0 && gd.Uint32Arena != nil {
		// Legacy path: restore into gd.Neighbors arena chunks
		for cID := 0; cID < rec.numChunks; cID++ {
			sz := rec.chunkSizes[cID]
			if sz == 0 {
				continue
			}

			buf := make([]uint32, sz)
			if len(buf) > 0 {
				byteBuf := unsafe.Slice((*byte)(unsafe.Pointer(&buf[0])), len(buf)*4) // #nosec G103
				if _, err := io.ReadFull(f, byteBuf); err != nil {
					return fmt.Errorf("read chunk layer=%d chunk=%d: %w", layer, cID, err)
				}
			}

			ref, allocErr := gd.Uint32Arena.AllocSlice(sz)
			if allocErr != nil {
				return fmt.Errorf("alloc chunk layer=%d chunk=%d: %w", layer, cID, allocErr)
			}

			chunk := gd.Uint32Arena.Get(ref)
			copy(chunk, buf)
			atomic.StoreUint64(&gd.Neighbors[layer][cID], ref.Offset)
		}
	}

	// Remove disk file and clear eviction record
	_ = os.Remove(rec.path)
	delete(t.evictedLayers, layer)

	m.logger.Info().
		Int("layer", layer).
		Str("dataset", gd.Name).
		Msg("HNSW layer restored from disk")
	return nil
}

// ForceEvictAll triggers eviction for all targets immediately, regardless of memory pressure.
func (m *GraphLayerEvictionManager) ForceEvictAll() {
	m.mu.Lock()
	targets := make([]*evictionTarget, len(m.targets))
	copy(targets, m.targets)
	m.mu.Unlock()

	for _, t := range targets {
		_ = m.evictTarget(t)
	}
}
