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
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

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
		return
	}

	for _, t := range m.targets {
		if t.gd == oldGD {
			t.gd = newGD
			if newGD != nil {
				newGD.OnNeighborsMiss = func(layer int) error {
					return m.RestoreLayer(t, layer)
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

// Stop halts the background goroutine.
func (m *GraphLayerEvictionManager) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
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

// evictTarget evicts Layer 0 from a target, pinning layers ≥ 1.
func (m *GraphLayerEvictionManager) evictTarget(t *evictionTarget) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	gd := t.gd
	if gd == nil {
		return nil
	}

	numLayers := len(gd.Neighbors)
	if numLayers <= 1 {
		return nil // Only layer 0 — nothing to evict
	}

	var totalFreedBytes int64

	// Only evict Layer 0 to preserve pinned upper layers for entry-point search
	layer := 0
	if _, alreadyEvicted := t.evictedLayers[layer]; !alreadyEvicted {

		rec, freedBytes, err := evictLayer(gd, layer)
		if err != nil {
			m.logger.Warn().Err(err).Int("layer", layer).Msg("Failed to evict HNSW layer")
		} else {
			t.evictedLayers[layer] = rec
			totalFreedBytes += freedBytes
		}
	}

	if totalFreedBytes > 0 {
		m.logger.Info().
			Int64("freed_bytes", totalFreedBytes).
			Int64("freed_mb", totalFreedBytes/(1024*1024)).
			Str("dataset", gd.Name).
			Msg("HNSW Layer 0 evicted to disk")
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
		if werr := binary.Write(f, binary.LittleEndian, chunk); werr != nil {
			_ = os.Remove(f.Name())
			return nil, 0, fmt.Errorf("write chunk layer=%d chunk=%d: %w", layer, cID, werr)
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
	if gd == nil || gd.Uint32Arena == nil {
		return fmt.Errorf("cannot restore layer %d: nil GraphData or arena", layer)
	}

	f, err := os.Open(rec.path)
	if err != nil {
		return fmt.Errorf("open evicted layer file: %w", err)
	}
	defer f.Close()

	for cID := 0; cID < rec.numChunks; cID++ {
		sz := rec.chunkSizes[cID]
		if sz == 0 {
			continue
		}

		buf := make([]uint32, sz)
		if err := binary.Read(f, binary.LittleEndian, buf); err != nil {
			return fmt.Errorf("read chunk layer=%d chunk=%d: %w", layer, cID, err)
		}

		// Allocate a new slab for this chunk
		ref, allocErr := gd.Uint32Arena.AllocSlice(sz)
		if allocErr != nil {
			return fmt.Errorf("alloc chunk layer=%d chunk=%d: %w", layer, cID, allocErr)
		}

		// Copy restored data into arena
		chunk := gd.Uint32Arena.Get(ref)
		copy(chunk, buf)
		atomic.StoreUint64(&gd.Neighbors[layer][cID], ref.Offset)
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
