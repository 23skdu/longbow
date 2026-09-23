package index

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
)

// ShardedHNSWConfig configures the sharded HNSW index.
type ShardedHNSWConfig struct {
	NumShards      int            // Initial/Currently active shards
	M              int            // HNSW M parameter
	EfConstruction int            // HNSW efConstruction parameter
	Metric         DistanceMetric // Distance metric for this index
	Dimension      uint32         // Vector dimension
	DataType       VectorDataType // Vector data type (float32, complex64, etc.)
	// ShardSplitThreshold is deprecated in favor of Ring Sharding but kept for interface/legacy compatibility.
	// In Ring mode, it implies the *initial capacity* of each shard.
	ShardSplitThreshold    int
	UseRingSharding        bool // If true, use Consistent Hashing (Ring). If false, use Linear Range.
	PackedAdjacencyEnabled bool // If true, use thread-safe packed neighbor storage (v0.1.4)
	SharedVectorSpace      bool // If true, shards use primary types.IndexDataProvider records for vector lookups
	IndexFactory           func(shardIdx int) VectorIndex
}

// Validate ensures the ShardedHNSWConfig is well-formed.
func (c ShardedHNSWConfig) Validate() error {
	if c.NumShards <= 0 {
		return fmt.Errorf("numShards must be > 0")
	}
	if c.M <= 0 {
		return fmt.Errorf("m must be > 0")
	}
	if c.EfConstruction <= 0 {
		return fmt.Errorf("efConstruction must be > 0")
	}
	if c.EfConstruction > math.MaxInt32 {
		return fmt.Errorf("efConstruction exceeds MaxInt32")
	}
	return nil
}

// DefaultShardedHNSWConfig returns sensible defaults.
func DefaultShardedHNSWConfig() ShardedHNSWConfig {
	return ShardedHNSWConfig{
		NumShards:              runtime.NumCPU(),
		M:                      32,
		EfConstruction:         400,
		Metric:                 core.MetricEuclidean,
		ShardSplitThreshold:    65536, // ~64k vectors per shard (L3 Cache Alignment)
		UseRingSharding:        true,  // Default to Ring
		PackedAdjacencyEnabled: true,
		SharedVectorSpace:      true, // Enable by default for sharded indexes (v0.2.1)
	}
}

// hnswShard represents a single HNSW graph shard backed by any VectorIndex.
type hnswShard struct {
	index         VectorIndex
	locationStore *ChunkedLocationStore // Global -> Local mapping (deprecated in favor of ShardedHNSW.globalToLocal)
}

func newHnswShard(idx VectorIndex) *hnswShard {
	return &hnswShard{
		index:         idx,
		locationStore: NewChunkedLocationStore(),
	}
}

// registerID records the mapping between a local shard ID and a global VectorID.
func (s *hnswShard) registerID(localID uint32, globalID VectorID, globalToLocal *ChunkedLocationStore) {
	// 1. Store Global -> Local mapping (Lock-Free)
	if globalToLocal != nil {
		globalToLocal.EnsureCapacity(globalID)
		// We pack the LocalID into a Location structure (BatchIdx = localID)
		globalToLocal.Set(globalID, Location{BatchIdx: int(localID)})
		globalToLocal.UpdateSize(globalID)
	}
}

// getGlobalID is now managed at the ShardedHNSW level using the index directly
// or via a reverse lookup if needed. For now, we rely on the fact that
// ShardedHNSW knows which GlobalID belongs to which LocalID during search.

// Warmup accesses all nodes in the shard.
func (s *hnswShard) Warmup() int {
	if s.index == nil {
		return 0
	}
	return s.index.Warmup()
}

// ShardedHNSW provides fine-grained locking via multiple independent HNSW shards.
// It uses Lock-Free Sharding or Ring Sharding strategies.
type ShardedHNSW struct {
	config  ShardedHNSWConfig
	shards  []*hnswShard
	dataset types.IndexDataProvider
	nextID  atomic.Int64

	// Location Storage (Lock-Free Read)
	locationStore *ChunkedLocationStore
	globalToLocal *ChunkedLocationStore // Mapping GlobalID -> LocalID

	dimension uint32

	// Dynamic Sharding
	sharder  ShardingStrategy
	shardsMu sync.RWMutex

	// Per-shard insertion locks — each shard processes one batch at a time.
	// Multiple shards can proceed concurrently, enabling N-callers × M-shards parallelism.
	shardLocks []sync.Mutex

	parallelConfig types.ParallelSearchConfig
}

// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset types.IndexDataProvider) VectorIndex {
	if config.NumShards <= 0 {
		config.NumShards = 1 // Start with at least 1 shard
	}
	if config.ShardSplitThreshold <= 0 {
		config.ShardSplitThreshold = 65536
	}

	var sharder ShardingStrategy
	if config.UseRingSharding {
		sharder = NewRingSharder(config.NumShards, 40) // 40 vnodes/shard
	} else {
		sharder = NewLinearSharding(config.ShardSplitThreshold)
	}

	s := &ShardedHNSW{
		config:         config,
		dataset:        dataset,
		locationStore:  NewChunkedLocationStore(),
		globalToLocal:  NewChunkedLocationStore(),
		dimension:      config.Dimension,
		sharder:        sharder,
		parallelConfig: types.DefaultParallelSearchConfig(),
	}

	s.shards = make([]*hnswShard, config.NumShards)
	s.shardLocks = make([]sync.Mutex, config.NumShards)
	for i := 0; i < config.NumShards; i++ {
		s.shards[i] = s.newShard(i)
	}

	return s
}

func (idx *ShardedHNSW) newShard(shardIdx int) *hnswShard {
	if idx.config.IndexFactory != nil {
		id := idx.config.IndexFactory(shardIdx)
		if id != nil {
			return newHnswShard(id)
		}
	}

	// Map ShardedHNSWConfig to ArrowHNSWConfig
	arrowConfig := DefaultArrowHNSWConfig()
	arrowConfig.M = idx.config.M
	arrowConfig.MMax = idx.config.M * 3
	arrowConfig.MMax0 = idx.config.M * 2
	// ArrowHNSW uses int32 for performance and atomic safety
	arrowConfig.EfConstruction = int32(idx.config.EfConstruction) // #nosec G115
	arrowConfig.InitialCapacity = 1024                            // Start small, grow dynamically
	arrowConfig.Metric = idx.config.Metric
	arrowConfig.PackedAdjacencyEnabled = idx.config.PackedAdjacencyEnabled

	// Preserve DataType from config (critical for complex64/complex128)
	if idx.config.DataType != types.VectorTypeUnknown {
		arrowConfig.DataType = idx.config.DataType
	}

	if idx.config.DataType == types.VectorTypeTQ {
		arrowConfig.TurboQuantEnabled = true
		if idx.dataset != nil && idx.dataset.TurboQuantBits() > 0 {
			arrowConfig.TurboQuantBits = idx.dataset.TurboQuantBits()
		} else if arrowConfig.TurboQuantBits == 0 {
			arrowConfig.TurboQuantBits = 8
		}
	}

	// We pass nil for ChunkedLocationStore because shards use local IDs and don't manage global locations
	// The ShardedHNSW manages the global location store.
	var topo *memory.NUMATopology
	if idx.dataset != nil {
		topo = idx.dataset.GetTopo()
	}

	// Assign shard to NUMA node if topology is available
	if topo != nil && topo.NumNodes > 0 {
		arrowConfig.NUMANode = shardIdx % topo.NumNodes
	}

	id := NewArrowHNSW(idx.dataset, &arrowConfig, topo)
	if id == nil {
		return nil
	}
	id.SetDisableNodeCountMetric(true)

	// Correct dimension initialization
	_ = id.SetDimension(int(idx.dimension))

	// Register shard with EvictionManager if present
	if idx.dataset != nil {
		if evMgrAny := idx.dataset.GetEvictionManager(); evMgrAny != nil {
			if evMgr, ok := evMgrAny.(*GraphLayerEvictionManager); ok && evMgr != nil {
				evMgr.Register(id.GetData())
			}
		}
	}

	return newHnswShard(id)
}

// GetShardForID returns the shard index for a given Global VectorID.
func (idx *ShardedHNSW) GetShardForID(id VectorID) int {
	return idx.sharder.GetShard(id)
}

// IsSharded returns true as this is a sharded index implementation.
func (idx *ShardedHNSW) IsSharded() bool {
	return true
}

func (idx *ShardedHNSW) Shards() []VectorIndex {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	res := make([]VectorIndex, len(idx.shards))
	for i, s := range idx.shards {
		if s != nil {
			res[i] = s.index
		}
	}
	return res
}

// GetGPUIndex returns the GPU-accelerated index if available.
func (idx *ShardedHNSW) GetGPUIndex() any {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	if len(idx.shards) > 0 && idx.shards[0] != nil && idx.shards[0].index != nil {
		return idx.shards[0].index.GetGPUIndex()
	}
	return nil
}

// GetIndexType returns the string identifier for the sharded HNSW index.
func (idx *ShardedHNSW) GetIndexType() string {
	return "sharded_hnsw"
}

// Len returns the total number of vectors indexed across all shards.
func (idx *ShardedHNSW) Len() int {
	return int(idx.nextID.Load())
}

// Size returns the capacity/size of the sharded index.
func (idx *ShardedHNSW) Size() int {
	return idx.Len()
}

// Search implements the VectorIndexer interface (fallback).
func (idx *ShardedHNSW) Search(ctx context.Context, queryVal any, k int, filter any) ([]types.Candidate, error) {
	return nil, fmt.Errorf("use SearchVectors for sharded search")
}

// Warmup warms up all shards.
func (idx *ShardedHNSW) Warmup() int {
	total := 0
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	for _, shard := range idx.shards {
		if shard != nil {
			total += shard.Warmup()
		}
	}
	return total
}

// SetIndexedColumns satisfies the VectorIndex interface.
func (idx *ShardedHNSW) SetIndexedColumns(cols []string) {
}

// Close releases resources for all shards in the index.
func (idx *ShardedHNSW) Close() error {
	idx.shardsMu.Lock()
	defer idx.shardsMu.Unlock()
	var lastErr error
	for _, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			if err := shard.index.Close(); err != nil {
				lastErr = err
			}
		}
	}
	idx.shards = nil

	if idx.locationStore != nil {
		idx.locationStore.Close()
	}
	if idx.globalToLocal != nil {
		idx.globalToLocal.Close()
	}

	return lastErr
}

// GetLocation returns the physical storage location for a vector ID.
func (idx *ShardedHNSW) GetLocation(id uint32) (any, bool) {
	return idx.locationStore.Get(VectorID(id))
}

// GetVectorID returns the VectorID for a given physical location.
func (idx *ShardedHNSW) GetVectorID(loc any) (uint32, bool) {
	if l, ok := loc.(Location); ok {
		id, found := idx.locationStore.GetID(l)
		return uint32(id), found
	}
	return 0, false
}

// GetDimension returns the vector dimension for this index.
func (idx *ShardedHNSW) GetDimension() uint32 {
	return idx.dimension
}

// SetEfConstruction updates the efConstruction parameter dynamically for all shards.
func (idx *ShardedHNSW) SetEfConstruction(ef int) {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	for _, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			// Check for SetEfConstruction method (supported by HNSWIndex and ArrowHNSW)
			switch h := shard.index.(type) {
			case interface{ SetEfConstruction(int) }:
				h.SetEfConstruction(ef)
			case interface{ SetEfConstruction(int32) }:
				val := ef
				if val > math.MaxInt32 {
					val = math.MaxInt32
				}
				h.SetEfConstruction(int32(val))
			}
		}
	}
}

// TrainPQ is not supported for sharded indexes.
func (idx *ShardedHNSW) TrainPQ(vectors [][]float32) error {
	return nil
}

// GetPQEncoder is not supported for sharded indexes.
func (idx *ShardedHNSW) GetPQEncoder() *pq.PQEncoder {
	return nil
}

// PreWarm triggers memory pre-warming across all shards.
func (idx *ShardedHNSW) PreWarm(targetSize int) {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	if len(idx.shards) == 0 {
		return
	}
	shardTarget := targetSize / len(idx.shards)
	for _, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			shard.index.PreWarm(shardTarget)
		}
	}
}

// ShardStats returns multi-index statistics for all shards.
func (idx *ShardedHNSW) ShardStats() []ShardStat {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	stats := make([]ShardStat, len(idx.shards))
	for i, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			stats[i] = ShardStat{
				ShardID: i,
				Count:   shard.index.Size(),
			}
		}
	}
	return stats
}

// ShardStat holds statistics for a single shard.
type ShardStat struct {
	ShardID int
	Count   int
}

// EstimateMemory implements VectorIndex by summing estimated memory across all shards.
func (idx *ShardedHNSW) EstimateMemory() int64 {
	size := int64(64)
	size += int64(idx.locationStore.Len() * 8)

	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	for _, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			size += shard.index.EstimateMemory()
		}
	}

	return size
}

// RemapFromBatchInfo updates locations based on compaction remapping.
func (idx *ShardedHNSW) RemapFromBatchInfo(remapping map[int]BatchRemapInfo) error {
	// ShardedHNSW locationStore (ChunkedLocationStore) holds global locations.
	// We need to iterate all locations and update them.
	// This is potentially expensive but necessary for compaction.

	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	maxID := int(idx.nextID.Load())
	for id := 0; id < maxID; id++ {
		vid := VectorID(id)
		loc, ok := idx.locationStore.Get(vid)
		if !ok {
			continue
		}

		info, ok := remapping[loc.BatchIdx]
		if ok {
			// This batch was compacted
			if loc.RowIdx < len(info.NewRowIdxs) {
				newRowIdx := info.NewRowIdxs[loc.RowIdx]
				if newRowIdx != -1 {
					newLoc := Location{
						BatchIdx: info.NewBatchIdx,
						RowIdx:   newRowIdx,
					}
					// Update global location
					idx.locationStore.Set(vid, newLoc)

					// Update inside the shard if it's an ArrowHNSW to maintain internal consistency
					for _, shard := range idx.shards {
						if shard == nil {
							continue
						}
						if loc, found := idx.globalToLocal.Get(vid); found {
							lid := uint32(loc.BatchIdx) // #nosec G115
							if ah, ok := shard.index.(*ArrowHNSW); ok {
								ah.SetLocation(VectorID(lid), newLoc)
							}
							break
						}
					}
				}
			}
		}
	}
	return nil
}

// GetEntryPoint implements VectorIndex.
func (idx *ShardedHNSW) GetEntryPoint() uint32 {
	return 0
}

// CleanupTombstones removes deleted nodes from the graph (Vacuum) for all shards.
func (idx *ShardedHNSW) CleanupTombstones(threshold int) int {
	totalPruned := 0
	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		wg.Add(1)
		go func(sh *hnswShard) {
			defer wg.Done()
			// Check for CleanupTombstones method (supported by ArrowHNSW)
			if h, ok := sh.index.(interface{ CleanupTombstones(int) int }); ok {
				pruned := h.CleanupTombstones(threshold)
				mu.Lock()
				totalPruned += pruned
				mu.Unlock()
			}
		}(shard)
	}
	wg.Wait()
	return totalPruned
}

// ExportState implements VectorIndex by exporting the combined state of all shards.
func (idx *ShardedHNSW) ExportState() ([]byte, error) {
	var buf bytes.Buffer
	if err := idx.ExportGraph(&buf); err != nil {
		return nil, fmt.Errorf("failed to export graph: %w", err)
	}
	return buf.Bytes(), nil
}

// ImportState implements VectorIndex by importing the combined state.
func (idx *ShardedHNSW) ImportState(data []byte) error {
	return idx.ImportGraph(bytes.NewReader(data))
}

// ExportGraph exports the sharded graph to an io.Writer.
func (idx *ShardedHNSW) ExportGraph(w io.Writer) error {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	header := struct {
		Version   uint32
		NumShards int32
		Dimension uint32
	}{
		Version:   1,
		NumShards: int32(len(idx.shards)), // #nosec G115
		Dimension: idx.dimension,
	}

	if err := binary.Write(w, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	for i, shard := range idx.shards {
		if shard == nil || shard.index == nil {
			var zero uint32
			if err := binary.Write(w, binary.LittleEndian, zero); err != nil {
				return fmt.Errorf("failed to write shard %d header: %w", i, err)
			}
			continue
		}

		if shard.locationStore == nil {
			var zero uint32
			if err := binary.Write(w, binary.LittleEndian, zero); err != nil {
				return fmt.Errorf("failed to write shard %d mappings count (nil store): %w", i, err)
			}
			continue
		}
		mappingsCount := shard.locationStore.Len()
		if err := binary.Write(w, binary.LittleEndian, uint32(mappingsCount)); err != nil { // #nosec G115
			return fmt.Errorf("failed to write shard %d mappings count: %w", i, err)
		}

		for j := 0; j < mappingsCount; j++ {
			loc, _ := shard.locationStore.Get(VectorID(j))
			// #nosec G115
			globalID := uint64(loc.BatchIdx) // We store globalID in BatchIdx
			if err := binary.Write(w, binary.LittleEndian, globalID); err != nil {
				return fmt.Errorf("failed to write shard %d mapping: %w", i, err)
			}
		}

		if err := shard.index.ExportGraph(w); err != nil {
			return fmt.Errorf("failed to export shard %d graph: %w", i, err)
		}
	}

	return nil
}

// ImportGraph imports a sharded graph from an io.Reader.
func (idx *ShardedHNSW) ImportGraph(r io.Reader) error {
	var header struct {
		Version   uint32
		NumShards int32
		Dimension uint32
	}

	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if header.Version != 1 {
		return fmt.Errorf("unsupported export version: %d", header.Version)
	}

	if header.Dimension != idx.dimension {
		return fmt.Errorf("dimension mismatch: expected %d, got %d", idx.dimension, header.Dimension)
	}

	idx.shardsMu.Lock()
	defer idx.shardsMu.Unlock()

	if int(header.NumShards) > len(idx.shards) {
		newShards := make([]*hnswShard, header.NumShards)
		copy(newShards, idx.shards)
		for i := len(idx.shards); i < int(header.NumShards); i++ {
			newShards[i] = idx.newShard(i)
		}
		idx.shards = newShards
	}

	var maxGlobalID int64 = -1
	for i := 0; i < int(header.NumShards); i++ {
		shard := idx.shards[i]

		if shard == nil {
			continue
		}

		var mappingCount uint32
		if err := binary.Read(r, binary.LittleEndian, &mappingCount); err != nil {
			return fmt.Errorf("failed to read shard %d mappings count: %w", i, err)
		}

		if mappingCount == 0 {
			continue
		}

		globalIDs := make([]VectorID, mappingCount)
		for j := uint32(0); j < mappingCount; j++ {
			var globalID uint64
			if err := binary.Read(r, binary.LittleEndian, &globalID); err != nil {
				return fmt.Errorf("failed to read shard %d mapping %d: %w", i, j, err)
			}
			globalIDs[j] = VectorID(globalID)
			if int64(globalID) > maxGlobalID {
				maxGlobalID = int64(globalID)
			}
		}

		if shard.locationStore == nil {
			shard.locationStore = NewChunkedLocationStore()
		}
		shard.locationStore.Reset()
		shard.locationStore.EnsureCapacity(VectorID(mappingCount - 1))
		for j, globalID := range globalIDs {
			shard.registerID(uint32(j), globalID, idx.globalToLocal)
		}

		if shard.index != nil {
			if err := shard.index.ImportGraph(r); err != nil {
				return fmt.Errorf("failed to import shard %d graph: %w", i, err)
			}
		}
	}

	if maxGlobalID >= 0 {
		idx.nextID.Store(maxGlobalID + 1)
	}

	return nil
}

// ExportDelta implements VectorIndex.
func (idx *ShardedHNSW) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	allLocs := make([]core.Location, 0)
	startIndex := 0

	for _, shard := range idx.shards {
		if shard == nil {
			continue
		}
		if shard.locationStore == nil {
			continue
		}
		for j := 0; j < shard.locationStore.Len(); j++ {
			if gid, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(j)}); ok {
				if gLoc, ok := idx.locationStore.Get(gid); ok {
					allLocs = append(allLocs, gLoc)
				}
			}
		}
	}

	return &types.DeltaSync{
		FromVersion:  fromVersion,
		ToVersion:    uint64(len(allLocs)),
		NewLocations: allLocs,
		StartIndex:   startIndex,
	}, nil
}

// ApplyDelta implements VectorIndex.
func (idx *ShardedHNSW) ApplyDelta(delta *types.DeltaSync) error {
	if delta == nil || len(delta.NewLocations) == 0 {
		return nil
	}

	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	for i, loc := range delta.NewLocations {
		globalID := VectorID(int(delta.StartIndex) + i) // #nosec G115
		shardIdx := idx.sharder.GetShard(globalID)

		if shardIdx >= len(idx.shards) || idx.shards[shardIdx] == nil {
			continue
		}

		shard := idx.shards[shardIdx]
		localID := uint32(0)
		if shard.locationStore != nil {
			localID = uint32(shard.locationStore.Len()) // #nosec G115
		}
		shard.registerID(localID, globalID, idx.globalToLocal)

		idx.locationStore.Set(VectorID(globalID), loc)
	}

	return nil
}

// GetParallelSearchConfig implements VectorIndex.
func (idx *ShardedHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return idx.parallelConfig
}

// SetParallelSearchConfig updates the parallel search configuration and propagates it to all shards.
func (idx *ShardedHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	idx.parallelConfig = cfg
	// Propagate to existing shards
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	for _, shard := range idx.shards {
		if shard != nil && shard.index != nil {
			shard.index.SetParallelSearchConfig(cfg)
		}
	}
}

// RemapLocations implements VectorIndex.
func (idx *ShardedHNSW) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	for id, locAny := range mapping {
		vid := VectorID(id)
		if loc, ok := locAny.(core.Location); ok {
			idx.locationStore.Set(vid, loc)
		} else if loc, ok := locAny.(Location); ok {
			idx.locationStore.Set(vid, loc)
		}
	}

	// Propagate to shards if needed (though usually global location store is enough if shards use local IDs)
	return nil
}

// GetShardedIndex returns this index as a ShardedHNSW pointer.
func (idx *ShardedHNSW) GetShardedIndex() *ShardedHNSW {
	return idx
}

// RelocateToOffHeap relocates all shards of the sharded HNSW index to off-heap memory.
func (idx *ShardedHNSW) RelocateToOffHeap() error {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
	for _, shard := range idx.shards {
		if err := shard.RelocateToOffHeap(); err != nil {
			return err
		}
	}
	return nil
}

// ReleaseMonolithicChunk releases a monolithic chunk from memory (no-op for sharded indexes).
func (idx *ShardedHNSW) ReleaseMonolithicChunk(cID int) error {
	// ShardedHNSW doesn't have a single monolithic store to release.
	// Memory is managed at the shard level.
	return nil
}

func (s *hnswShard) RelocateToOffHeap() error {
	return s.index.RelocateToOffHeap()
}

func (idx *ShardedHNSW) updateShardBalanceMetrics() {
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	numShards := len(idx.shards)
	if numShards <= 1 {
		return
	}

	counts := make([]float64, numShards)
	sum := 0.0
	for i, s := range idx.shards {
		if s != nil && s.index != nil {
			cnt := float64(s.index.Len())
			counts[i] = cnt
			sum += cnt

			datasetName := ""
			if idx.dataset != nil {
				datasetName = idx.dataset.GetName()
			}
			metrics.HNSWNodeCount.WithLabelValues(datasetName, fmt.Sprintf("%d", i)).Set(cnt)
		}
	}

	mean := sum / float64(numShards)
	if mean <= 0 {
		return
	}

	varianceSum := 0.0
	for _, c := range counts {
		diff := c - mean
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(numShards)
	stdDev := math.Sqrt(variance)
	coefficientOfVariation := stdDev / mean

	datasetName := ""
	if idx.dataset != nil {
		datasetName = idx.dataset.GetName()
	}
	metrics.ShardBalanceImbalanceRatio.WithLabelValues(datasetName).Set(coefficientOfVariation)
}

// GetShardIndex returns the underlying index of a specific shard, mainly for graph API access
func (s *ShardedHNSW) GetShardIndex(shardIdx int) VectorIndex {
	if shardIdx >= 0 && shardIdx < len(s.shards) {
		return s.shards[shardIdx].index
	}
	return nil
}

// NumShards returns the number of shards
func (s *ShardedHNSW) NumShards() int {
	return len(s.shards)
}

// LocationStore returns the location store
func (s *ShardedHNSW) LocationStore() *ChunkedLocationStore {
	return s.locationStore
}

// Dataset returns the underlying index data provider
func (s *ShardedHNSW) Dataset() types.IndexDataProvider {
	return s.dataset
}

// SetDataset sets the dataset for testing
func (s *ShardedHNSW) SetDataset(dp types.IndexDataProvider) {
	s.dataset = dp
}

// GetConfig returns the sharded index configuration.
func (s *ShardedHNSW) GetConfig() ShardedHNSWConfig {
	return s.config
}
