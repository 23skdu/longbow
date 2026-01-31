package store

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"
)

// ShardedHNSWConfig configures the sharded HNSW index.
type ShardedHNSWConfig struct {
	NumShards      int            // Initial/Currently active shards
	M              int            // HNSW M parameter
	EfConstruction int            // HNSW efConstruction parameter
	Metric         DistanceMetric // Distance metric for this index
	Dimension      uint32         // Vector dimension
	// ShardSplitThreshold is deprecated in favor of Ring Sharding but kept for interface/legacy compatibility.
	// In Ring mode, it implies the *initial capacity* of each shard.
	ShardSplitThreshold    int
	UseRingSharding        bool // If true, use Consistent Hashing (Ring). If false, use Linear Range.
	PackedAdjacencyEnabled bool // If true, use thread-safe packed neighbor storage (v0.1.4)
}

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
	return nil
}

// DefaultShardedHNSWConfig returns sensible defaults.
func DefaultShardedHNSWConfig() ShardedHNSWConfig {
	return ShardedHNSWConfig{
		NumShards:              runtime.NumCPU(),
		M:                      32,
		EfConstruction:         400,
		Metric:                 MetricEuclidean,
		ShardSplitThreshold:    65536, // ~64k vectors per shard (L3 Cache Alignment)
		UseRingSharding:        true,  // Default to Ring
		PackedAdjacencyEnabled: true,
	}
}

// hnswShard represents a single HNSW graph shard backed by any VectorIndex.
type hnswShard struct {
	index         VectorIndex
	mu            sync.RWMutex
	globalToLocal map[VectorID]uint32
	localToGlobal []VectorID
}

func newHnswShard(idx VectorIndex) *hnswShard {
	return &hnswShard{
		index:         idx,
		localToGlobal: make([]VectorID, 0),
		globalToLocal: make(map[VectorID]uint32),
	}
}

// registerID records the mapping between a local shard ID and a global VectorID.
func (s *hnswShard) registerID(localID uint32, globalID VectorID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure localToGlobal slice is large enough
	if int(localID) >= len(s.localToGlobal) {
		newLen := int(localID) + 1
		if cap(s.localToGlobal) < newLen {
			newCap := cap(s.localToGlobal) * 2
			if newCap < newLen {
				newCap = newLen
			}
			newSlice := make([]VectorID, newLen, newCap)
			copy(newSlice, s.localToGlobal)
			s.localToGlobal = newSlice
		}
		s.localToGlobal = s.localToGlobal[:newLen]
	}

	s.localToGlobal[localID] = globalID
	s.globalToLocal[globalID] = localID
}

// getGlobalID returns the global ID for a local ID.
func (s *hnswShard) getGlobalID(localID uint32) (VectorID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if int(localID) >= len(s.localToGlobal) {
		return 0, false
	}
	return s.localToGlobal[localID], true
}

// getLocalID returns the local ID for a global ID.
func (s *hnswShard) getLocalID(globalID VectorID) (uint32, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lid, ok := s.globalToLocal[globalID]
	return lid, ok
}

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
	dataset *Dataset
	nextID  atomic.Int64

	// Location Storage (Lock-Free Read)
	locationStore *ChunkedLocationStore

	dimension uint32

	// Dynamic Sharding
	sharder  ShardingStrategy
	shardsMu sync.RWMutex

	parallelConfig types.ParallelSearchConfig
}

// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset *Dataset) *ShardedHNSW {
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
		dimension:      config.Dimension,
		sharder:        sharder,
		parallelConfig: types.DefaultParallelSearchConfig(),
	}

	s.shards = make([]*hnswShard, config.NumShards)
	for i := 0; i < config.NumShards; i++ {
		s.shards[i] = s.newShard(i)
	}

	return s
}

func (s *ShardedHNSW) newShard(_ int) *hnswShard {
	// Map ShardedHNSWConfig to ArrowHNSWConfig
	arrowConfig := DefaultArrowHNSWConfig()
	arrowConfig.M = s.config.M
	arrowConfig.MMax = s.config.M * 3
	arrowConfig.MMax0 = s.config.M * 2
	arrowConfig.EfConstruction = int32(s.config.EfConstruction)
	arrowConfig.InitialCapacity = s.config.ShardSplitThreshold // Capacity hint
	arrowConfig.Metric = s.config.Metric
	arrowConfig.PackedAdjacencyEnabled = s.config.PackedAdjacencyEnabled

	// We pass nil for ChunkedLocationStore because shards use local IDs and don't manage global locations
	// The ShardedHNSW manages the global location store.
	idx := NewArrowHNSW(s.dataset, &arrowConfig)

	// Correct dimension initialization
	idx.SetDimension(int(s.dimension))

	return newHnswShard(idx)
}

// GetShardForID returns the shard index for a given Global VectorID.
func (s *ShardedHNSW) GetShardForID(id VectorID) int {
	return s.sharder.GetShard(id)
}

// AddByLocation implements VectorIndex.
func (s *ShardedHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if s.dataset == nil {
		return 0, fmt.Errorf("no dataset")
	}
	s.dataset.dataMu.RLock()
	defer s.dataset.dataMu.RUnlock()

	return s.AddByLocationUnsafe(ctx, batchIdx, rowIdx)
}

// AddByLocationUnsafe adds a vector from the dataset without taking dataset.dataMu.
func (s *ShardedHNSW) AddByLocationUnsafe(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if batchIdx >= len(s.dataset.Records) {
		return 0, fmt.Errorf("invalid batch idx")
	}
	rec := s.dataset.Records[batchIdx]
	return s.AddByRecord(ctx, rec, rowIdx, batchIdx)
}

// AddSafe alias.
func (s *ShardedHNSW) AddSafe(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (VectorID, error) {
	id, err := s.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err != nil {
		return 0, err
	}
	return VectorID(id), nil
}

// AddBatch implements VectorIndex.
func (s *ShardedHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	// Delegating to simple loop for now to ensure correctness with sharding.
	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		if i%100 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		// Robust record resolution
		var rec arrow.RecordBatch
		bIdx := batchIdxs[i]
		switch {
		case bIdx < len(recs) && recs[bIdx] != nil:
			rec = recs[bIdx]
		case len(recs) == 1:
			rec = recs[0]
		case i < len(recs):
			rec = recs[i]
		default:
			return nil, fmt.Errorf("could not resolve record batch for row %d (batchIdx %d, recs len %d)", i, bIdx, len(recs))
		}

		id, err := s.AddByRecord(ctx, rec, rowIdxs[i], batchIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// DeleteBatch implements VectorIndex.
func (s *ShardedHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	// Group by shard
	shardIds := make(map[int][]uint32)
	for _, id := range ids {
		vid := VectorID(id)
		shardIdx := s.sharder.GetShard(vid)
		shardIds[shardIdx] = append(shardIds[shardIdx], id)
	}

	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()

	for shardIdx, distinctIDs := range shardIds {
		if shardIdx >= len(s.shards) || s.shards[shardIdx] == nil {
			continue
		}
		shard := s.shards[shardIdx]
		// Convert Global IDs to Local IDs
		var localIDs []uint32
		for _, gid := range distinctIDs {
			if lid, ok := shard.getLocalID(VectorID(gid)); ok {
				localIDs = append(localIDs, lid)
			}
		}
		if len(localIDs) > 0 {
			if err := shard.index.DeleteBatch(ctx, localIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

// AddByRecord implements VectorIndex.
func (s *ShardedHNSW) IsSharded() bool {
	return true
}

func (s *ShardedHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	// Allocate Global ID
	id := VectorID(s.nextID.Add(1) - 1)

	// Update global locations (Lock-Free)
	s.locationStore.EnsureCapacity(id)
	s.locationStore.Set(id, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	s.locationStore.UpdateSize(id)

	// Route to Shard
	shardIdx := s.sharder.GetShard(id)

	s.shardsMu.RLock()
	if shardIdx < len(s.shards) {
		shard := s.shards[shardIdx]
		s.shardsMu.RUnlock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id)
		metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
		return uint32(id), nil
	}
	s.shardsMu.RUnlock()

	// If we are here, we might need to grow.
	// Only linear sharding supports growth.
	if s.config.UseRingSharding {
		return 0, fmt.Errorf("shard index out of bounds (dynamic growth not supported in ring mode)")
	}

	// Dynamic Growth (Double-checked locking)
	s.shardsMu.Lock()
	if shardIdx < len(s.shards) {
		// Someone else created it
		shard := s.shards[shardIdx]
		s.shardsMu.Unlock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id)
		metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
		return uint32(id), nil
	}

	// Grow
	// We fill potential gaps if shardIdx skips
	for i := len(s.shards); i <= shardIdx; i++ {
		s.shards = append(s.shards, s.newShard(i))
	}
	shard := s.shards[shardIdx]
	s.shardsMu.Unlock()

	// Insert
	localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err != nil {
		return 0, fmt.Errorf("shard insert failed: %w", err)
	}
	shard.registerID(localID, id)

	metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
	return uint32(id), nil
}

// SearchVectors implements VectorIndex.
func (s *ShardedHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	searchOptions := SearchOptions{}
	if opt, ok := options.(SearchOptions); ok {
		searchOptions = opt
	}
	_ = searchOptions // Mark as used

	// 1. Optimization: Try bitmap-based filtering
	if len(filters) > 0 && s.dataset != nil {
		bitset, err := s.dataset.GenerateFilterBitset(filters)
		if err == nil && bitset != nil {
			defer bitset.Release()
			res, _ := s.SearchVectorsWithBitmap(ctx, queryVec, k, bitset.AsRoaring(), options)
			// Sharded SearchVectorsWithBitmap already handles Local->Global mapping
			// and global bitset filtering.
			return res, nil
		}
	}

	// 2. Parallel Search across all shards (Fallback path)
	type shardResult struct {
		results  []SearchResult
		shardIdx int
	}

	ch := make(chan shardResult, len(s.shards))
	g, ctx := errgroup.WithContext(ctx)

	s.shardsMu.RLock()
	currentShards := s.shards
	s.shardsMu.RUnlock()

	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			res, err := shard.index.SearchVectors(ctx, queryVec, k*2, filters, options) // Oversample
			if err != nil {
				return err
			}
			ch <- shardResult{results: res, shardIdx: i}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	close(ch)

	// 2. Merge Results
	merged := make([]SearchResult, 0, k*len(currentShards))

	for sr := range ch {
		shard := currentShards[sr.shardIdx]
		for _, r := range sr.results {
			// Convert LocalID to GlobalID
			globalID, ok := shard.getGlobalID(uint32(r.ID))
			if !ok {
				continue // Should not happen
			}
			r.ID = globalID

			// Re-check global filters if needed
			if len(filters) > 0 {
				_, ok := s.locationStore.Get(globalID)
				if !ok {
					continue
				}
				// Evaluate filters here if needed.
			}

			merged = append(merged, r)
		}
	}

	// Filter Block (Redundant if shards filtered, but kept for safety/fallback)
	if len(filters) > 0 && s.dataset != nil {
		s.dataset.dataMu.RLock()
		if len(s.dataset.Records) > 0 {
			evaluators := make(map[int]*query.FilterEvaluator)
			filtered := merged[:0]
			for _, r := range merged {
				loc, ok := s.locationStore.Get(r.ID)
				if !ok || loc.BatchIdx >= len(s.dataset.Records) {
					continue
				}

				ev, ok := evaluators[loc.BatchIdx]
				if !ok {
					var err error
					ev, err = query.NewFilterEvaluator(s.dataset.Records[loc.BatchIdx], filters)
					if err != nil {
						continue
					}
					evaluators[loc.BatchIdx] = ev
				}

				if ev.Matches(loc.RowIdx) {
					filtered = append(filtered, r)
				}
			}
			merged = filtered
		}
		s.dataset.dataMu.RUnlock()
	}

	// Sort and limit
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (s *ShardedHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	type shardResult struct {
		results  []SearchResult
		shardIdx int
	}
	ch := make(chan shardResult, len(s.shards))
	var wg sync.WaitGroup

	s.shardsMu.RLock()
	currentShards := s.shards
	s.shardsMu.RUnlock()

	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, sh *hnswShard) {
			defer wg.Done()
			// Pass nil filter to shard, filter globally
			res, err := sh.index.SearchVectorsWithBitmap(ctx, queryVec, k*2, nil, options)
			if err == nil {
				ch <- shardResult{results: res, shardIdx: idx}
			}
		}(i, shard)
	}
	wg.Wait()
	close(ch)

	merged := make([]SearchResult, 0, k*2)
	for sr := range ch {
		shard := currentShards[sr.shardIdx]
		for _, r := range sr.results {
			globalID, ok := shard.getGlobalID(uint32(r.ID))
			if !ok {
				continue
			}

			// Global Bitset Filter
			if filter != nil && !filter.Contains(uint32(globalID)) {
				continue
			}

			r.ID = globalID
			merged = append(merged, r)
		}
	}

	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged, nil
}

// Len implements VectorIndex.
func (s *ShardedHNSW) Len() int {
	return int(s.nextID.Load())
}

// Size implements VectorIndexer.
func (s *ShardedHNSW) Size() int {
	return s.Len()
}

func (s *ShardedHNSW) Search(ctx context.Context, queryVal any, k int, filter any) ([]types.Candidate, error) {
	return nil, fmt.Errorf("use SearchVectors for sharded search")
}

// SearchByID searches for vectors similar to the vector at the given ID.
func (s *ShardedHNSW) SearchByID(ctx context.Context, id VectorID, k int) []VectorID {
	if k <= 0 {
		return nil
	}

	loc, ok := s.locationStore.Get(id)
	if !ok {
		return nil
	}

	// Retrieve vector from dataset
	s.dataset.dataMu.RLock()
	if loc.BatchIdx >= len(s.dataset.Records) {
		s.dataset.dataMu.RUnlock()
		return nil
	}
	rec := s.dataset.Records[loc.BatchIdx]
	s.dataset.dataMu.RUnlock()

	// Find vector column
	colIdx := -1
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		colIdx = 0
	}

	vec, err := ExtractVectorFromArrow(rec, loc.RowIdx, colIdx)
	if err != nil {
		return nil
	}

	// Perform global search
	results, err := s.SearchVectors(ctx, vec, k, nil, SearchOptions{})
	if err != nil {
		return nil
	}

	ids := make([]VectorID, len(results))
	for i, r := range results {
		ids[i] = VectorID(r.ID)
	}
	return ids
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

// SetIndexedColumns satisfies VectorIndex interface
func (s *ShardedHNSW) SetIndexedColumns(cols []string) {
}

// Close implements VectorIndex.
func (s *ShardedHNSW) Close() error {
	s.shardsMu.Lock()
	defer s.shardsMu.Unlock()
	var lastErr error
	for _, shard := range s.shards {
		if shard != nil && shard.index != nil {
			if err := shard.index.Close(); err != nil {
				lastErr = err
			}
		}
	}
	s.shards = nil
	return lastErr
}

// GetLocation returns the storage location for a given VectorID
func (s *ShardedHNSW) GetLocation(id uint32) (any, bool) {
	return s.locationStore.Get(VectorID(id))
}

// GetVectorID returns the VectorID for a given storage location
func (s *ShardedHNSW) GetVectorID(loc any) (uint32, bool) {
	if l, ok := loc.(Location); ok {
		id, found := s.locationStore.GetID(l)
		return uint32(id), found
	}
	return 0, false
}

// GetDimension implements VectorIndex.
func (s *ShardedHNSW) GetDimension() uint32 {
	return s.dimension
}

// SetEfConstruction updates the efConstruction parameter dynamically for all shards.
func (s *ShardedHNSW) SetEfConstruction(ef int) {
	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()
	for _, shard := range s.shards {
		if shard != nil && shard.index != nil {
			// Check for SetEfConstruction method (supported by HNSWIndex and ArrowHNSW)
			// Check for SetEfConstruction method (supported by HNSWIndex and ArrowHNSW)
			switch h := shard.index.(type) {
			case interface{ SetEfConstruction(int) }:
				h.SetEfConstruction(ef)
			case interface{ SetEfConstruction(int32) }:
				h.SetEfConstruction(int32(ef))
			}
		}
	}
}

func (s *ShardedHNSW) TrainPQ(vectors [][]float32) error {
	return nil
}

func (s *ShardedHNSW) GetPQEncoder() *pq.PQEncoder {
	return nil
}

func (s *ShardedHNSW) PreWarm(targetSize int) {
	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()
	for _, shard := range s.shards {
		if shard != nil && shard.index != nil {
			shard.index.PreWarm(targetSize / len(s.shards))
		}
	}
}

// GetNeighbors returns the nearest neighbors for a given vector ID.
func (s *ShardedHNSW) GetNeighbors(id uint32) ([]uint32, error) {
	shardIdx := s.GetShardForID(VectorID(id))

	s.shardsMu.RLock()
	if shardIdx >= len(s.shards) || s.shards[shardIdx] == nil {
		s.shardsMu.RUnlock()
		return nil, fmt.Errorf("invalid shard index")
	}
	shard := s.shards[shardIdx]
	s.shardsMu.RUnlock()

	localID, ok := shard.getLocalID(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector id not found in shard")
	}

	// Get local neighbors
	localNeighbors, err := shard.index.GetNeighbors(localID)
	if err != nil {
		return nil, err
	}

	// Map to Global IDs
	globalNeighbors := make([]uint32, 0, len(localNeighbors))
	for _, ln := range localNeighbors {
		globalID, ok := shard.getGlobalID(ln)
		if !ok {
			continue
		}
		globalNeighbors = append(globalNeighbors, uint32(globalID))
	}
	return globalNeighbors, nil
}

// Stats returns multi-index statistics for all shards.
func (s *ShardedHNSW) ShardStats() []ShardStat {
	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()
	stats := make([]ShardStat, len(s.shards))
	for i, shard := range s.shards {
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

// EstimateMemory implements VectorIndex.
func (s *ShardedHNSW) EstimateMemory() int64 {
	size := int64(64)
	size += int64(s.locationStore.Len() * 8)

	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()
	for _, shard := range s.shards {
		if shard != nil && shard.index != nil {
			size += shard.index.EstimateMemory()
		}
	}

	return size
}

// RemapFromBatchInfo updates locations based on compaction remapping.
func (s *ShardedHNSW) RemapFromBatchInfo(remapping map[int]BatchRemapInfo) error {
	// ShardedHNSW locationStore (ChunkedLocationStore) holds global locations.
	// We need to iterate all locations and update them.
	// This is potentially expensive but necessary for compaction.

	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()

	maxID := int(s.nextID.Load())
	for id := 0; id < maxID; id++ {
		vid := VectorID(id)
		loc, ok := s.locationStore.Get(vid)
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
					s.locationStore.Set(vid, newLoc)

					// Shards currently handle their own internal consistency.
					// If they are ArrowHNSW, they might need location updates,
					// but since they don't have SetLocation in the interface,
					// we skip it for generic support.
				}
			}
		}
	}
	return nil
}

// GetEntryPoint implements VectorIndex.
func (s *ShardedHNSW) GetEntryPoint() uint32 {
	return 0
}

// CleanupTombstones removes deleted nodes from the graph (Vacuum).
func (s *ShardedHNSW) CleanupTombstones(threshold int) int {
	totalPruned := 0
	s.shardsMu.RLock()
	currentShards := s.shards
	s.shardsMu.RUnlock()

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

// ExportState implements VectorIndex.
func (s *ShardedHNSW) ExportState() ([]byte, error) {
	return nil, fmt.Errorf("ExportState not yet implemented for ShardedHNSW")
}

// ImportState implements VectorIndex.
func (s *ShardedHNSW) ImportState(data []byte) error {
	return fmt.Errorf("ImportState not yet implemented for ShardedHNSW")
}

// ExportGraph implements VectorIndex.
func (s *ShardedHNSW) ExportGraph(w io.Writer) error {
	return fmt.Errorf("ExportGraph not yet implemented for ShardedHNSW")
}

// ImportGraph implements VectorIndex.
func (s *ShardedHNSW) ImportGraph(r io.Reader) error {
	return fmt.Errorf("ImportGraph not yet implemented for ShardedHNSW")
}

// ExportDelta implements VectorIndex.
func (s *ShardedHNSW) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	return nil, fmt.Errorf("ExportDelta not yet implemented for ShardedHNSW")
}

// ApplyDelta implements VectorIndex.
func (s *ShardedHNSW) ApplyDelta(delta *types.DeltaSync) error {
	return fmt.Errorf("ApplyDelta not yet implemented for ShardedHNSW")
}

// GetParallelSearchConfig implements VectorIndex.
func (s *ShardedHNSW) GetParallelSearchConfig() types.ParallelSearchConfig {
	return s.parallelConfig
}

// SetParallelSearchConfig updates the parallel search configuration
func (s *ShardedHNSW) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	s.parallelConfig = cfg
	// Propagate to existing shards
	s.shardsMu.RLock()
	defer s.shardsMu.RUnlock()
	for _, shard := range s.shards {
		if shard != nil && shard.index != nil {
			shard.index.SetParallelSearchConfig(cfg)
		}
	}
}
