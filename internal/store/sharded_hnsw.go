package store

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	ShardSplitThreshold int
	UseRingSharding     bool // If true, use Consistent Hashing (Ring). If false, use Linear Range.
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
		NumShards:           runtime.NumCPU(),
		M:                   32,
		EfConstruction:      400,
		Metric:              MetricEuclidean,
		ShardSplitThreshold: 65536, // ~64k vectors per shard (L3 Cache Alignment)
		UseRingSharding:     true,  // Default to Ring
	}
}

// hnswShard represents a single HNSW graph shard backed by ArrowHNSW.
type hnswShard struct {
	index         *ArrowHNSW
	mu            sync.RWMutex
	globalToLocal map[VectorID]uint32
	localToGlobal []VectorID
}

func newHnswShard(index *ArrowHNSW) *hnswShard {
	return &hnswShard{
		index:         index,
		globalToLocal: make(map[VectorID]uint32),
		localToGlobal: make([]VectorID, 0),
	}
}

// mapID registers a global ID and assigns/returns a local ID.
func (s *hnswShard) mapID(globalID VectorID) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	localID := uint32(len(s.localToGlobal))
	s.localToGlobal = append(s.localToGlobal, globalID)
	s.globalToLocal[globalID] = localID
	return localID
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
		config:        config,
		dataset:       dataset,
		locationStore: NewChunkedLocationStore(),
		dimension:     config.Dimension,
		sharder:       sharder,
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
	arrowConfig.EfConstruction = s.config.EfConstruction
	arrowConfig.InitialCapacity = s.config.ShardSplitThreshold // Capacity hint
	arrowConfig.Metric = s.config.Metric

	// We pass nil for ChunkedLocationStore because shards use local IDs and don't manage global locations
	// The ShardedHNSW manages the global location store.
	idx := NewArrowHNSW(s.dataset, arrowConfig, nil)

	// Manually set dims if not yet inferred (safe backup)
	dims := int(s.dimension)
	if dims > 0 && idx.dims.Load() == 0 {
		idx.dims.Store(int32(dims))
	}

	return newHnswShard(idx)
}

// GetShardForID returns the shard index for a given Global VectorID.
func (s *ShardedHNSW) GetShardForID(id VectorID) int {
	return s.sharder.GetShard(id)
}

// AddByLocation implements VectorIndex.
func (s *ShardedHNSW) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	if s.dataset == nil {
		return 0, fmt.Errorf("no dataset")
	}
	s.dataset.dataMu.RLock()
	defer s.dataset.dataMu.RUnlock()

	return s.AddByLocationUnsafe(batchIdx, rowIdx)
}

// AddByLocationUnsafe adds a vector from the dataset without taking dataset.dataMu.
func (s *ShardedHNSW) AddByLocationUnsafe(batchIdx, rowIdx int) (uint32, error) {
	if batchIdx >= len(s.dataset.Records) {
		return 0, fmt.Errorf("invalid batch idx")
	}
	rec := s.dataset.Records[batchIdx]
	return s.AddByRecord(rec, rowIdx, batchIdx)
}

// AddSafe alias.
func (s *ShardedHNSW) AddSafe(rec arrow.RecordBatch, rowIdx, batchIdx int) (VectorID, error) {
	id, err := s.AddByRecord(rec, rowIdx, batchIdx)
	if err != nil {
		return 0, err
	}
	return VectorID(id), nil
}

// AddBatch implements VectorIndex.
func (s *ShardedHNSW) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	// Delegating to simple loop for now to ensure correctness with sharding.
	ids := make([]uint32, len(recs))
	for i := range recs {
		id, err := s.AddByRecord(recs[i], rowIdxs[i], batchIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

// AddByRecord implements VectorIndex.
func (s *ShardedHNSW) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	// Extract vector
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}
	if vecCol == nil {
		return 0, fmt.Errorf("vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return 0, fmt.Errorf("invalid vector column format")
	}

	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	end := start + width
	if start < 0 || end > floatArr.Len() {
		return 0, fmt.Errorf("row index out of bounds")
	}

	vec := floatArr.Float32Values()[start:end]

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
		// Common path: existing shard
		// Map Global ID to Local ID in Shard
		localID := shard.mapID(id)
		// Store location in shard index to support filtering
		shard.index.SetLocation(VectorID(localID), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

		level := shard.index.generateLevel()
		err := shard.index.InsertWithVector(localID, vec, level)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
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
		localID := shard.mapID(id)
		// Store location in shard index to support filtering
		shard.index.SetLocation(VectorID(localID), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

		level := shard.index.generateLevel()
		err := shard.index.InsertWithVector(localID, vec, level)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
		return uint32(id), nil
	}

	// Grow
	// We fill potential gaps if shardIdx skips (e.g. if batch added many IDs at once)
	for i := len(s.shards); i <= shardIdx; i++ {
		s.shards = append(s.shards, s.newShard(i))
	}
	shard := s.shards[shardIdx]
	s.shardsMu.Unlock()

	// Insert
	localID := shard.mapID(id)
	// Store location in shard index to support filtering
	shard.index.SetLocation(VectorID(localID), Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	level := shard.index.generateLevel()
	err := shard.index.InsertWithVector(localID, vec, level)
	if err != nil {
		return 0, fmt.Errorf("shard insert failed: %w", err)
	}

	metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
	return uint32(id), nil
}

// SearchVectors implements VectorIndex.
func (s *ShardedHNSW) SearchVectors(queryVec []float32, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}

	// 1. Search all shards in parallel
	type shardResult struct {
		results  []SearchResult
		shardIdx int
	}

	ch := make(chan shardResult, len(s.shards))
	g, _ := errgroup.WithContext(context.TODO())

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
			res, err := shard.index.SearchVectors(queryVec, k*2, filters, options) // Oversample
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

	// Filter Block
	if len(filters) > 0 && s.dataset != nil {
		s.dataset.dataMu.RLock()
		if len(s.dataset.Records) > 0 {
			evaluator, err := query.NewFilterEvaluator(s.dataset.Records[0], filters)
			if err == nil {
				filtered := merged[:0]
				for _, r := range merged {
					loc, ok := s.locationStore.Get(r.ID)
					if ok && loc.BatchIdx < len(s.dataset.Records) {
						if evaluator.Matches(loc.RowIdx) {
							filtered = append(filtered, r)
						}
					}
				}
				merged = filtered
			}
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
func (s *ShardedHNSW) SearchVectorsWithBitmap(queryVec []float32, k int, filter *query.Bitset, options SearchOptions) []SearchResult {

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
			res := sh.index.SearchVectorsWithBitmap(queryVec, k*2, nil, options)
			ch <- shardResult{results: res, shardIdx: idx}
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
			if filter != nil && !filter.Contains(int(globalID)) {
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

	return merged
}

// Len implements VectorIndex.
func (s *ShardedHNSW) Len() int {
	return int(s.nextID.Load())
}

// SearchByID searches for vectors similar to the vector at the given ID.
func (s *ShardedHNSW) SearchByID(id VectorID, k int) []VectorID {
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
	results, err := s.SearchVectors(vec, k, nil, SearchOptions{})
	if err != nil {
		return nil
	}

	ids := make([]VectorID, len(results))
	for i, r := range results {
		ids[i] = r.ID
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
	return nil
}

// GetLocation returns the storage location for a given VectorID
func (s *ShardedHNSW) GetLocation(id VectorID) (Location, bool) {
	return s.locationStore.Get(id)
}

// GetDimension implements VectorIndex.
func (s *ShardedHNSW) GetDimension() uint32 {
	return s.dimension
}

func (s *ShardedHNSW) TrainPQ(vectors [][]float32) error {
	// For sharded HNSW, training PQ should ideally happen on the whole dataset
	// and then distributed to shards or managed at the top level.
	// For now, let's assume it's a stub or managed by Dataset.
	return nil
}

func (s *ShardedHNSW) GetPQEncoder() *pq.PQEncoder {
	return nil
}

// GetNeighbors returns the nearest neighbors for a given vector ID.
func (s *ShardedHNSW) GetNeighbors(id VectorID) ([]VectorID, error) {
	shardIdx := s.GetShardForID(id)

	s.shardsMu.RLock()
	if shardIdx >= len(s.shards) || s.shards[shardIdx] == nil {
		s.shardsMu.RUnlock()
		return nil, fmt.Errorf("invalid shard index")
	}
	shard := s.shards[shardIdx]
	s.shardsMu.RUnlock()

	localID, ok := shard.getLocalID(id)
	if !ok {
		return nil, fmt.Errorf("vector id not found in shard")
	}

	// Get local neighbors
	localNeighbors, err := shard.index.GetNeighbors(VectorID(localID))
	if err != nil {
		return nil, err
	}

	// Map to Global IDs
	globalNeighbors := make([]VectorID, len(localNeighbors))
	for i, ln := range localNeighbors {
		globalID, ok := shard.getGlobalID(uint32(ln))
		if !ok {
			continue // Should not happen
		}
		globalNeighbors[i] = globalID
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

					// Update shard location
					shardIdx := s.sharder.GetShard(vid)
					if shardIdx < len(s.shards) {
						shard := s.shards[shardIdx]
						if shard != nil {
							if localID, ok := shard.getLocalID(vid); ok {
								shard.index.SetLocation(VectorID(localID), newLoc)
							}
						}
					}
				}
			}
		}
	}
	return nil
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
			// Forward to underlying ArrowHNSW
			pruned := sh.index.CleanupTombstones(threshold)
			mu.Lock()
			totalPruned += pruned
			mu.Unlock()
		}(shard)
	}
	wg.Wait()
	return totalPruned
}
