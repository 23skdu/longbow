package store

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/sync/errgroup"
)

// ShardedHNSWConfig configures the sharded HNSW index.
type ShardedHNSWConfig struct {
	NumShards           int          // Initial/Currently active shards
	M                   int          // HNSW M parameter
	EfConstruction      int          // HNSW efConstruction parameter
	Metric              VectorMetric // Distance metric for this index
	Dimension           uint32       // Vector dimension
	ShardSplitThreshold int          // Number of vectors per shard before splitting
}

func (c ShardedHNSWConfig) Validate() error {
	if c.NumShards <= 0 {
		return fmt.Errorf("numShards must be > 0")
	}
	if c.M <= 0 {
		return fmt.Errorf("M must be > 0")
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
	}
}

// hnswShard represents a single HNSW graph shard backed by ArrowHNSW.
type hnswShard struct {
	index *ArrowHNSW
}

// Warmup accesses all nodes in the shard.
func (s *hnswShard) Warmup() int {
	if s.index == nil {
		return 0
	}
	return s.index.Warmup()
}

// ShardedHNSW provides fine-grained locking via multiple independent HNSW shards.
// It uses Lock-Free Sharding by mapping independent global ID ranges to shards.
type ShardedHNSW struct {
	config  ShardedHNSWConfig
	shards  []*hnswShard
	dataset *Dataset
	nextID  atomic.Int64

	// Location Storage (Lock-Free Read)
	locationStore *ChunkedLocationStore

	dimension uint32

	// Dynamic Sharding
	activeShards atomic.Int32
	shardsMu     sync.RWMutex
}

// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset *Dataset) *ShardedHNSW {
	if config.NumShards <= 0 {
		config.NumShards = 1 // Start with at least 1 shard
	}
	if config.ShardSplitThreshold <= 0 {
		config.ShardSplitThreshold = 65536
	}

	s := &ShardedHNSW{
		config:        config,
		dataset:       dataset,
		locationStore: NewChunkedLocationStore(),
		dimension:     config.Dimension,
	}
	s.activeShards.Store(int32(config.NumShards))

	s.shards = make([]*hnswShard, config.NumShards)
	for i := 0; i < config.NumShards; i++ {
		s.shards[i] = s.newShard()
	}

	return s
}

func (s *ShardedHNSW) newShard() *hnswShard {
	// Map ShardedHNSWConfig to ArrowHNSWConfig
	arrowConfig := DefaultArrowHNSWConfig()
	arrowConfig.M = s.config.M
	arrowConfig.MMax = s.config.M * 3
	arrowConfig.MMax0 = s.config.M * 2
	arrowConfig.EfConstruction = s.config.EfConstruction
	arrowConfig.InitialCapacity = s.config.ShardSplitThreshold
	// Metric support TODO: Pass metric to ArrowHNSW once supported

	// We pass nil for ChunkedLocationStore because shards use local IDs and don't manage global locations
	// The ShardedHNSW manages the global location store.
	idx := NewArrowHNSW(s.dataset, arrowConfig, nil)

	// Manually set dims if not yet inferred (safe backup)
	if s.dimension > 0 && idx.dims == 0 {
		idx.dims = int(s.dimension)
	}

	return &hnswShard{
		index: idx,
	}
}

// GetShardForID returns the shard index for a given Global VectorID.
func (s *ShardedHNSW) GetShardForID(id VectorID) int {
	return int(uint64(id) / uint64(s.config.ShardSplitThreshold))
}

// GetLocalID converts a Global VectorID to a Shard-Local ID.
func (s *ShardedHNSW) GetLocalID(id VectorID) uint32 {
	return uint32(uint64(id) % uint64(s.config.ShardSplitThreshold))
}

// GetGlobalID converts a Shard-Local ID to a Global VectorID.
func (s *ShardedHNSW) GetGlobalID(shardIdx int, localID uint32) VectorID {
	return VectorID(uint64(shardIdx)*uint64(s.config.ShardSplitThreshold) + uint64(localID))
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
func (s *ShardedHNSW) AddBatch(recs []arrow.RecordBatch, rowIdxs []int, batchIdxs []int) ([]uint32, error) {
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
	shardIdx := s.GetShardForID(id)
	localID := s.GetLocalID(id)

	s.shardsMu.Lock()
	// Dynamically grow shards if needed
	for len(s.shards) <= shardIdx {
		s.shards = append(s.shards, s.newShard())
		s.activeShards.Add(1)
		metrics.ShardedHnswShardSplitCount.WithLabelValues(s.dataset.Name).Inc()
	}
	shard := s.shards[shardIdx]
	s.shardsMu.Unlock()

	// Insert into Shard
	level := shard.index.generateLevel()

	// Note: localID is used within the shard. ID mapping is handled by ShardedHNSW.
	err := shard.index.InsertWithVector(uint32(localID), vec, level)
	if err != nil {
		return 0, fmt.Errorf("shard insert failed: %w", err)
	}

	metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
	return uint32(id), nil
}

// SearchVectors implements VectorIndex.
func (s *ShardedHNSW) SearchVectors(query []float32, k int, filters []Filter) ([]SearchResult, error) {
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
			res, err := shard.index.SearchVectors(query, k*2, filters) // Oversample
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
		for _, r := range sr.results {
			// Convert LocalID to GlobalID
			globalID := s.GetGlobalID(sr.shardIdx, uint32(r.ID))
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
			evaluator, err := NewFilterEvaluator(s.dataset.Records[0], filters)
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
func (s *ShardedHNSW) SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {

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
			res := sh.index.SearchVectorsWithBitmap(query, k*2, nil)
			ch <- shardResult{results: res, shardIdx: idx}
		}(i, shard)
	}
	wg.Wait()
	close(ch)

	merged := make([]SearchResult, 0, k*2)
	for sr := range ch {
		for _, r := range sr.results {
			globalID := s.GetGlobalID(sr.shardIdx, uint32(r.ID))

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

	// Shard resolution not strictly needed if using global location store
	// but kept if we need to ensure shard existence or locking logic.
	// Actually, we don't strictly need shard lock to read from dataset.

	// Use ArrowHNSW's vector retrieval
	// Use global vector retrieval
	// shard.index.getVector uses localID which won't map correctly to the shared locationStore
	// if keys are GlobalIDs.
	// Instead, we resolve using GlobalID directly.

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
		// Fallback to 0 if not found, though realistically this shouldn't happen in valid dataset
		colIdx = 0
	}

	vec, err := ExtractVectorFromArrow(rec, loc.RowIdx, colIdx)
	if err != nil {
		return nil
	}

	// Perform global search
	results, err := s.SearchVectors(vec, k, nil)
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

// GetNeighbors returns the nearest neighbors for a given vector ID.
func (s *ShardedHNSW) GetNeighbors(id VectorID) ([]VectorID, error) {
	shardIdx := s.GetShardForID(id)
	localID := s.GetLocalID(id)

	s.shardsMu.RLock()
	if shardIdx >= len(s.shards) || s.shards[shardIdx] == nil {
		s.shardsMu.RUnlock()
		return nil, fmt.Errorf("invalid shard index")
	}
	shard := s.shards[shardIdx]
	s.shardsMu.RUnlock()

	// Get local neighbors
	localNeighbors, err := shard.index.GetNeighbors(VectorID(localID))
	if err != nil {
		return nil, err
	}

	// Map to Global IDs
	globalNeighbors := make([]VectorID, len(localNeighbors))
	for i, ln := range localNeighbors {
		globalNeighbors[i] = s.GetGlobalID(shardIdx, uint32(ln))
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
