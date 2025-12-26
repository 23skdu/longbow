package hnsw2

import (
	"context"
	"fmt"
	"sync"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"
)

// ShardedArrowIndex implements a sharded HNSW index using ArrowHNSW shards.
// It maps Global VectorIDs to Shard-Local IDs to ensure dense packing in each shard.
type ShardedArrowIndex struct {
	config    ShardedConfig
	dataset   *store.Dataset
	
	// Global location store (shared across all shards)
	locationStore *store.ChunkedLocationStore
	
	shards []*ArrowSham // Wrapper around ArrowHNSW to handle ID mapping
	
	// Routing strategy: Modulo
	// shardIdx = GlobalID % NumShards

	aggregator *ShardedResultAggregator
}

// ShardedConfig holds configuration for the sharded index.
type ShardedConfig struct {
	NumShards int
	Graph     Config // HNSW config for each shard
}

// ArrowSham (Shard + HNSW) wraps an ArrowHNSW with ID mapping.
// It manages the conversion between LocalID (used by HNSW) and GlobalID (used by App).
type ArrowSham struct {
	index *ArrowHNSW
	
	// ID Mapping: Local -> Global
	// Index is LocalID, Value is GlobalID
	localToGlobal []uint32
	mu            sync.RWMutex
}

// NewShardedArrowIndex creates a new sharded index.
func NewShardedArrowIndex(dataset *store.Dataset, config *ShardedConfig) *ShardedArrowIndex {
	if config.NumShards <= 0 {
		config.NumShards = 1
	}
	
	si := &ShardedArrowIndex{
		config:        *config,
		dataset:       dataset,
		locationStore: store.NewChunkedLocationStore(),
		shards:        make([]*ArrowSham, config.NumShards),
		aggregator:    NewShardedResultAggregator(),
	}
	
	for i := 0; i < config.NumShards; i++ {
		// Each shard gets a pointer to the SAME dataset
		// But handles its own "Local" IDs starting from 0
		hnsw := NewArrowHNSW(dataset, config.Graph)
		// We need to bypass the internal locationStore of ArrowHNSW since we manage locations globally
		// Actually, ArrowHNSW.AddByLocation writes to its own locationStore. 
		// We'll need to use lower-level Insert API on ArrowHNSW.
		
		si.shards[i] = &ArrowSham{
			index:         hnsw,
			localToGlobal: make([]uint32, 0, 1024),
		}
	}
	
	return si
}

// AddByLocation implements vector_index.go
func (s *ShardedArrowIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	// 1. Assign Global ID
	globalID := s.locationStore.Append(store.Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	gid := uint32(globalID)
	
	// 2. Route to Shard
	shardIdx := gid % uint32(s.config.NumShards)
	shard := s.shards[shardIdx]
	
	// 3. Add to Shard (Thread-safe)
	if err := shard.Add(gid, batchIdx, rowIdx); err != nil {
		return 0, err
	}
	
	return gid, nil
}

// AddBatch implements vector_index.go
func (s *ShardedArrowIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	n := len(rowIdxs)
	ids := make([]uint32, n)
	
	// 1. Assign Global IDs sequentially to maintain order
	for i := 0; i < n; i++ {
		globalID := s.locationStore.Append(store.Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		ids[i] = uint32(globalID)
	}

	// 2. Fan out for indexing
	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			gid := ids[i]
			shardIdx := gid % uint32(s.config.NumShards)
			shard := s.shards[shardIdx]
			return shard.Add(gid, batchIdxs[i], rowIdxs[i])
		})
	}
	
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return ids, nil
}

// AddByRecord adds a single record to the index.
func (s *ShardedArrowIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return s.AddByLocation(batchIdx, rowIdx)
}

// SearchVectors searches the index for the K nearest neighbors of the query vector.
func (s *ShardedArrowIndex) SearchVectors(query []float32, k int, filters []store.Filter) ([]store.SearchResult, error) {
	// Scatter-Gather using errgroup
	g, _ := errgroup.WithContext(context.Background())
	results := make([][]store.SearchResult, len(s.shards))
	
	for i, shard := range s.shards {
		i, shard := i, shard
		g.Go(func() error {
			// We search for k candidates in EACH shard
			// This improves recall probability
			res, err := shard.Search(query, k, filters)
			if err != nil {
				return err
			}
			results[i] = res
			return nil
		})
	}
	
	if err := g.Wait(); err != nil {
		return nil, err
	}
	
	// Merge
	return s.aggregator.MergeKWay(results, k), nil
}


// GetNeighbors returns the nearest neighbors for a given global vector ID.
func (s *ShardedArrowIndex) GetNeighbors(id store.VectorID) ([]store.VectorID, error) {
	numShards := uint32(s.config.NumShards)
	shardIdx := uint32(id) % numShards
	localID := uint32(id) / numShards

	if int(shardIdx) >= len(s.shards) {
		return nil, fmt.Errorf("invalid shard index %d for vector ID %d", shardIdx, id)
	}

	shard := s.shards[shardIdx]
	return shard.GetNeighbors(localID)
}

// ---- ArrowSham methods ----

func (s *ArrowSham) Add(globalID uint32, batchIdx, rowIdx int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Use public API which handles locationStore and Insert internally
	localID, err := s.index.AddByLocation(batchIdx, rowIdx)
	if err != nil {
		return err
	}
	
	lID := uint32(len(s.localToGlobal))
	s.localToGlobal = append(s.localToGlobal, globalID)
	
	// Sanity check: IDs must match sequential assumption
	if localID != lID {
		return fmt.Errorf("local ID mismatch in shard: expected %d, got %d", lID, localID)
	}

	return nil
}

func (s *ArrowSham) Search(query []float32, k int, filters []store.Filter) ([]store.SearchResult, error) {
	// Search returns LocalIDs
	// We assume filters are not used/supported yet or need translation
	// ArrowHNSW currently ignores filters in Search() core but signature matches
	
	// Note: ArrowHNSW.Search returns `store.SearchResult` which has `ID` field.
	res, err := s.index.SearchVectors(query, k, filters)
	if err != nil {
		return nil, err
	}
	
	// Map LocalID -> GlobalID
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for i := range res {
		localID := res[i].ID
		if int(localID) < len(s.localToGlobal) {
			res[i].ID = store.VectorID(s.localToGlobal[localID])
		}
	}
	return res, nil
}

func (s *ArrowSham) GetNeighbors(localID uint32) ([]store.VectorID, error) {
	// 1. Get neighbors from shard index (returns LocalIDs)
	localNeighbors, err := s.index.GetNeighbors(store.VectorID(localID))
	if err != nil {
		return nil, err
	}

	// 2. Map LocalIDs back to GlobalIDs
	s.mu.RLock()
	defer s.mu.RUnlock()

	globalNeighbors := make([]store.VectorID, len(localNeighbors))
	for i, lid := range localNeighbors {
		if int(lid) < len(s.localToGlobal) {
			globalNeighbors[i] = store.VectorID(s.localToGlobal[lid])
		} else {
			// Fallback: This shouldn't really happen in a well-formed index
			globalNeighbors[i] = store.VectorID(lid)
		}
	}
	return globalNeighbors, nil
}

// Implement interface methods...

func (s *ShardedArrowIndex) Len() int {
	return s.locationStore.Len() // Total vectors
}

func (s *ShardedArrowIndex) GetDimension() uint32 {
	if len(s.shards) > 0 {
		return s.shards[0].index.GetDimension()
	}
	return 0
}

func (s *ShardedArrowIndex) Warmup() int {
	total := 0
	for _, sh := range s.shards {
		total += sh.index.Warmup()
	}
	return total
}

func (s *ShardedArrowIndex) SetIndexedColumns(cols []string) {
	// No-op
}

func (s *ShardedArrowIndex) EstimateMemory() int64 {
	// Sum of shards + global overhead
	total := int64(0)
	for _, sh := range s.shards {
		total += sh.index.EstimateMemory()
		sh.mu.RLock()
		total += int64(len(sh.localToGlobal) * 4) // mapping map
		sh.mu.RUnlock()
	}
	return total
}

func (s *ShardedArrowIndex) Close() error {
	// Close all shards
	for _, sh := range s.shards {
		_ = sh.index.Close()
	}
	return nil
}
func (s *ShardedArrowIndex) SearchVectorsWithBitmap(query []float32, k int, filter *store.Bitset) []store.SearchResult {
	if k <= 0 {
		return nil
	}
	
	// If no filter, use standard search
	if filter == nil {
		res, _ := s.SearchVectors(query, k, nil)
		return res
	}

	// Scatter-Gather with oversampling to compensate for filtering
	// We search for k*2 in each shard and filter by the global bitmap
	g, _ := errgroup.WithContext(context.Background())
	results := make([][]store.SearchResult, len(s.shards))
	
	oversampleK := k * 2
	
	for i, shard := range s.shards {
		i, shard := i, shard
		g.Go(func() error {
			// Search shard without filtering (since bitset is global)
			res, err := shard.Search(query, oversampleK, nil)
			if err != nil {
				return err
			}
			
			// Filter locally-mapped results by global bitset
			filtered := make([]store.SearchResult, 0, len(res))
			for _, r := range res {
				if filter.Contains(int(r.ID)) {
					filtered = append(filtered, r)
				}
			}
			results[i] = filtered
			return nil
		})
	}
	
	if err := g.Wait(); err != nil {
		return nil
	}
	
	// Merge
	return s.aggregator.MergeKWay(results, k)
}

// Helper to expose shards for testing?
func (s *ShardedArrowIndex) Shards() []*ArrowSham {
	return s.shards
}
