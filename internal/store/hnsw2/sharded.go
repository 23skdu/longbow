package hnsw2

import (
	"context"
	"fmt"
	"sort"
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
	
	g, _ := errgroup.WithContext(context.Background())
	// Use a semaphore to limit total concurrency across all shards if needed,
	// but since each shard has its own locks, we can just fan out.
	// For large batches, we might want to group by shard first.
	
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			id, err := s.AddByLocation(batchIdxs[i], rowIdxs[i])
			if err != nil {
				return err
			}
			ids[i] = id
			return nil
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
	return s.mergeResults(results, k), nil
}

func (s *ShardedArrowIndex) mergeResults(shardResults [][]store.SearchResult, k int) []store.SearchResult {
	// Flatten
	totalLen := 0
	for _, r := range shardResults {
		totalLen += len(r)
	}
	
	all := make([]store.SearchResult, 0, totalLen)
	for _, r := range shardResults {
		all = append(all, r...)
	}
	
	// Sort by Score (Distance) Ascending
	sort.Slice(all, func(i, j int) bool {
		return all[i].Score < all[j].Score // euclidean distance
	})
	
	// Top K
	if len(all) > k {
		return all[:k]
	}
	return all
}

// ---- ArrowSham methods ----

func (s *ArrowSham) Add(globalID uint32, batchIdx, rowIdx int) error {
	s.mu.Lock()
	lID := uint32(len(s.localToGlobal))
	s.localToGlobal = append(s.localToGlobal, globalID)
	s.mu.Unlock()
	
	// Use public API which handles locationStore and Insert internally
	localID, err := s.index.AddByLocation(batchIdx, rowIdx)
	if err != nil {
		return err
	}
	
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
	// TODO: Implement scatter-gather for bitmap search
	// For now, delegate to SearchVectors (ignoring bitmap) or return empty?
	// Returning empty is safer than wrong results.
	// Users of this index likely use SearchVectors.
	return nil
}

// Helper to expose shards for testing?
func (s *ShardedArrowIndex) Shards() []*ArrowSham {
	return s.shards
}
