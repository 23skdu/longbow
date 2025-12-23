package store

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/coder/hnsw"
)

// ShardedHNSWConfig configures the sharded HNSW index.
type ShardedHNSWConfig struct {
	NumShards      int          // Number of independent HNSW shards
	M              int          // HNSW M parameter
	EfConstruction int          // HNSW efConstruction parameter
	Metric         VectorMetric // Distance metric for this index
	Dimension      uint32       // Vector dimension
}

func (c ShardedHNSWConfig) Validate() error {
	if c.NumShards <= 0 {
		return fmt.Errorf("NumShards must be > 0")
	}
	if c.M <= 0 {
		return fmt.Errorf("M must be > 0")
	}
	if c.EfConstruction <= 0 {
		return fmt.Errorf("EfConstruction must be > 0")
	}
	return nil
}

// DefaultShardedHNSWConfig returns sensible defaults.
func DefaultShardedHNSWConfig() ShardedHNSWConfig {
	return ShardedHNSWConfig{
		NumShards:      runtime.NumCPU(),
		M:              16,
		EfConstruction: 200,
		Metric:         MetricEuclidean,
	}
}

// hnswShard represents a single HNSW graph with its own lock.
type hnswShard struct {
	mu    sync.RWMutex
	graph *hnsw.Graph[VectorID]
}

// Warmup accesses all nodes in the shard.
func (s *hnswShard) Warmup() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.graph.Len()
}

// ShardedHNSW provides fine-grained locking via multiple independent HNSW shards.
type ShardedHNSW struct {
	config     ShardedHNSWConfig
	shards     []*hnswShard
	dataset    *Dataset
	nextID     atomic.Int64
	globalLocs []Location
	globalMu   sync.RWMutex
	dimension  uint32
}

// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset *Dataset) *ShardedHNSW {
	if config.NumShards <= 0 {
		config.NumShards = runtime.NumCPU()
	}

	shards := make([]*hnswShard, config.NumShards)
	for i := 0; i < config.NumShards; i++ {
		shards[i] = &hnswShard{
			graph: hnsw.NewGraph[VectorID](),
		}
		shards[i].graph.Distance = sHNSWGetDistFunc(config.Metric)
	}

	return &ShardedHNSW{
		config:     config,
		shards:     shards,
		dataset:    dataset,
		globalLocs: make([]Location, 0, 4096),
		dimension:  config.Dimension,
	}
}

func sHNSWGetDistFunc(m VectorMetric) func(a, b []float32) float32 {
	switch m {
	case MetricCosine:
		return simd.CosineDistance
	case MetricDotProduct:
		return func(a, b []float32) float32 {
			return -simd.DotProduct(a, b)
		}
	default:
		return simd.EuclideanDistance
	}
}

// GetShardForID returns the shard index for a given VectorID.
func (s *ShardedHNSW) GetShardForID(id VectorID) int {
	return int(uint64(id) % uint64(s.config.NumShards))
}

// AddByLocation implements VectorIndex.
func (s *ShardedHNSW) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	// Transitionary: get vector from dataset to add to graph
	s.dataset.dataMu.RLock()
	if batchIdx >= len(s.dataset.Records) {
		s.dataset.dataMu.RUnlock()
		return 0, fmt.Errorf("invalid batch idx")
	}
	rec := s.dataset.Records[batchIdx]
	s.dataset.dataMu.RUnlock()

	return s.AddByRecord(rec, rowIdx, batchIdx)
}

// AddSafe is an alias for AddByRecord for consistency with HNSWIndex.
func (s *ShardedHNSW) AddSafe(rec arrow.RecordBatch, rowIdx, batchIdx int) (VectorID, error) {
	id, err := s.AddByRecord(rec, rowIdx, batchIdx)
	if err != nil {
		return 0, err
	}
	return VectorID(id), nil
}

// AddByRecord implements VectorIndex.
func (s *ShardedHNSW) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	// Extract vector (Simplified for brevity, following hnsw.go pattern)
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

	// Allocate ID
	id := VectorID(s.nextID.Add(1) - 1)

	// Update global locations
	s.globalMu.Lock()
	for len(s.globalLocs) <= int(id) {
		s.globalLocs = append(s.globalLocs, Location{})
	}
	s.globalLocs[id] = Location{BatchIdx: batchIdx, RowIdx: rowIdx}
	s.globalMu.Unlock()

	// Add to shard
	shardIdx := s.GetShardForID(id)
	shard := s.shards[shardIdx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Copy vector for stability if not using zero-copy storage in shards yet
	vecCopy := make([]float32, len(vec))
	copy(vecCopy, vec)
	shard.graph.Add(hnsw.MakeNode(id, vecCopy))

	metrics.ShardedHnswShardSize.WithLabelValues(s.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
	return uint32(id), nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (s *ShardedHNSW) SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {
	if k <= 0 {
		return nil
	}

	// 1. Search all shards in parallel
	type shardResults struct {
		nodes []hnsw.Node[VectorID]
	}
	resultsByShard := make([]shardResults, len(s.shards))
	var wg sync.WaitGroup

	limit := k * 10
	if filter != nil && filter.Count() > 0 && filter.Count() < 1000 {
		// Use bruteforce/filter check?
		// Sharded bruteforce?
		// For MVP, stick to Post-Filtering.
	}

	for i, shard := range s.shards {
		wg.Add(1)
		go func(idx int, sh *hnswShard) {
			defer wg.Done()
			sh.mu.RLock()
			resultsByShard[idx].nodes = sh.graph.Search(query, limit)
			sh.mu.RUnlock()
		}(i, shard)
	}
	wg.Wait()

	merged := make([]SearchResult, 0, k*2)
	_ = sHNSWGetDistFunc(s.config.Metric)

	for _, sr := range resultsByShard {
		for _, node := range sr.nodes {
			id := uint32(node.Key)

			// Check filters
			if filter != nil && !filter.Contains(int(id)) {
				continue
			}

			// Resolve Location for distance calc (or use node?)
			// node has distance if computed by Search?
			// Need to verify. Assume we need recompute or `hnsw` provides it.
			// Recomputing to be safe.

			s.globalMu.RLock()
			if int(node.Key) >= len(s.globalLocs) {
				s.globalMu.RUnlock()
				continue
			}
			loc := s.globalLocs[node.Key]
			s.globalMu.RUnlock()

			// Recompute distance
			var score float32
			vec, err := s.getVectorFromDataset(loc)
			if err == nil {
				distFunc := sHNSWGetDistFunc(s.config.Metric)
				score = distFunc(query, vec)
			}

			merged = append(merged, SearchResult{ID: node.Key, Score: score})
		}
	}

	// Sort and limit
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged
}

// SearchVectors implements VectorIndex.
func (s *ShardedHNSW) SearchVectors(query []float32, k int, filters []Filter) []SearchResult {
	if k <= 0 {
		return nil
	}

	// 1. Search all shards in parallel
	type shardResults struct {
		nodes []hnsw.Node[VectorID]
	}
	resultsByShard := make([]shardResults, len(s.shards))
	var wg sync.WaitGroup

	for i, shard := range s.shards {
		wg.Add(1)
		go func(idx int, sh *hnswShard) {
			defer wg.Done()
			sh.mu.RLock()
			resultsByShard[idx].nodes = sh.graph.Search(query, k*2) // Oversample for merging
			sh.mu.RUnlock()
		}(i, shard)
	}
	wg.Wait()

	// 2. Merge and Filter
	// In a real sharded system, we'd do a heap merge.
	// To support FilterEvaluator, we need to access records.

	var evaluator *FilterEvaluator
	if len(filters) > 0 && s.dataset != nil {
		s.dataset.dataMu.RLock()
		if len(s.dataset.Records) > 0 {
			evaluator, _ = NewFilterEvaluator(s.dataset.Records[0], filters)
		}
		s.dataset.dataMu.RUnlock()
	}

	merged := make([]SearchResult, 0, k*2)
	_ = sHNSWGetDistFunc(s.config.Metric) // Ensure it compiles if we don't use it yet

	for _, sr := range resultsByShard {
		for _, node := range sr.nodes {
			// Check filters
			s.globalMu.RLock()
			if int(node.Key) >= len(s.globalLocs) {
				s.globalMu.RUnlock()
				continue
			}
			loc := s.globalLocs[node.Key]
			s.globalMu.RUnlock()

			if s.dataset != nil {
				s.dataset.dataMu.RLock()
				if loc.BatchIdx >= len(s.dataset.Records) {
					s.dataset.dataMu.RUnlock()
					continue
				}

				if evaluator != nil {
					if !evaluator.Matches(loc.RowIdx) {
						s.dataset.dataMu.RUnlock()
						continue
					}
				}
				s.dataset.dataMu.RUnlock()
			}

			// Recompute distance for merging/sorting
			var score float32
			vec, err := s.getVectorFromDataset(loc)
			if err == nil {
				distFunc := sHNSWGetDistFunc(s.config.Metric)
				score = distFunc(query, vec)
			}

			merged = append(merged, SearchResult{ID: node.Key, Score: score})
		}
	}

	// Sort and limit
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score // Ascending distance (lower is better for Euclidean/Cosine distance as implemented)
	})

	if len(merged) > k {
		merged = merged[:k]
	}

	return merged
}

func (s *ShardedHNSW) getVectorFromDataset(loc Location) ([]float32, error) {
	if s.dataset == nil {
		return nil, fmt.Errorf("no dataset")
	}
	s.dataset.dataMu.RLock()
	defer s.dataset.dataMu.RUnlock()

	if loc.BatchIdx >= len(s.dataset.Records) {
		return nil, fmt.Errorf("batch index out of bounds")
	}
	rec := s.dataset.Records[loc.BatchIdx]

	// Fast path: cached schema info? For now walk fields.
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}
	if vecCol == nil {
		return nil, fmt.Errorf("vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil, fmt.Errorf("invalid vector column")
	}

	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release() // Are we sure? NewFloat32Data usually retains? Yes, must release to match Retain if any.
	// Actually NewFloat32Data creates a wrapper around existing Buffer. If we don't Retain buffer, we don't need Release wrapper?
	// Arrow Go semantics: New...Data usually calls Retain on buffers.

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width
	if start < 0 || end > floatArr.Len() {
		return nil, fmt.Errorf("row index out of bounds")
	}

	// Copy to be safe from Release? No, we return slice.
	// If floatArr is Released, buffer might be freed?
	// We need safe copy.
	src := floatArr.Float32Values()[start:end]
	dst := make([]float32, len(src))
	copy(dst, src)
	return dst, nil
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

	// 1. Get location and search the corresponding shard
	loc, ok := s.GetLocation(id)
	if !ok {
		return nil
	}

	if s.dataset == nil {
		return nil
	}

	shardIdx := s.GetShardForID(id)
	shard := s.shards[shardIdx]

	shard.mu.RLock()
	// Get query vector from shard if possible, or from dataset
	// Simplified: use search by ID if HNSW supports it, or get vector first.
	// For now, let's get vector and search.
	// We need the query vector.
	// We can use the dataset and location.
	shard.mu.RUnlock()

	s.dataset.dataMu.RLock()
	if loc.BatchIdx >= len(s.dataset.Records) {
		s.dataset.dataMu.RUnlock()
		return nil
	}
	rec := s.dataset.Records[loc.BatchIdx]
	s.dataset.dataMu.RUnlock()

	// Extract vector (simplified)
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}
	if vecCol == nil {
		return nil
	}

	listArr := vecCol.(*array.FixedSizeList)
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	start := loc.RowIdx * width
	vec := floatArr.Float32Values()[start : start+width]

	// Search all shards using this vector
	results := s.SearchVectors(vec, k, nil)
	ids := make([]VectorID, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	return ids
}

// SetIndexedColumns satisfies VectorIndex interface
func (s *ShardedHNSW) SetIndexedColumns(cols []string) {
	// Sharded HNSW itself doesn't use these yet
}

// Warmup warms up all shards.
func (idx *ShardedHNSW) Warmup() int {
	total := 0
	for _, shard := range idx.shards {
		// Concurrent warmup could be better, but sequential is safer for now
		if shard != nil {
			total += shard.Warmup()
		}
	}
	return total
}

// Close implements VectorIndex.
func (s *ShardedHNSW) Close() error {
	return nil
}

// GetLocation returns the storage location for a given VectorID
func (s *ShardedHNSW) GetLocation(id VectorID) (Location, bool) {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	if int(id) >= len(s.globalLocs) {
		return Location{}, false
	}
	return s.globalLocs[id], true
}

// GetDimension implements VectorIndex.
func (s *ShardedHNSW) GetDimension() uint32 {
	return s.dimension
}

// Stats returns multi-index statistics for all shards.
func (s *ShardedHNSW) ShardStats() []ShardStat {
	stats := make([]ShardStat, len(s.shards))
	for i, shard := range s.shards {
		shard.mu.RLock()
		stats[i] = ShardStat{
			ShardID: i,
			Count:   int(shard.graph.Len()),
		}
		shard.mu.RUnlock()
	}
	return stats
}

// ShardStat holds statistics for a single shard.
type ShardStat struct {
	ShardID int
	Count   int
}
