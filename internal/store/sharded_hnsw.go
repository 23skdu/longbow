package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/23skdu/longbow/internal/memory"
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
	DataType       VectorDataType // Vector data type (float32, complex64, etc.)
	// ShardSplitThreshold is deprecated in favor of Ring Sharding but kept for interface/legacy compatibility.
	// In Ring mode, it implies the *initial capacity* of each shard.
	ShardSplitThreshold    int
	UseRingSharding        bool // If true, use Consistent Hashing (Ring). If false, use Linear Range.
	PackedAdjacencyEnabled bool // If true, use thread-safe packed neighbor storage (v0.1.4)
	SharedVectorSpace      bool // If true, shards use primary Dataset records for vector lookups
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
		Metric:                 MetricEuclidean,
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
	dataset *Dataset
	nextID  atomic.Int64

	// Location Storage (Lock-Free Read)
	locationStore *ChunkedLocationStore
	globalToLocal *ChunkedLocationStore // Mapping GlobalID -> LocalID

	dimension uint32

	// Dynamic Sharding
	sharder  ShardingStrategy
	shardsMu sync.RWMutex

	parallelConfig types.ParallelSearchConfig
}

// NewShardedHNSW creates a new sharded HNSW index.
// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset *Dataset) VectorIndex {
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
	for i := 0; i < config.NumShards; i++ {
		s.shards[i] = s.newShard(i)
	}

	if dataset != nil {
		dataset.Index = s
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
	arrowConfig.InitialCapacity = 1024                          // Start small, grow dynamically
	arrowConfig.Metric = idx.config.Metric
	arrowConfig.PackedAdjacencyEnabled = idx.config.PackedAdjacencyEnabled
 
	// Preserve DataType from config (critical for complex64/complex128)
	if idx.config.DataType != types.VectorTypeUnknown {
		arrowConfig.DataType = idx.config.DataType
	}

	if idx.config.DataType == types.VectorTypeTQ {
		arrowConfig.TurboQuantEnabled = true
		if idx.dataset != nil && idx.dataset.TurboQuantBits > 0 {
			arrowConfig.TurboQuantBits = idx.dataset.TurboQuantBits
		} else if arrowConfig.TurboQuantBits == 0 {
			arrowConfig.TurboQuantBits = 8
		}
	}
 
	// We pass nil for ChunkedLocationStore because shards use local IDs and don't manage global locations
	// The ShardedHNSW manages the global location store.
	var topo *memory.NUMATopology
	if idx.dataset != nil {
		topo = idx.dataset.Topo
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
 
	return newHnswShard(id)
}

// GetShardForID returns the shard index for a given Global VectorID.
func (idx *ShardedHNSW) GetShardForID(id VectorID) int {
	return idx.sharder.GetShard(id)
}

// AddByLocation adds a vector to the sharded index using its storage location.
func (idx *ShardedHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if idx.dataset == nil {
		return 0, fmt.Errorf("no dataset")
	}
	idx.dataset.dataMu.RLock()
	defer idx.dataset.dataMu.RUnlock()
 
	return idx.AddByLocationUnsafe(ctx, batchIdx, rowIdx)
}

// AddByLocationUnsafe adds a vector without taking dataset locks.
func (idx *ShardedHNSW) AddByLocationUnsafe(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if batchIdx >= len(idx .dataset.Records.Read()) {
		return 0, fmt.Errorf("invalid batch idx")
	}
	rec := idx .dataset.Records.Read()[batchIdx]
	return idx.AddByRecord(ctx, rec, rowIdx, batchIdx)
}

// AddSafe adds a single record to the sharded index safely.
func (idx *ShardedHNSW) AddSafe(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (VectorID, error) {
	id, err := idx.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err != nil {
		return 0, err
	}
	return VectorID(id), nil
}

// AddBatch adds a batch of records to the sharded index in parallel.
func (idx *ShardedHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 || len(rowIdxs) == 0 {
		return nil, nil
	}

	n := len(rowIdxs)
	globalIDs := make([]uint32, n)
	
	// 1. Group indices by shard
	type shardJob struct {
		indices []int // Original indices in the batch
	}
	shardJobs := make(map[int]*shardJob)

	for i := 0; i < n; i++ {
		// Allocate Global ID
		gid := VectorID(idx.nextID.Add(1) - 1) // #nosec G115
		globalIDs[i] = uint32(gid)

		// Set Global Location
		idx.locationStore.EnsureCapacity(gid)
		idx.locationStore.Set(gid, Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		idx.locationStore.UpdateSize(gid)

		// Route to Shard
		shardIdx := idx.sharder.GetShard(gid)
		job, ok := shardJobs[shardIdx]
		if !ok {
			job = &shardJob{indices: make([]int, 0, n/idx.config.NumShards)}
			shardJobs[shardIdx] = job
		}
		job.indices = append(job.indices, i)
	}

	// 2. Parallel Insert across shards
	g, ctx := errgroup.WithContext(ctx)

	// Time from here until the lock is fully acquired is the contention window.
	contentionStart := time.Now()
	idx.shardsMu.RLock()
	// Ensure shards exist (linear sharding growth)
	maxShardIdx := 0
	for sIdx := range shardJobs {
		if sIdx > maxShardIdx {
			maxShardIdx = sIdx
		}
	}
	if maxShardIdx >= len(idx.shards) {
		idx.shardsMu.RUnlock()
		idx.shardsMu.Lock()
		for i := len(idx.shards); i <= maxShardIdx; i++ {
			idx.shards = append(idx.shards, idx.newShard(i))
		}
		idx.shardsMu.Unlock()
		idx.shardsMu.RLock()
	}

	// Record the lock-acquisition latency (write-contention proxy) once per AddBatch call.
	if idx.dataset != nil {
		metrics.HnswUpdateContentionSeconds.WithLabelValues(idx.dataset.Name).Observe(time.Since(contentionStart).Seconds())
	}

	for shardIdx, job := range shardJobs {
		sIdx := shardIdx
		j := job
		shard := idx.shards[sIdx]

		g.Go(func() error {
			shardRowIdxs := make([]int, len(j.indices))
			shardBatchIdxs := make([]int, len(j.indices))
			for k, idxInBatch := range j.indices {
				shardRowIdxs[k] = rowIdxs[idxInBatch]
				shardBatchIdxs[k] = batchIdxs[idxInBatch]
			}

			localIDs, err := shard.index.AddBatch(ctx, recs, shardRowIdxs, shardBatchIdxs)
			if err != nil {
				return err
			}

			// Register Global->Local mappings
			for k, lid := range localIDs {
				idxInBatch := j.indices[k]
				gid := VectorID(globalIDs[idxInBatch])
				shard.registerID(lid, gid, idx.globalToLocal)
			}

			// Each successful batch insert into a shard implies a CoW adjacency
			// list copy inside the underlying ArrowHNSW graph.  Count them to
			// surface write-contention pressure in dashboards.
			if idx.dataset != nil {
				metrics.HnswCowCopyCount.WithLabelValues(idx.dataset.Name, fmt.Sprintf("%d", sIdx)).Add(float64(len(localIDs)))
			}
			metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.Name, fmt.Sprintf("%d", sIdx)).Add(float64(len(localIDs)))
			return nil
		})
	}
	
	idx.shardsMu.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, err
	}

	idx.updateShardBalanceMetrics()
	return globalIDs, nil
}

// DeleteBatch removes multiple vectors from the sharded index.
func (idx *ShardedHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	// Group by shard
	shardIds := make(map[int][]uint32)
	for _, id := range ids {
		vid := VectorID(id)
		shardIdx := idx.sharder.GetShard(vid)
		shardIds[shardIdx] = append(shardIds[shardIdx], id)
	}
 
	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()
 
	for shardIdx, distinctIDs := range shardIds {
		if shardIdx >= len(idx.shards) || idx.shards[shardIdx] == nil {
			continue
		}
		shard := idx.shards[shardIdx]
		// Convert Global IDs to Local IDs
		var localIDs []uint32
		for _, gid := range distinctIDs {
			if loc, ok := idx.globalToLocal.Get(VectorID(gid)); ok {
				localIDs = append(localIDs, uint32(loc.BatchIdx)) // #nosec G115
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

// IsSharded returns true as this is a sharded index implementation.
func (idx *ShardedHNSW) IsSharded() bool {
	return true
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

// AddByRecord adds a single vector from an Arrow RecordBatch to the sharded index.
func (idx *ShardedHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	// Allocate Global ID
	id := VectorID(idx.nextID.Add(1) - 1) // #nosec G115
 
	// Update global locations (Lock-Free)
	idx.locationStore.EnsureCapacity(id)
	idx.locationStore.Set(id, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	idx.locationStore.UpdateSize(id)
 
	// Route to Shard
	shardIdx := idx.sharder.GetShard(id)
 
	idx.shardsMu.RLock()
	if shardIdx < len(idx.shards) {
		shard := idx.shards[shardIdx]
		idx.shardsMu.RUnlock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id, idx.globalToLocal)
		metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
		if id%1000 == 0 {
			idx.updateShardBalanceMetrics()
		}
		return uint32(id), nil
	}
	idx.shardsMu.RUnlock()
 
	// If we are here, we might need to grow.
	// Only linear sharding supports growth.
	if idx.config.UseRingSharding {
		return 0, fmt.Errorf("shard index out of bounds (dynamic growth not supported in ring mode)")
	}
 
	// Dynamic Growth (Double-checked locking)
	idx.shardsMu.Lock()
	if shardIdx < len(idx.shards) {
		// Someone else created it
		shard := idx.shards[shardIdx]
		idx.shardsMu.Unlock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id, idx.globalToLocal)
		metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
		if id%1000 == 0 {
			idx.updateShardBalanceMetrics()
		}
		return uint32(id), nil
	}
 
	// Grow
	// We fill potential gaps if shardIdx skips
	for i := len(idx.shards); i <= shardIdx; i++ {
		idx.shards = append(idx.shards, idx.newShard(i))
	}
	shard := idx.shards[shardIdx]
	idx.shardsMu.Unlock()
 
	// Insert
	localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err != nil {
		return 0, fmt.Errorf("shard insert failed: %w", err)
	}
	shard.registerID(localID, id, idx.globalToLocal)
 
	metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.Name, fmt.Sprintf("%d", shardIdx)).Inc()
	if id%1000 == 0 {
		idx.updateShardBalanceMetrics()
	}
	return uint32(id), nil
}

// SearchVectors finds the k-nearest neighbors using similarity search across all shards.
func (idx *ShardedHNSW) SearchVectors(ctx context.Context, queryVec any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
 
	searchOptions := SearchOptions{}
	if opt, ok := options.(SearchOptions); ok {
		searchOptions = opt
	}
	_ = searchOptions // Mark as used
 
	// 1. Optimization: Try bitmap-based filtering
	if len(filters) > 0 && idx.dataset != nil {
		var filterExpr FilterExpr
		if opts, ok := options.(SearchOptions); ok {
			filterExpr = opts.FilterExpr
		}
		bitset, err := idx.dataset.GenerateFilterBitset(filters, filterExpr)
		if err == nil && bitset != nil {
			defer bitset.Release()
			res, _ := idx.SearchVectorsWithBitmap(ctx, queryVec, k, bitset.AsRoaring(), options)
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
 
	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)
 
	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()
 
	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}
 
			res, err := shard.index.SearchVectors(ctx, queryVec, k*2, nil, options) // Oversample, evaluate filters on merge
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
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
			if !ok {
				continue
			}
			r.ID = globalID
 
			// Re-check global filters if needed
			if len(filters) > 0 {
				_, ok := idx.locationStore.Get(globalID)
				if !ok {
					continue
				}
				// Evaluate filters here if needed.
			}
 
			merged = append(merged, r)
		}
	}
 
	// Filter Block (Redundant if shards filtered, but kept for safety/fallback)
	if len(filters) > 0 && idx.dataset != nil {
		idx.dataset.dataMu.RLock()
		if len(idx .dataset.Records.Read()) > 0 {
			// 1. Group row indices by BatchIdx
			type batchJob struct {
				rowIndices []int
				resultIdx  []int
			}
			batchJobs := make(map[int]*batchJob)

			for i, r := range merged {
				loc, ok := idx.locationStore.Get(r.ID)
				if !ok || loc.BatchIdx >= len(idx .dataset.Records.Read()) {
					continue
				}
				job, ok := batchJobs[loc.BatchIdx]
				if !ok {
					job = &batchJob{}
					batchJobs[loc.BatchIdx] = job
				}
				job.rowIndices = append(job.rowIndices, loc.RowIdx)
				job.resultIdx = append(job.resultIdx, i)
			}

			// 2. Evaluate each batch
			filteredMask := make([]bool, len(merged))
			for bIdx, job := range batchJobs {
				ev, err := query.NewFilterEvaluator(idx .dataset.Records.Read()[bIdx], filters)
				if err != nil {
					continue
				}
				matches := ev.MatchesBatch(job.rowIndices)
				
				// Create a quick lookup for matched row indices in this batch
				matchMap := make(map[int]struct{}, len(matches))
				for _, m := range matches {
					matchMap[m] = struct{}{}
				}
				
				// Mark results in the filteredMask
				for k, rowIdx := range job.rowIndices {
					if _, matched := matchMap[rowIdx]; matched {
						resIdx := job.resultIdx[k]
						filteredMask[resIdx] = true
					}
				}
			}

			// 3. Rebuild filtered results
			filtered := merged[:0]
			for i, r := range merged {
				if filteredMask[i] {
					filtered = append(filtered, r)
				}
			}
			merged = filtered
		}
		idx.dataset.dataMu.RUnlock()
	}
 
	// Sort and limit (ascending - lower distance/score is better)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})
 
	if len(merged) > k {
		merged = merged[:k]
	}
 
	return merged, nil
}

// SearchVectorsWithBitmap finds k-nearest neighbors using a bitmap filter.
func (idx *ShardedHNSW) SearchVectorsWithBitmap(ctx context.Context, queryVec any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	type shardResult struct {
		results  []SearchResult
		shardIdx int
		err      error
	}
	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)
 
	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()
 
	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}
 
			// Pass nil filter to shard, filter globally
			res, err := shard.index.SearchVectorsWithBitmap(ctx, queryVec, k*2, nil, options)
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
 
	// Check for errors first
	merged := make([]SearchResult, 0, k*2)
	for sr := range ch {
		for _, r := range sr.results {
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
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

// SearchVectorsInRange finds all vectors within a given distance threshold.
func (idx *ShardedHNSW) SearchVectorsInRange(ctx context.Context, queryVec any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	type shardResult struct {
		results  []SearchResult
		shardIdx int
		err      error
	}
	ch := make(chan shardResult, len(idx.shards))
	g, ctx := errgroup.WithContext(ctx)
 
	idx.shardsMu.RLock()
	currentShards := idx.shards
	idx.shardsMu.RUnlock()
 
	for i, shard := range currentShards {
		if shard == nil || shard.index == nil {
			continue
		}
		i := i
		shard := shard
		g.Go(func() error {
			// Pin thread to shard's NUMA node if possible
			if h, ok := shard.index.(interface {
				GetNUMANode() (int, *memory.NUMATopology)
			}); ok {
				nodeID, topo := h.GetNUMANode()
				if topo != nil && nodeID >= 0 {
					_ = memory.PinToNUMANode(topo, nodeID)
				}
			}
 
			res, err := shard.index.SearchVectorsInRange(ctx, queryVec, threshold, nil, options)
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
 
	var merged []SearchResult
	for sr := range ch {
		if sr.err != nil {
			continue
		}
		for _, r := range sr.results {
			globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(r.ID)})
			if !ok {
				continue
			}
			r.ID = globalID
 
			if len(filters) > 0 {
				_, ok := idx.locationStore.Get(globalID)
				if !ok {
					continue
				}
			}
			merged = append(merged, r)
		}
	}
 
	if len(filters) > 0 && idx.dataset != nil {
		idx.dataset.dataMu.RLock()
		if len(idx .dataset.Records.Read()) > 0 {
			filtered := merged[:0]
			for _, r := range merged {
				loc, ok := idx.locationStore.Get(r.ID)
				if !ok || loc.BatchIdx >= len(idx .dataset.Records.Read()) {
					continue
				}
				filtered = append(filtered, r)
			}
			merged = filtered
		}
		idx.dataset.dataMu.RUnlock()
	}
 
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Score < merged[j].Score
	})
 
	return merged, nil
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

// SearchByID searches for vectors similar to the vector at the given ID.
func (idx *ShardedHNSW) SearchByID(ctx context.Context, id VectorID, k int) []VectorID {
	if k <= 0 {
		return nil
	}
 
	loc, ok := idx.locationStore.Get(id)
	if !ok {
		return nil
	}
 
	// Retrieve vector from dataset
	idx.dataset.dataMu.RLock()
	if loc.BatchIdx >= len(idx .dataset.Records.Read()) {
		idx.dataset.dataMu.RUnlock()
		return nil
	}
	rec := idx .dataset.Records.Read()[loc.BatchIdx]
	idx.dataset.dataMu.RUnlock()
 
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
	results, err := idx.SearchVectors(ctx, vec, k, nil, SearchOptions{})
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

// GetRawNeighbors returns the internal IDs of nearest neighbors.
func (idx *ShardedHNSW) GetRawNeighbors(id uint32) ([]uint32, error) {
	shardIdx := idx.GetShardForID(VectorID(id))
 
	idx.shardsMu.RLock()
	if shardIdx >= len(idx.shards) || idx.shards[shardIdx] == nil {
		idx.shardsMu.RUnlock()
		return nil, fmt.Errorf("invalid shard index")
	}
	shard := idx.shards[shardIdx]
	idx.shardsMu.RUnlock()
 
	loc, ok := idx.globalToLocal.Get(VectorID(id))
	if !ok {
		return nil, fmt.Errorf("vector id not found in mapping")
	}
	localID := uint32(loc.BatchIdx) // #nosec G115
 
	// Get local neighbors
	localNeighbors, err := shard.index.GetRawNeighbors(localID)
	if err != nil {
		return nil, err
	}
 
	// Map to Global IDs
	globalNeighbors := make([]uint32, 0, len(localNeighbors))
	for _, ln := range localNeighbors {
		globalID, ok := idx.globalToLocal.GetID(types.Location{BatchIdx: int(ln)})
		if !ok {
			continue
		}
		globalNeighbors = append(globalNeighbors, uint32(globalID))
	}
	return globalNeighbors, nil
}

// GetNeighbors returns the k nearest neighbors for a given vector ID as SearchResults.
func (idx *ShardedHNSW) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	neighbors, err := idx.GetRawNeighbors(id)
	if err != nil {
		return nil, err
	}
 
	results := make([]types.SearchResult, 0, min(k, len(neighbors)))
	for i := 0; i < len(neighbors) && i < k; i++ {
		results = append(results, types.SearchResult{
			ID: types.VectorID(neighbors[i]),
		})
	}
 
	return results, nil
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
				datasetName = idx.dataset.Name
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
		datasetName = idx.dataset.Name
	}
	metrics.ShardBalanceImbalanceRatio.WithLabelValues(datasetName).Set(coefficientOfVariation)
}
