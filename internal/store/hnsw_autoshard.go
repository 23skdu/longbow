package store

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"io"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
)

// AutoShardingConfig configures the auto-sharding behavior.
type AutoShardingConfig struct {
	// Enabled determines if auto-sharding is active.
	Enabled bool
	// ShardThreshold is the number of vectors at which to trigger sharding.
	ShardThreshold int
	// ShardCount is the target number of shards to create (defaults to NumCPU).
	ShardCount int
	// ShardSplitThreshold is the size of each shard (defaults to 65536).
	ShardSplitThreshold int
	// UseRingSharding determines if consistent hashing sharding is used.
	UseRingSharding bool

	// IndexConfig holds the configuration for the underlying HNSW index (optional).
	// If set, NewAutoShardingIndex will generic ArrowHNSW with this config.
	IndexConfig *ArrowHNSWConfig
}

// DefaultAutoShardingConfig returns a standard configuration.
func DefaultAutoShardingConfig() AutoShardingConfig {
	return AutoShardingConfig{
		ShardThreshold: 10000,
		ShardCount:     4, // Simplified default, real impl might use runtime.NumCPU()
	}
}

// AutoShardingIndex wraps a VectorIndex and transparently upgrades it
// to a ShardedHNSW when the dataset grows beyond a threshold.
type AutoShardingIndex struct {
	mu           sync.RWMutex
	current      VectorIndex
	config       AutoShardingConfig
	dataset      *Dataset
	sharded      bool
	interimIndex VectorIndex // NEW: Used during migration to handle new writes

	migrating atomic.Bool // Added migrating field
}

// NewAutoShardingIndex creates a new auto-sharding index.
// Initially, it uses a standard HNSWIndex.
func NewAutoShardingIndex(ds *Dataset, config AutoShardingConfig) VectorIndex {
	if config.ShardThreshold <= 0 {
		config.ShardThreshold = 10000 // Default to 10k
	}

	var idx VectorIndex
	if config.IndexConfig != nil {
		idx = NewArrowHNSW(ds, config.IndexConfig, ds.Topo)
	} else {
		// Use ArrowHNSW as default for better performance (parallelism, batching)
		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.Metric = ds.Metric
		hnswConfig.Logger = ds.Logger
		idx = NewArrowHNSW(ds, &hnswConfig, ds.Topo)
	}

	return &AutoShardingIndex{
		current: idx,
		config:  config,
		dataset: ds,
		sharded: false,
	}
}

// SetInitialDimension sets the dimension for the underlying index if not yet set.
// SetInitialDimension sets the dimension for the underlying index if not yet set.
func (idx *AutoShardingIndex) SetInitialDimension(dim int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if h, ok := idx.current.(*ArrowHNSW); ok {
		_ = h.SetDimension(int(dim))
	}
}

// AddByLocation adds a vector to the index.
// AddByLocation adds a vector to the index.
func (idx *AutoShardingIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	// Lock held throughout execution to prevent Close during migration
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		id, err := curr.AddByLocation(ctx, batchIdx, rowIdx)
		idx.mu.RUnlock()
		return id, err
	}

	if interim != nil {
		id, err := interim.AddByLocation(ctx, batchIdx, rowIdx)
		idx.mu.RUnlock()
		return id, err
	}

	id, err := curr.AddByLocation(ctx, batchIdx, rowIdx)
	idx.mu.RUnlock()

	if err == nil {
		idx.checkShardThreshold()
	}
	return id, err
}

// AddByRecord implementation to support interim index.
// AddByRecord adds a vector from a record batch.
func (idx *AutoShardingIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		id, err := curr.AddByRecord(ctx, rec, rowIdx, batchIdx)
		idx.mu.RUnlock()
		return id, err
	}

	if interim != nil {
		id, err := interim.AddByRecord(ctx, rec, rowIdx, batchIdx)
		idx.mu.RUnlock()
		return id, err
	}

	id, err := curr.AddByRecord(ctx, rec, rowIdx, batchIdx)
	idx.mu.RUnlock()

	if err == nil {
		idx.checkShardThreshold()
	}
	return id, err
}

// AddBatch adds multiple vectors from multiple record batches efficiently.
// AddBatch adds multiple vectors efficiently.
func (idx *AutoShardingIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		ids, err := curr.AddBatch(ctx, recs, rowIdxs, batchIdxs)
		idx.mu.RUnlock()
		return ids, err
	}

	if interim != nil {
		// During migration, add to the NEW index directly
		ids, err := interim.AddBatch(ctx, recs, rowIdxs, batchIdxs)
		idx.mu.RUnlock()
		return ids, err
	}

	ids, err := curr.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	idx.mu.RUnlock()

	if err == nil {
		idx.checkShardThreshold()
	}
	return ids, err
}

func (idx *AutoShardingIndex) checkShardThreshold() {
	idx.mu.RLock()
	if idx.sharded || !idx.config.Enabled {
		idx.mu.RUnlock()
		return
	}
	currentLen := idx.current.Len()
	threshold := idx.config.ShardThreshold
	idx.mu.RUnlock()

	if currentLen >= threshold {
		if !idx.sharded && idx.migrating.CompareAndSwap(false, true) {
			go idx.migrateToSharded()
		}
	}
}

// migrateToSharded performs the migration from HNSWIndex to ShardedHNSW.
func (idx *AutoShardingIndex) migrateToSharded() {
	defer idx.migrating.Store(false) // Ensure migrating flag is reset on exit
	start := time.Now()

	// LOCK ORDER: ds.dataMu MUST be locked before idx.mu to avoid deadlock with Search
	idx.dataset.dataMu.RLock()
	idx.mu.Lock()
	if idx.sharded {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}
	if idx.current.Len() < idx.config.ShardThreshold {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}

	// Starting migration
	oldIndex := idx.current

	// Get DataType from old index (it's ArrowHNSW at this point)
	var oldDataType types.VectorDataType
	if ah, ok := oldIndex.(*ArrowHNSW); ok {
		oldDataType = ah.GetConfig().DataType
	}

	// Create new ShardedHNSW config
	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.Metric = idx.dataset.Metric
	shardedConfig.Dimension = oldIndex.GetDimension()
	shardedConfig.DataType = oldDataType // Preserve DataType for complex64/complex128
	if idx.config.ShardCount > 0 {
		shardedConfig.NumShards = idx.config.ShardCount
	}
	if idx.config.ShardSplitThreshold > 0 {
		shardedConfig.ShardSplitThreshold = idx.config.ShardSplitThreshold
	}
	shardedConfig.UseRingSharding = idx.config.UseRingSharding // Propagate ring sharding setting

	// IMPORTANT: Unlock here! We have captured our snapshots (oldIndex, n, shardedConfig).
	// We can now create the new index and run the migration without holding these global locks.
	idx.mu.Unlock()
	idx.dataset.dataMu.RUnlock()

	newIndex := NewShardedHNSW(shardedConfig, idx.dataset)

	// Promote newIndex to interimIndex so that AddBatch starts hitting it immediately
	idx.mu.Lock()
	idx.interimIndex = newIndex
	idx.mu.Unlock()

	// Migrate data in batches, releasing locks between items
	batchSize := 50
	lastMigrated := 0

	// Migration started

	for {
		// Read state under lock
		idx.mu.RLock()
		oldIdx := idx.current
		isSharded := idx.sharded
		nSnap := oldIdx.Len()
		idx.mu.RUnlock()

		if isSharded || lastMigrated >= nSnap {
			break
		}

		endIdx := lastMigrated + batchSize
		if endIdx > nSnap {
			endIdx = nSnap
		}

		// Process batch: capture records under lock, then add outside.
		type item struct {
			rec arrow.RecordBatch
			loc Location
			id  VectorID
		}
		items := make([]item, 0, endIdx-lastMigrated)

		idx.dataset.dataMu.RLock()
		for id := lastMigrated; id < endIdx; id++ {
			vid := VectorID(id)
			locAny, ok := oldIdx.GetLocation(uint32(vid))
			if ok {
				if loc, ok := locAny.(Location); ok && loc.BatchIdx >= 0 && loc.BatchIdx < len(idx.dataset.Records) {
					rec := idx.dataset.Records[loc.BatchIdx]
					rec.Retain()
					items = append(items, item{rec: rec, loc: loc, id: vid})
				}
			}
		}
		idx.dataset.dataMu.RUnlock()

		// Perform expensive additions outside dataMu
		for _, it := range items {
			_, err := newIndex.AddByRecord(context.Background(), it.rec, it.loc.RowIdx, it.loc.BatchIdx)
			it.rec.Release()
			if err != nil {
				// migration skip
				continue
			}
		}

		lastMigrated = endIdx
		if lastMigrated%1000 == 0 {
			// progress logging (future)
			_ = lastMigrated
		}

		// Give other threads a window
		runtime.Gosched()
		// Small sleep to ensure fairness on high-core machines
		time.Sleep(10 * time.Microsecond)
	}

	// Final swap

	// LOCK ORDER: ds.dataMu MUST be locked before idx.mu to avoid deadlock with Search
	idx.dataset.dataMu.RLock()
	idx.mu.Lock()

	if idx.sharded {
		idx.dataset.dataMu.RUnlock()
		idx.mu.Unlock()
		return
	}

	// Swap
	idx.current = newIndex
	idx.sharded = true
	idx.interimIndex = nil // Clear interim index

	// Close old index to release resources
	_ = oldIndex.Close()

	idx.dataset.dataMu.RUnlock()
	idx.mu.Unlock()

	duration := time.Since(start)
	metrics.IndexMigrationDuration.Observe(duration.Seconds())
}

// SearchVectors implements VectorIndex.
// SearchVectors returns the k nearest neighbors for a query vector.
func (idx *AutoShardingIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	sharded := idx.sharded
	if sharded {
		return idx.current.SearchVectors(ctx, q, k, filters, options)
	}

	interim := idx.interimIndex
	res, err := idx.current.SearchVectors(ctx, q, k, filters, options)
	if err != nil {
		return nil, err
	}

	if interim != nil {
		res2, err := interim.SearchVectors(ctx, q, k, filters, options)
		if err != nil {
			// Log error but return what we have
			return res, nil
		}
		res = idx.mergeSearchResults(res, res2, k)
	}

	return res, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (idx *AutoShardingIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	sharded := idx.sharded
	if sharded {
		return idx.current.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	interim := idx.interimIndex
	res, err := idx.current.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	if err != nil {
		return nil, err
	}
	if interim != nil {
		res2, err := interim.SearchVectorsWithBitmap(ctx, q, k, filter, options)
		if err == nil {
			res = idx.mergeSearchResults(res, res2, k)
		}
	}

	return res, nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (idx *AutoShardingIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	sharded := idx.sharded
	if sharded {
		return idx.current.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}

	interim := idx.interimIndex
	res, err := idx.current.SearchVectorsInRange(ctx, q, threshold, filters, options)
	if err != nil {
		return nil, err
	}
	if interim != nil {
		res2, err := interim.SearchVectorsInRange(ctx, q, threshold, filters, options)
		if err == nil {
			res = append(res, res2...)
		}
	}

	return res, nil
}

func (idx *AutoShardingIndex) mergeSearchResults(res1, res2 []SearchResult, k int) []SearchResult {
	if len(res1) == 0 {
		if len(res2) > k {
			return res2[:k]
		}
		return res2
	}
	if len(res2) == 0 {
		if len(res1) > k {
			return res1[:k]
		}
		return res1
	}

	combined := make([]SearchResult, 0, len(res1)+len(res2))
	combined = append(combined, res1...)
	combined = append(combined, res2...)

	// Sort by score ascending (lower is better for HNSW distances in Longbow)
	sort.Slice(combined, func(i, j int) bool {
		return combined[i].Score < combined[j].Score
	})

	if len(combined) > k {
		return combined[:k]
	}
	return combined
}

func (idx *AutoShardingIndex) IsSharded() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.sharded
}

// Len implements VectorIndex.
// GetIndexType returns the type of the active index.
func (idx *AutoShardingIndex) GetIndexType() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetIndexType()
}

func (idx *AutoShardingIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.Len()
}

// GetDimension implements VectorIndex.
func (idx *AutoShardingIndex) GetDimension() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetDimension()
}

// SetEfConstruction updates the efConstruction parameter dynamically.
// SetEfConstruction updates the efConstruction parameter dynamically.
func (idx *AutoShardingIndex) SetEfConstruction(ef int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Update current index if supported
	if h, ok := idx.current.(interface{ SetEfConstruction(int) }); ok {
		h.SetEfConstruction(ef)
	}

	// Update interim index if exists
	if idx.interimIndex != nil {
		if h, ok := idx.interimIndex.(interface{ SetEfConstruction(int) }); ok {
			h.SetEfConstruction(ef)
		}
	}
}

// TrainPQ trains a Product Quantizer for the index.
func (idx *AutoShardingIndex) TrainPQ(vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.TrainPQ(vectors)
}

// GetPQEncoder returns the Product Quantizer encoder for the index.
func (idx *AutoShardingIndex) GetPQEncoder() *pq.PQEncoder {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetPQEncoder()
}

// SetIndexedColumns configures which columns are indexed for fast equality lookups
func (idx *AutoShardingIndex) SetIndexedColumns(cols []string) {
	// No-op for now, or delegate if underlying supports it
}

// GetLocation retrieves the storage location for a given vector ID.
func (idx *AutoShardingIndex) GetLocation(id uint32) (any, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetLocation(id)
}

// GetVectorID retrieves the vector ID for a given storage location.
func (idx *AutoShardingIndex) GetVectorID(loc any) (uint32, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetVectorID(loc)
}

// Search implements VectorIndexer.
func (idx *AutoShardingIndex) Search(ctx context.Context, q any, k int, options any) ([]types.Candidate, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.Search(ctx, q, k, options)
}

// Warmup delegates to the current index.
func (idx *AutoShardingIndex) Warmup() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.Warmup()
}

// PreWarm explicitly warms up the underlying index to a target size.
func (idx *AutoShardingIndex) PreWarm(targetSize int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if h, ok := idx.current.(*ArrowHNSW); ok {
		h.PreWarm(targetSize)
	}
}

// Close closes the current index.

func (idx *AutoShardingIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.Close()
}

// GetRawNeighbors returns the diagnostic neighbor IDs for a given vector ID.
func (idx *AutoShardingIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Primarily check current index
	neighbors, err := idx.current.GetRawNeighbors(id)
	if err == nil {
		return neighbors, nil
	}

	// If not found and merging, check interim
	if idx.interimIndex != nil {
		return idx.interimIndex.GetRawNeighbors(id)
	}

	return nil, err
}

// GetNeighbors returns the k nearest neighbors for a given vector ID as SearchResults.
func (idx *AutoShardingIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]types.SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Primarily check current index
	res, err := idx.current.GetNeighbors(ctx, id, k)
	if err == nil {
		return res, nil
	}

	// If not found and merging, check interim
	if idx.interimIndex != nil {
		return idx.interimIndex.GetNeighbors(ctx, id, k)
	}

	return nil, err
}

// EstimateMemory implements VectorIndex.
func (idx *AutoShardingIndex) EstimateMemory() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	size := int64(64) // Base struct overhead
	size += idx.current.EstimateMemory()

	if idx.interimIndex != nil {
		size += idx.interimIndex.EstimateMemory()
	}

	return size
}

// DeleteBatch removes a batch of vectors from the index.
func (idx *AutoShardingIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.DeleteBatch(ctx, ids)
}

// GetEntryPoint returns the entry point node ID for HNSW traversal.
func (idx *AutoShardingIndex) GetEntryPoint() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetEntryPoint()
}

// Size returns the total number of nodes in the index.
func (idx *AutoShardingIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.Size()
}

// ExportState serializes the current index state.
func (idx *AutoShardingIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportState()
}

// ImportState restores the index state from a byte slice.
func (idx *AutoShardingIndex) ImportState(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ImportState(data)
}

// ExportGraph serializes the graph structure to a writer.
func (idx *AutoShardingIndex) ExportGraph(w io.Writer) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportGraph(w)
}

// ImportGraph restores the graph structure from a reader.
func (idx *AutoShardingIndex) ImportGraph(r io.Reader) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ImportGraph(r)
}

// ExportDelta returns a delta sync object since the given version.
func (idx *AutoShardingIndex) ExportDelta(fromVersion uint64) (*types.DeltaSync, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.ExportDelta(fromVersion)
}

// ApplyDelta applies a delta sync object to the index.
func (idx *AutoShardingIndex) ApplyDelta(delta *types.DeltaSync) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.ApplyDelta(delta)
}

// GetParallelSearchConfig returns the current parallel search configuration.
func (idx *AutoShardingIndex) GetParallelSearchConfig() types.ParallelSearchConfig {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetParallelSearchConfig()
}

// SetParallelSearchConfig updates the parallel search configuration
func (idx *AutoShardingIndex) SetParallelSearchConfig(cfg types.ParallelSearchConfig) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.current.SetParallelSearchConfig(cfg)
	if idx.interimIndex != nil {
		idx.interimIndex.SetParallelSearchConfig(cfg)
	}
}

// RemapLocations updates storage locations for physical movement (compaction).
func (idx *AutoShardingIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.current.RemapLocations(ctx, mapping)
}

// GetData returns graph data from the current index.
func (idx *AutoShardingIndex) GetData() *types.GraphData {
	idx.mu.RLock()
	curr := idx.current
	idx.mu.RUnlock()

	if g, ok := curr.(interface{ GetData() *types.GraphData }); ok {
		return g.GetData()
	}
	return nil
}

// GetShardedIndex returns the underlying sharded index if it exists.
func (idx *AutoShardingIndex) GetShardedIndex() *ShardedHNSW {
	idx.mu.RLock()
	curr := idx.current
	idx.mu.RUnlock()

	if s, ok := curr.(interface{ GetShardedIndex() *ShardedHNSW }); ok {
		return s.GetShardedIndex()
	}
	return nil
}

// GetGPUIndex returns the GPU-accelerated index if active.
func (idx *AutoShardingIndex) GetGPUIndex() any {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetGPUIndex()
}
