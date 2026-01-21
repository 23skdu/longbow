package store

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
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
func NewAutoShardingIndex(ds *Dataset, config AutoShardingConfig) *AutoShardingIndex {
	if config.ShardThreshold <= 0 {
		config.ShardThreshold = 10000 // Default to 10k
	}

	var idx VectorIndex
	switch {
	case config.IndexConfig != nil:
		idx = NewArrowHNSW(ds, *config.IndexConfig, nil)
	case ds.UseHNSW2():
		// Use HNSW2 default config if enabled
		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.Metric = ds.Metric
		idx = NewArrowHNSW(ds, hnswConfig, nil)
	default:
		// Use ArrowHNSW as default for better performance (parallelism, batching)
		hnswConfig := DefaultArrowHNSWConfig()
		hnswConfig.Metric = ds.Metric
		idx = NewArrowHNSW(ds, hnswConfig, nil)
	}

	return &AutoShardingIndex{
		current: idx,
		config:  config,
		dataset: ds,
		sharded: false,
	}
}

// SetInitialDimension sets the dimension for the underlying index if not yet set.
func (a *AutoShardingIndex) SetInitialDimension(dim int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch h := a.current.(type) {
	case *HNSWIndex:
		h.dimsOnce.Do(func() {
			h.dims = dim
		})
	case *ArrowHNSW:
		h.SetDimension(dim)
	}
}

// AddByLocation adds a vector to the index.
func (a *AutoShardingIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	// Lock held throughout execution to prevent Close during migration
	a.mu.RLock()
	sharded := a.sharded
	interim := a.interimIndex
	curr := a.current

	if sharded {
		id, err := curr.AddByLocation(batchIdx, rowIdx)
		a.mu.RUnlock()
		return id, err
	}

	if interim != nil {
		id, err := interim.AddByLocation(batchIdx, rowIdx)
		a.mu.RUnlock()
		return id, err
	}

	id, err := curr.AddByLocation(batchIdx, rowIdx)
	a.mu.RUnlock()

	if err == nil {
		a.checkShardThreshold()
	}
	return id, err
}

// AddByRecord implementation to support interim index.
func (a *AutoShardingIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	a.mu.RLock()
	sharded := a.sharded
	interim := a.interimIndex
	curr := a.current

	if sharded {
		id, err := curr.AddByRecord(rec, rowIdx, batchIdx)
		a.mu.RUnlock()
		return id, err
	}

	if interim != nil {
		id, err := interim.AddByRecord(rec, rowIdx, batchIdx)
		a.mu.RUnlock()
		return id, err
	}

	id, err := curr.AddByRecord(rec, rowIdx, batchIdx)
	a.mu.RUnlock()

	if err == nil {
		a.checkShardThreshold()
	}
	return id, err
}

// AddBatch adds multiple vectors from multiple record batches efficiently.
func (a *AutoShardingIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	a.mu.RLock()
	sharded := a.sharded
	interim := a.interimIndex
	curr := a.current

	if sharded {
		ids, err := curr.AddBatch(recs, rowIdxs, batchIdxs)
		a.mu.RUnlock()
		return ids, err
	}

	if interim != nil {
		// During migration, add to the NEW index directly
		ids, err := interim.AddBatch(recs, rowIdxs, batchIdxs)
		a.mu.RUnlock()
		return ids, err
	}

	ids, err := curr.AddBatch(recs, rowIdxs, batchIdxs)
	a.mu.RUnlock()

	if err == nil {
		a.checkShardThreshold()
	}
	return ids, err
}

func (a *AutoShardingIndex) checkShardThreshold() {
	a.mu.RLock()
	if a.sharded || !a.config.Enabled {
		a.mu.RUnlock()
		return
	}
	currentLen := a.current.Len()
	threshold := a.config.ShardThreshold
	a.mu.RUnlock()

	if currentLen >= threshold {
		// Only trigger migration if not already sharded and not already migrating
		// AND if the current index is of a type that supports migration (HNSWIndex only for now)
		if _, ok := a.current.(*HNSWIndex); !ok {
			return
		}

		if !a.sharded && a.migrating.CompareAndSwap(false, true) {
			go a.migrateToSharded()
		}
	}
}

// migrateToSharded performs the migration from HNSWIndex to ShardedHNSW.
func (a *AutoShardingIndex) migrateToSharded() {
	defer a.migrating.Store(false) // Ensure migrating flag is reset on exit
	start := time.Now()

	// LOCK ORDER: ds.dataMu MUST be locked before a.mu to avoid deadlock with Search
	a.dataset.dataMu.RLock()
	a.mu.Lock()
	if a.sharded {
		a.mu.Unlock()
		a.dataset.dataMu.RUnlock()
		return
	}
	if a.current.Len() < a.config.ShardThreshold {
		a.mu.Unlock()
		a.dataset.dataMu.RUnlock()
		return
	}

	// Starting migration

	// Verify we have a basic HNSWIndex
	oldIndex, ok := a.current.(*HNSWIndex)
	if !ok {
		// Should not happen unless initialized incorrectly or wrapped recursively
		// Silent failure
		a.mu.Unlock()
		a.dataset.dataMu.RUnlock()
		return
	}

	// Create new ShardedHNSW config
	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.Metric = oldIndex.Metric
	shardedConfig.Dimension = oldIndex.GetDimension()
	if a.config.ShardCount > 0 {
		shardedConfig.NumShards = a.config.ShardCount
	}
	if a.config.ShardSplitThreshold > 0 {
		shardedConfig.ShardSplitThreshold = a.config.ShardSplitThreshold
	}
	shardedConfig.UseRingSharding = a.config.UseRingSharding // Propagate ring sharding setting

	// IMPORTANT: Unlock here! We have captured our snapshots (oldIndex, n, shardedConfig).
	// We can now create the new index and run the migration without holding these global locks.
	a.mu.Unlock()
	a.dataset.dataMu.RUnlock()

	newIndex := NewShardedHNSW(shardedConfig, a.dataset)

	// Promote newIndex to interimIndex so that AddBatch starts hitting it immediately
	a.mu.Lock()
	a.interimIndex = newIndex
	a.mu.Unlock()

	// Migrate data in batches, releasing locks between items
	batchSize := 50
	lastMigrated := 0

	// Migration started

	for {
		// Read state under lock
		a.mu.RLock()
		oldIdx := a.current
		isSharded := a.sharded
		nSnap := oldIdx.Len()
		a.mu.RUnlock()

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

		a.dataset.dataMu.RLock()
		for id := lastMigrated; id < endIdx; id++ {
			vid := VectorID(id)
			loc, ok := oldIdx.GetLocation(vid)
			if ok && loc.BatchIdx < len(a.dataset.Records) {
				rec := a.dataset.Records[loc.BatchIdx]
				rec.Retain()
				items = append(items, item{rec: rec, loc: loc, id: vid})
			}
		}
		a.dataset.dataMu.RUnlock()

		// Perform expensive additions outside dataMu
		for _, it := range items {
			_, err := newIndex.AddByRecord(it.rec, it.loc.RowIdx, it.loc.BatchIdx)
			it.rec.Release()
			if err != nil {
				// migration skip
			}
		}

		lastMigrated = endIdx
		if lastMigrated%1000 == 0 {
			// progress
		}

		// Give other threads a window
		runtime.Gosched()
		// Small sleep to ensure fairness on high-core machines
		time.Sleep(10 * time.Microsecond)
	}

	// Final swap

	// LOCK ORDER: ds.dataMu MUST be locked before a.mu to avoid deadlock with Search
	a.dataset.dataMu.RLock()
	a.mu.Lock()

	if a.sharded {
		a.dataset.dataMu.RUnlock()
		a.mu.Unlock()
		return
	}

	// Swap
	a.current = newIndex
	a.sharded = true
	a.interimIndex = nil // Clear interim index

	// Close old index to release resources
	_ = oldIndex.Close()

	a.dataset.dataMu.RUnlock()
	a.mu.Unlock()

	duration := time.Since(start)
	metrics.IndexMigrationDuration.Observe(duration.Seconds())
}

// SearchVectors implements VectorIndex.
func (a *AutoShardingIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	sharded := a.sharded
	if sharded {
		return a.current.SearchVectors(ctx, q, k, filters, options)
	}

	interim := a.interimIndex
	res, err := a.current.SearchVectors(ctx, q, k, filters, options)
	if err != nil {
		return nil, err
	}

	if interim != nil {
		res2, err := interim.SearchVectors(ctx, q, k, filters, options)
		if err != nil {
			// Log error but return what we have
			return res, nil
		}
		res = a.mergeSearchResults(res, res2, k)
	}

	return res, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (a *AutoShardingIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *query.Bitset, options SearchOptions) []SearchResult {
	a.mu.RLock()
	defer a.mu.RUnlock()

	sharded := a.sharded
	if sharded {
		return a.current.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	interim := a.interimIndex
	res := a.current.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	if interim != nil {
		res2 := interim.SearchVectorsWithBitmap(ctx, q, k, filter, options)
		res = a.mergeSearchResults(res, res2, k)
	}

	return res
}

func (a *AutoShardingIndex) mergeSearchResults(res1, res2 []SearchResult, k int) []SearchResult {
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

// Len implements VectorIndex.
func (a *AutoShardingIndex) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.current.Len()
}

// GetDimension implements VectorIndex.
func (a *AutoShardingIndex) GetDimension() uint32 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.current.GetDimension()
}

// SetEfConstruction updates the efConstruction parameter dynamically.
func (a *AutoShardingIndex) SetEfConstruction(ef int) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Update current index if supported
	if h, ok := a.current.(interface{ SetEfConstruction(int) }); ok {
		h.SetEfConstruction(ef)
	}

	// Update interim index if exists
	if a.interimIndex != nil {
		if h, ok := a.interimIndex.(interface{ SetEfConstruction(int) }); ok {
			h.SetEfConstruction(ef)
		}
	}
}

func (a *AutoShardingIndex) TrainPQ(vectors [][]float32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current.TrainPQ(vectors)
}

func (a *AutoShardingIndex) GetPQEncoder() *pq.PQEncoder {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.current.GetPQEncoder()
}

// SetIndexedColumns configures which columns are indexed for fast equality lookups
func (idx *AutoShardingIndex) SetIndexedColumns(cols []string) {
	// No-op for now, or delegate if underlying supports it
}

// GetLocation retrieves the storage location for a given vector ID.
func (idx *AutoShardingIndex) GetLocation(id VectorID) (Location, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetLocation(id)
}

// GetVectorID retrieves the vector ID for a given storage location.
func (idx *AutoShardingIndex) GetVectorID(loc Location) (VectorID, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetVectorID(loc)
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

// GetNeighbors returns the nearest neighbors for a given vector ID.
func (idx *AutoShardingIndex) GetNeighbors(id VectorID) ([]VectorID, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Primarily check current index
	neighbors, err := idx.current.GetNeighbors(id)
	if err == nil {
		return neighbors, nil
	}

	// If not found and merging, check interim
	if idx.interimIndex != nil {
		return idx.interimIndex.GetNeighbors(id)
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
