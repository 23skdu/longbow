package store

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
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
	if config.IndexConfig != nil {
		idx = NewArrowHNSW(ds, *config.IndexConfig, nil)
	} else {
		// Initialize HNSW config with dataset metric
		hnswConfig := DefaultConfig()
		hnswConfig.Metric = ds.Metric
		idx = NewHNSWIndex(ds, hnswConfig)
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
	if h, ok := a.current.(*HNSWIndex); ok {
		h.dimsOnce.Do(func() {
			h.dims = dim
		})
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
	fmt.Println("[DEBUG] migrateToSharded: Acquiring dataMu.RLock...")
	a.dataset.dataMu.RLock()
	fmt.Println("[DEBUG] migrateToSharded: Acquiring a.mu.Lock...")
	a.mu.Lock()
	fmt.Println("[DEBUG] migrateToSharded: Locks acquired.")
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

	fmt.Println("Starting migration to ShardedHNSW...")

	n := a.current.Len()

	// Verify we have a basic HNSWIndex
	oldIndex, ok := a.current.(*HNSWIndex)
	if !ok {
		// Should not happen unless initialized incorrectly or wrapped recursively
		fmt.Printf("Cannot migrate: current index is not *HNSWIndex, but %T\n", a.current)
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

	fmt.Printf("Migration started: %d vectors to move...\n", n)

	fmt.Printf("Migration started: %d vectors to move...\n", n)

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
				fmt.Printf("Error migrating vector %d: %v\n", it.id, err)
			}
		}

		lastMigrated = endIdx
		if lastMigrated%50 == 0 {
			fmt.Printf("Migration progress: %d/%d vectors...\n", lastMigrated, nSnap)
		}

		// Give other threads a window
		runtime.Gosched()
		// Small sleep to ensure fairness on high-core machines
		time.Sleep(10 * time.Microsecond)
	}

	totalMigrated := lastMigrated // For logging accuracy

	// Final swap
	fmt.Printf("[DEBUG] migrateToSharded: Starting final swap at %d, n=%d\n", totalMigrated, n)
	fmt.Println("[DEBUG] migrateToSharded: Acquiring a.mu.Lock for final swap...")
	a.mu.Lock()
	fmt.Println("[DEBUG] migrateToSharded: Acquiring a.dataset.dataMu.RLock for final swap...")
	a.dataset.dataMu.RLock()
	fmt.Println("[DEBUG] migrateToSharded: Locks acquired for final swap.")

	if a.sharded {
		fmt.Println("[DEBUG] migrateToSharded: Releasing a.dataset.dataMu.RUnlock for final swap (already sharded).")
		a.dataset.dataMu.RUnlock()
		fmt.Println("[DEBUG] migrateToSharded: Releasing a.mu.Unlock for final swap (already sharded).")
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
	fmt.Printf("Migration complete. %d vectors moved to %d shards in %v.\n", totalMigrated, shardedConfig.NumShards, duration)
}

// SearchVectors implements VectorIndex.
func (a *AutoShardingIndex) SearchVectors(query []float32, k int, filters []query.Filter) ([]SearchResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	sharded := a.sharded
	if sharded {
		return a.current.SearchVectors(query, k, filters)
	}

	interim := a.interimIndex
	res, err := a.current.SearchVectors(query, k, filters)
	if err != nil {
		return nil, err
	}

	if interim != nil {
		res2, err := interim.SearchVectors(query, k, filters)
		if err != nil {
			// Log error but return what we have
			fmt.Printf("Error searching interim index: %v\n", err)
			return res, nil
		}
		res = a.mergeSearchResults(res, res2, k)
	}

	return res, nil
}

// SearchVectorsWithBitmap implements VectorIndex.
func (a *AutoShardingIndex) SearchVectorsWithBitmap(query []float32, k int, filter *query.Bitset) []SearchResult {
	a.mu.RLock()
	defer a.mu.RUnlock()

	sharded := a.sharded
	if sharded {
		return a.current.SearchVectorsWithBitmap(query, k, filter)
	}

	interim := a.interimIndex
	res := a.current.SearchVectorsWithBitmap(query, k, filter)
	if interim != nil {
		res2 := interim.SearchVectorsWithBitmap(query, k, filter)
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

// Warmup delegates to the current index.
func (idx *AutoShardingIndex) Warmup() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.Warmup()
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
