package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
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
	mu      sync.RWMutex
	current VectorIndex
	config  AutoShardingConfig
	dataset *Dataset
	sharded bool
}

// NewAutoShardingIndex creates a new auto-sharding index.
// Initially, it uses a standard HNSWIndex.
func NewAutoShardingIndex(ds *Dataset, config AutoShardingConfig) *AutoShardingIndex {
	if config.ShardThreshold <= 0 {
		config.ShardThreshold = 10000 // Default to 10k
	}
	return &AutoShardingIndex{
		current: NewHNSWIndex(ds),
		config:  config,
		dataset: ds,
		sharded: false,
	}
}

// AddByLocation adds a vector to the index.
func (a *AutoShardingIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	a.mu.RLock()
	// Optimistic check: if not sharded, we might trigger migration *after* this add.
	// But simply calling add is safe.
	id, err := a.current.AddByLocation(batchIdx, rowIdx)
	currentLen := a.current.Len()
	sharded := a.sharded
	a.mu.RUnlock()

	if err != nil {
		return 0, err
	}

	// Check threshold synchronously
	if !sharded && currentLen >= a.config.ShardThreshold {
		a.migrateToSharded()
	}

	return id, nil
}

// AddByRecord adds a vector from a record batch.
func (a *AutoShardingIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	a.mu.RLock()
	id, err := a.current.AddByRecord(rec, rowIdx, batchIdx)
	currentLen := a.current.Len()
	sharded := a.sharded
	a.mu.RUnlock()

	if err != nil {
		return 0, err
	}

	if !sharded && currentLen >= a.config.ShardThreshold {
		a.migrateToSharded()
	}
	return id, nil
}

// migrateToSharded performs the migration from HNSWIndex to ShardedHNSW.
func (a *AutoShardingIndex) migrateToSharded() {
	start := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double check inside lock
	if a.sharded {
		return
	}
	if a.current.Len() < a.config.ShardThreshold {
		return
	}

	fmt.Println("Starting migration to ShardedHNSW...")

	// Verify we have a basic HNSWIndex
	oldIndex, ok := a.current.(*HNSWIndex)
	if !ok {
		// Should not happen unless initialized incorrectly or wrapped recursively
		fmt.Printf("Cannot migrate: current index is not *HNSWIndex, but %T\n", a.current)
		return
	}

	// Create new ShardedHNSW
	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.Metric = oldIndex.Metric
	if a.config.ShardCount > 0 {
		shardedConfig.NumShards = a.config.ShardCount
	}

	newIndex := NewShardedHNSW(shardedConfig, a.dataset)

	// Migrate data
	// Iterate through all vectors in old index
	// Iterate through all vectors in old index
	// NOTE: We assume IDs are contiguous [0, count).
	// If HNSWIndex supported deletes/gaps, this would need iteration over Graph nodes.
	// Current HNSW implementation uses monotonic IDs via nextVecID.

	// Better approach: Iterate the graph nodes just in case.
	// But HNSWIndex.locations is slice, accessing by ID.
	// nextVecID tracks max ID.
	// Let's iterate 0..nextVecID.
	// But `nextVecID` is essentially `Len()` if no deletes.
	// We'll iterate up to `len(oldIndex.locations)` to be safe.

	// We access locations directly via oldIndex.GetLocation
	// Wait, we need to iterate.
	// oldIndex.Len() returns count.
	// Safest is to loop over IDs. The locations slice grows with IDs.
	// Accessing locations via GetLocation is safe.

	// We can't easily access the length of locations slice from outside package (if unexported).
	// But we are in `package store`, so we can access unexported fields of HNSWIndex!
	// Yes, accessing `oldIndex.locations` is possible since we are in `package store`.

	n := oldIndex.Len()
	for id := 0; id < n; id++ {
		loc, ok := oldIndex.GetLocation(VectorID(id))
		if !ok {
			continue
		}
		// Check for empty/tombstone if applicable (Location{0,0} might be valid though).
		// HNSWIndex initializes locations slice locs.
		// If we support deletes later, we'd check for tombstone.
		// For now, blindly migrate.

		// Add to new index
		// Use AddByLocation which looks up data in dataset.
		// Ignore returned ID (we assume it preserves order 0..N)
		_, err := newIndex.AddByLocation(loc.BatchIdx, loc.RowIdx)
		if err != nil {
			fmt.Printf("Error migrating vector %d: %v\n", id, err)
			// Continue or abort?
			// Abort might leave us in weird state.
			// Log and continue best effort.
		}
	}

	// Swap
	a.current = newIndex
	a.sharded = true

	// Close old index to release resources if any (currently none for HNSWIndex except generic GC)
	_ = oldIndex.Close()

	duration := time.Since(start)
	metrics.IndexMigrationDuration.Observe(duration.Seconds())
	fmt.Printf("Migration complete. %d vectors moved to %d shards in %v.\n", n, shardedConfig.NumShards, duration)
}

// SearchVectors implements VectorIndex.
func (a *AutoShardingIndex) SearchVectors(query []float32, k int, filters []Filter) []SearchResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.current.SearchVectors(query, k, filters)
}

// SearchVectorsWithBitmap implements VectorIndex.
func (a *AutoShardingIndex) SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.current.SearchVectorsWithBitmap(query, k, filter)
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
