package engine

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/gpu"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
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
		ShardThreshold:      256,
		ShardCount:          runtime.NumCPU(),
		UseRingSharding:     true,
		ShardSplitThreshold: 65536,
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

	migrating        atomic.Bool    // Added migrating field
	waitGroup        sync.WaitGroup // Track active AddBatch ops on old index
	migrationPause   chan struct{}  // Blocks ingestion AddBatch during migration
	migrationPauseMu sync.Mutex     // Protects migrationPause
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

	if ds.EvictionManager != nil {
		if ah, ok := idx.(*ArrowHNSW); ok {
			ds.EvictionManager.Register(ah.GetData())
		}
	}

	return &AutoShardingIndex{
		current: idx,
		config:  config,
		dataset: ds,
		sharded: false,
	}
}

// SetInitialDimension sets the dimension for the underlying index if not yet set.
func (idx *AutoShardingIndex) SetInitialDimension(dim int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if h, ok := idx.current.(*ArrowHNSW); ok {
		_ = h.SetDimension(int(dim))
	}
}

// AddByLocation adds a vector to the index using its storage location.
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

	if idx.migrating.Load() {
		idx.checkMigrationPressure()
	}
	return id, err
}

// AddByRecord adds a vector from an Arrow record batch.
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

	if idx.migrating.Load() {
		idx.checkMigrationPressure()
	}
	return id, err
}

// waitForMigrationPause blocks until the in-progress migration finishes,
// or the context is cancelled. This prevents ingestion workers from contending
// on shard locks with the migration loop.
func (idx *AutoShardingIndex) waitForMigrationPause(ctx context.Context) {
	idx.migrationPauseMu.Lock()
	ch := idx.migrationPause
	idx.migrationPauseMu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
		}
	}
}

// AddBatch adds multiple vectors from Arrow record batches efficiently.
func (idx *AutoShardingIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	if !sharded && interim == nil {
		idx.waitGroup.Add(1)
	}
	idx.mu.RUnlock()

	if sharded {
		return curr.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	}

	if interim != nil {
		// During migration, wait for it to finish before adding to the interim index.
		// This prevents lock contention between the migration loop and ingestion workers
		// on the new sharded index's per-shard locks.
		idx.waitForMigrationPause(ctx)
		return interim.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	}

	defer idx.waitGroup.Done()
	ids, err := curr.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	if err == nil {
		idx.checkShardThreshold()
	}

	if idx.migrating.Load() {
		idx.checkMigrationPressure()
	}
	return ids, err
}

func (idx *AutoShardingIndex) checkShardThreshold() {
	idx.mu.RLock()
	curr := idx.current
	sharded := idx.sharded
	migrating := idx.migrating.Load()
	idx.mu.RUnlock()

	if sharded || migrating {
		return
	}

	currentLen := curr.Len()
	threshold := idx.config.ShardThreshold

	if currentLen >= threshold {
		if idx.config.Enabled && idx.migrating.CompareAndSwap(false, true) {
			idx.dataset.Logger.Info().
				Int("current_len", currentLen).
				Int("threshold", threshold).
				Msg("Triggering auto-sharding migration")
			go idx.migrateToSharded()
		}
	}
}

// checkMigrationPressure monitors memory during active migration and throttles the caller.
// It accounts for both Go heap and off-heap arena memory.
func (idx *AutoShardingIndex) checkMigrationPressure() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	var maxMem int64
	if idx.dataset.Admission != nil {
		maxMem = idx.dataset.Admission.maxMemory.Load()
	}
	if maxMem <= 0 {
		return
	}

	// Calculate physical memory (Heap + Off-Heap Arenas)
	offHeapMem := lbmem.GetGlobalOffHeapAllocated()

	usage := float64(int64(m.HeapAlloc)+offHeapMem) / float64(maxMem) // #nosec G115
	if usage > 0.85 {
		// Migration is happening and we are above 85% total memory limit.
		// Slow down the caller to allow GC and migration loop to keep up.
		time.Sleep(10 * time.Millisecond)
		if usage > 0.92 {
			runtime.GC()
		}
	}
}

// IsSharded reports whether the condition holds.
func (idx *AutoShardingIndex) IsSharded() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.sharded
}

// GetIndexType returns the index type identifier for the active index.
func (idx *AutoShardingIndex) GetIndexType() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.current.GetIndexType()
}

// Len returns the total number of vectors across all shards.
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

// SetIndexedColumns configures which columns are indexed for fast equality lookups.
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

// Warmup implements VectorIndexer.
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

// InitGPU initializes GPU acceleration on the underlying HNSW index.
func (idx *AutoShardingIndex) InitGPU(deviceID int32, logger zerolog.Logger) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if h, ok := idx.current.(*ArrowHNSW); ok {
		return h.InitGPU(deviceID, logger)
	}
	return nil
}

// SetGPUIndex assigns a GPU index to the underlying HNSW index.
func (idx *AutoShardingIndex) SetGPUIndex(gIdx any) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if h, ok := idx.current.(*ArrowHNSW); ok {
		if gi, ok := gIdx.(gpu.Index); ok {
			h.SetGPUIndex(gi)
		}
	}
}

// Close releases all resources held by the index.
func (idx *AutoShardingIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	err := idx.current.Close()
	if idx.interimIndex != nil {
		if ierr := idx.interimIndex.Close(); ierr != nil && err == nil {
			err = ierr
		}
		idx.interimIndex = nil
	}
	return err
}

// EstimateMemory returns the diagnostic neighbor IDs for a given vector ID.
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

// GetData serializes the current index state.
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

// RelocateToOffHeap relocates the index to off-heap memory.
func (idx *AutoShardingIndex) RelocateToOffHeap() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if err := idx.current.RelocateToOffHeap(); err != nil {
		return err
	}
	if idx.interimIndex != nil {
		return idx.interimIndex.RelocateToOffHeap()
	}
	return nil
}
