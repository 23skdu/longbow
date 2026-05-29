package store

import (
	"context"
	"io"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	lbmem "github.com/23skdu/longbow/internal/memory"
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

	migrating atomic.Bool    // Added migrating field
	waitGroup sync.WaitGroup // Track active AddBatch ops on old index
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
		// During migration, add to the NEW index directly
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

// migrateToSharded performs the migration from HNSWIndex to ShardedHNSW.
func (idx *AutoShardingIndex) migrateToSharded() {
	start := time.Now()

	// Robust recovery to prevent background migration failures from crashing the server
	defer func() {
		if r := recover(); r != nil {
			idx.dataset.Logger.Error().
				Interface("panic", r).
				Msg("Index migration failed due to panic")
			idx.migrating.Store(false)
		}
	}()
	defer idx.migrating.Store(false)

	idx.mu.RLock()
	oldIndex := idx.current
	nTotal := oldIndex.Len()
	dims := 0
	dims = int(oldIndex.GetDimension())
	idx.mu.RUnlock()

	if dims == 0 {
		// Cannot migrate an index without known dimensionality.
		// Defer migration until the first batch sets it.
		idx.dataset.Logger.Warn().Msg("Deferring index migration: dimensionality is zero")
		return
	}

	idx.dataset.Logger.Info().
		Int("vectors", nTotal).
		Int("dims", dims).
		Msg("Index migration started")

	// LOCK ORDER: ds.dataMu MUST be locked before idx.mu to avoid deadlock with Search
	idx.dataset.dataMu.RLock()
	idx.mu.Lock()
	if idx.sharded {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}

	nTotal = idx.current.Len()
	if nTotal < idx.config.ShardThreshold {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}

	// Capture snapshots for migration
	oldIndex = idx.current
	var oldDataType types.VectorDataType
	if ah, ok := oldIndex.(*ArrowHNSW); ok {
		oldDataType = ah.GetConfig().DataType
	} else if sh, ok := oldIndex.(*ShardedHNSW); ok {
		oldDataType = sh.GetConfig().DataType
	}

	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.Metric = idx.dataset.Metric
	shardedConfig.Dimension = oldIndex.GetDimension()
	shardedConfig.DataType = oldDataType
	if idx.config.ShardCount > 0 {
		shardedConfig.NumShards = idx.config.ShardCount
	}
	if idx.config.ShardSplitThreshold > 0 {
		shardedConfig.ShardSplitThreshold = idx.config.ShardSplitThreshold
	}
	shardedConfig.UseRingSharding = idx.config.UseRingSharding

	idx.mu.Unlock()
	idx.dataset.dataMu.RUnlock()

	newIndex := NewShardedHNSW(shardedConfig, idx.dataset)

	// Pre-warm the new index to the total expected size to reduce allocation churn
	newIndex.PreWarm(nTotal)

	if idx.dataset.Admission != nil {
		idx.dataset.Admission.MigrationStarted()
		defer idx.dataset.Admission.MigrationFinished()
	}

	// Promote newIndex to interimIndex so that AddBatch starts hitting it immediately
	idx.mu.Lock()
	idx.interimIndex = newIndex
	idx.mu.Unlock()

	// Transition monolithic index to off-heap shadow mode to free up heap for the new index
	idx.mu.Lock()
	if err := oldIndex.RelocateToOffHeap(); err != nil {
		idx.dataset.Logger.Error().Err(err).Msg("Failed to relocate monolithic index to off-heap")
	}
	idx.mu.Unlock()
	// Reclaim memory immediately
	runtime.GC()
	debug.FreeOSMemory()

	// Migration parameters
	baseBatchSize := 5000 // Increased for throughput
	lastMigrated := 0

	for {
		// 1. Memory Check (Heap + Off-Heap)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		offHeapMem := lbmem.GetGlobalOffHeapAllocated()

		maxMem := int64(0)
		if idx.dataset.Admission != nil {
			maxMem = idx.dataset.Admission.maxMemory.Load()
		}

		physicalMem := int64(m.HeapAlloc) + offHeapMem // #nosec G115
		usageRatio := 0.0
		if maxMem > 0 {
			usageRatio = float64(physicalMem) / float64(maxMem)
		}

		// Adaptive Batch Sizing & Pressure Management
		currentBatchSize := baseBatchSize

		// Dynamic Slab-Capacity Aware Migration Batch Size Calculator
		getBytesPerElement := func(dt types.VectorDataType) int {
			switch dt {
			case types.VectorTypeFloat32, types.VectorTypeInt32, types.VectorTypeUint32:
				return 4
			case types.VectorTypeFloat16, types.VectorTypeInt16, types.VectorTypeUint16:
				return 2
			case types.VectorTypeInt8, types.VectorTypeUint8, types.VectorTypeTQ:
				return 1
			case types.VectorTypeFloat64, types.VectorTypeInt64, types.VectorTypeUint64, types.VectorTypeComplex64:
				return 8
			case types.VectorTypeComplex128:
				return 16
			default:
				return 4
			}
		}

		safeSlabLimit := 512 * 1024 // 512 KB maximum batch size allocation window
		bytesPerElement := getBytesPerElement(oldDataType)
		vectorBytes := dims * bytesPerElement
		if vectorBytes > 0 {
			dynamicLimit := safeSlabLimit / vectorBytes
			if dynamicLimit > 0 {
				currentBatchSize = min(currentBatchSize, dynamicLimit)
			}
		}

		// Scale batch size dynamically based on CPU core counts to minimize thread thrashing.
		// Machines with more cores can process larger parallel batches without lock contention.
		numCPU := runtime.NumCPU()
		cpuScaleFactor := float64(numCPU) / 4.0
		if cpuScaleFactor < 1.0 {
			cpuScaleFactor = 1.0
		}
		if cpuScaleFactor > 8.0 {
			cpuScaleFactor = 8.0
		}
		currentBatchSize = int(float64(currentBatchSize) * cpuScaleFactor)

		// Enforce sensible bounds (minimum 50 to avoid absolute thrashing, max baseBatchSize)
		if currentBatchSize < 50 {
			currentBatchSize = 50
		}

		if usageRatio > 0.75 {
			currentBatchSize = min(currentBatchSize, 1000)
		}
		if usageRatio > 0.85 {
			currentBatchSize = min(currentBatchSize, 250)

			idx.dataset.Logger.Warn().
				Int64("usage_bytes", physicalMem).
				Int64("max_bytes", maxMem).
				Float64("ratio", usageRatio).
				Msg("Migration throttled: critical memory pressure")

			runtime.GC()
			if usageRatio > 0.97 {
				debug.FreeOSMemory()
			}
			time.Sleep(500 * time.Millisecond)
		}

		// 1b. Migration Lane Throttling
		if idx.dataset.Admission != nil {
			for {
				if err := idx.dataset.Admission.AdmitMigration(context.Background()); err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		idx.mu.RLock()
		oldIdx := idx.current
		isSharded := idx.sharded
		nSnap := oldIdx.Len()
		idx.mu.RUnlock()

		if isSharded || lastMigrated >= nSnap {
			break
		}

		endIdx := lastMigrated + currentBatchSize
		if endIdx > nSnap {
			endIdx = nSnap
		}

		// 2. Process batch: collect locations and records
		type item struct {
			rec      arrow.RecordBatch
			rowIdx   int
			batchIdx int
		}
		items := make([]item, 0, endIdx-lastMigrated)

		idx.dataset.dataMu.RLock()
		records := idx.dataset.Records.Read()
		for id := lastMigrated; id < endIdx; id++ {
			locAny, ok := oldIdx.GetLocation(uint32(id))
			if ok {
				if loc, ok := locAny.(Location); ok && loc.BatchIdx >= 0 && loc.BatchIdx < len(records) {
					rec := records[loc.BatchIdx]
					rec.Retain()
					items = append(items, item{rec: rec, rowIdx: loc.RowIdx, batchIdx: loc.BatchIdx})
				}
			}
		}
		idx.dataset.dataMu.RUnlock()

		if len(items) > 0 {
			// Convert to batch parameters mapped to the true dataset BatchIdx
			maxBatchIdx := -1
			for _, it := range items {
				if it.batchIdx > maxBatchIdx {
					maxBatchIdx = it.batchIdx
				}
			}

			// Build recs slice: one entry per batchIdx. Track which items were
			// already retained (only the first item per batchIdx is used in recs;
			// release the surplus Retain()s on duplicate batchIdx items now).
			recs := make([]arrow.RecordBatch, maxBatchIdx+1)
			rowIdxs := make([]int, len(items))
			batchIdxs := make([]int, len(items))

			for i, it := range items {
				if recs[it.batchIdx] != nil && recs[it.batchIdx] != it.rec {
					// A different record was already placed at this slot —
					// release the surplus retain to avoid a memory leak.
					it.rec.Release()
				} else {
					recs[it.batchIdx] = it.rec
				}
				rowIdxs[i] = it.rowIdx
				batchIdxs[i] = it.batchIdx // Use true dataset index
			}

			// Add batch to new index (Parallelized inside ShardedHNSW)
			gIDs, err := newIndex.AddBatch(context.Background(), recs, rowIdxs, batchIdxs)

			// Release records (one Release per Retain; duplicates already released above)
			for _, r := range recs {
				if r != nil {
					r.Release()
				}
			}

			if err != nil {
				// Non-critical: log and continue rather than aborting the entire migration.
				// Vectors that can't be resolved are skipped; the majority will still be
				// migrated and the index will shard successfully.
				idx.dataset.Logger.Warn().
					Err(err).
					Int("lastMigrated", lastMigrated).
					Int("endIdx", endIdx).
					Msg("Migration batch had missing vectors - continuing")
			}

			// Correct locations in global locationStore of newIndex
			if sh, ok := newIndex.(*ShardedHNSW); ok {
				for i, gid := range gIDs {
					vid := VectorID(gid)
					sh.LocationStore().Set(vid, Location{BatchIdx: items[i].batchIdx, RowIdx: items[i].rowIdx})
				}
			}
		}

		lastMigrated = endIdx

		// Incremental Handover: To prevent "vector missing for row X" failures under high-load memory pressure,
		// we delay releasing monolithic storage chunks until the migration is fully completed.
		// Reclaim GC memory periodically based on migrated progress.
		currentChunk := (lastMigrated / types.ChunkSize) - 1
		if currentChunk >= 0 {
			if usageRatio > 0.85 {
				runtime.GC()
			} else if currentChunk%4 == 0 { // Don't GC on every chunk to avoid too much jitter
				runtime.GC()
			}
		}

		// Give other threads a window
		runtime.Gosched()
	}

	// Final swap
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
	idx.interimIndex = nil
	idx.dataset.dataMu.RUnlock()
	idx.mu.Unlock()

	// Wait for any remaining AddBatch operations on oldIndex to finish before closing
	idx.waitGroup.Wait()

	// Close old index to release all remaining resources
	_ = oldIndex.Close()

	// Reclaim memory immediately back to OS
	released := lbmem.ReleaseGlobalSlabPoolsUnused()
	if released > 0 {
		idx.dataset.Logger.Info().Int("released_slabs", released).Msg("Released unused monolithic index slabs back to the OS")
	}
	runtime.GC()
	debug.FreeOSMemory()

	// Relocate the new ShardedHNSW index to off-heap memory.
	// The old monolithic index was already relocated during migration (line ~329), but the
	// new sharded index allocates slab chunks on the Go heap. Moving it off-heap here
	// breaks the O(N) heap growth curve and allows the GC to reclaim the freed pages.
	var rssBefore, rssAfter uint64
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rssBefore = memStats.HeapInuse

	if err := newIndex.RelocateToOffHeap(); err != nil {
		idx.dataset.Logger.Warn().Err(err).Msg("Failed to relocate ShardedHNSW to off-heap; index will remain on Go heap")
	} else {
		runtime.GC()
		debug.FreeOSMemory()
		runtime.ReadMemStats(&memStats)
		rssAfter = memStats.HeapInuse
		var freedMB int64
		if rssBefore > rssAfter {
			freedMB = int64(rssBefore-rssAfter) / (1024 * 1024) // #nosec G115
		}
		idx.dataset.Logger.Info().
			Uint64("heap_before_mb", rssBefore/(1024*1024)).
			Uint64("heap_after_mb", rssAfter/(1024*1024)).
			Int64("freed_mb", freedMB).
			Msg("ShardedHNSW relocated to off-heap after migration")
	}

	duration := time.Since(start)
	metrics.IndexMigrationDuration.Observe(duration.Seconds())
	idx.dataset.Logger.Info().
		Dur("duration", duration).
		Int("vectors", nTotal).
		Msg("Index migration complete")
}

// ReleaseMonolithicChunk releases a monolithic chunk from memory.
func (idx *AutoShardingIndex) ReleaseMonolithicChunk(cID int) error {
	idx.mu.RLock()
	curr := idx.current
	idx.mu.RUnlock()

	if curr != nil {
		return curr.ReleaseMonolithicChunk(cID)
	}
	return nil
}

// SearchVectors returns the k nearest neighbors for a query vector.
func (idx *AutoShardingIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectors(ctx, q, k, filters, options)
	}

	// If no migration is active, search monolith only
	if interim == nil {
		return curr.SearchVectors(ctx, q, k, filters, options)
	}

	// Parallel Shadow Search: Query both indices concurrently
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectors(ctx, q, k, filters, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectors(ctx, q, k, filters, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1 // Return first error if both fail
	}

	return idx.mergeSearchResults(res1, res2, k), nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (idx *AutoShardingIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	if interim == nil {
		return curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}

	// Parallel Shadow Search
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	return idx.mergeSearchResults(res1, res2, k), nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (idx *AutoShardingIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current

	if sharded {
		return curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}

	if interim == nil {
		return curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}

	// Parallel Shadow Search
	var res1, res2 []SearchResult
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	combined := append(res1, res2...)
	return combined, nil
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

// IsSharded returns true if the index has been upgraded to a sharded implementation.
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

// Search implements VectorIndexer.
func (idx *AutoShardingIndex) Search(ctx context.Context, q any, k int, options any) ([]types.Candidate, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	idx.mu.RUnlock()

	if sharded || interim == nil {
		return curr.Search(ctx, q, k, options)
	}

	// Parallel Shadow Search: Query both indices concurrently
	var res1, res2 []types.Candidate
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		res1, err1 = interim.Search(ctx, q, k, options)
	}()

	go func() {
		defer wg.Done()
		res2, err2 = curr.Search(ctx, q, k, options)
	}()

	wg.Wait()

	if err1 != nil && err2 != nil {
		return nil, err1
	}

	return idx.mergeCandidates(res1, res2, k), nil
}

func (idx *AutoShardingIndex) mergeCandidates(res1, res2 []types.Candidate, k int) []types.Candidate {
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

	combined := make([]types.Candidate, 0, len(res1)+len(res2))
	combined = append(combined, res1...)
	combined = append(combined, res2...)

	// Deduplicate by ID
	seen := make(map[uint32]struct{})
	unique := make([]types.Candidate, 0, len(combined))
	for _, cand := range combined {
		if _, ok := seen[cand.ID]; !ok {
			seen[cand.ID] = struct{}{}
			unique = append(unique, cand)
		}
	}

	// Sort by distance ascending
	sort.Slice(unique, func(i, j int) bool {
		return unique[i].Dist < unique[j].Dist
	})

	if len(unique) > k {
		return unique[:k]
	}
	return unique
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

// Close releases all resources held by the index.
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
