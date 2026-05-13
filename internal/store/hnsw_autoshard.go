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
	idx.mu.RUnlock()

	if sharded {
		return curr.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	}

	if interim != nil {
		// During migration, add to the NEW index directly
		return interim.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	}

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

// checkMigrationPressure monitors memory during active migration and throttles the caller
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

	usage := float64(m.HeapAlloc) / float64(maxMem)
	if usage > 0.85 {
		// Migration is happening and we are above 85% heap.
		// Slow down the caller to allow GC and migration loop to keep up.
		time.Sleep(5 * time.Millisecond)
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
	
	// Track migration in AdmissionController to apply tighter global limits
	if idx.dataset.Admission != nil {
		idx.dataset.Admission.MigrationStarted()
		defer idx.dataset.Admission.MigrationFinished()
	}

	// Promote newIndex to interimIndex so that AddBatch starts hitting it immediately
	idx.mu.Lock()
	idx.interimIndex = newIndex
	idx.mu.Unlock()

	// Migrate data in batches, releasing locks between items
	batchSize := 1000 // Increased to further reduce loop overhead
	lastMigrated := 0

	for {
		// 1. Memory Check: If heap usage > 80% (or 75% if already high), pause and trigger GC
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		
		maxMem := int64(0)
		if idx.dataset.Admission != nil {
			maxMem = idx.dataset.Admission.maxMemory.Load()
		}

		usage := float64(m.HeapAlloc)
		// Only trigger aggressive reclamation if we are actually under pressure (> 80%)
		if maxMem > 0 && usage > float64(maxMem)*0.80 {
			idx.dataset.Logger.Warn().
				Int64("usage_bytes", int64(usage)).
				Int64("max_bytes", maxMem).
				Msg("Migration paused: critical memory pressure")
			
			runtime.GC()
			debug.FreeOSMemory()
			
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// ... (reading state)
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
				if loc, ok := locAny.(Location); ok && loc.BatchIdx >= 0 && loc.BatchIdx < len(idx.dataset.Records.Read()) {
					rec := idx.dataset.Records.Read()[loc.BatchIdx]
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
				continue
			}
		}

		lastMigrated = endIdx

		// Incremental Handover: Release monolithic storage segments as they are replicated.
		if lastMigrated > 0 && lastMigrated%types.ChunkSize == 0 {
			if ah, ok := oldIndex.(interface{ ReleaseMonolithicChunk(int) }); ok {
				ah.ReleaseMonolithicChunk((lastMigrated / types.ChunkSize) - 1)
			}
		}

		// Frequent GC (every 10k) but NO FreeOSMemory unless pressured
		if lastMigrated%10000 == 0 {
			runtime.GC()
		}

		// Give other threads a window
		runtime.Gosched()
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

// SearchVectors returns the k nearest neighbors for a query vector.
func (idx *AutoShardingIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options any) ([]SearchResult, error) {
	idx.mu.RLock()
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	idx.mu.RUnlock()

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
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	idx.mu.RUnlock()

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
	sharded := idx.sharded
	interim := idx.interimIndex
	curr := idx.current
	idx.mu.RUnlock()

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
