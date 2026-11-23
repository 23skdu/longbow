package hnsw

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
)

// =============================================================================
// AdaptiveIndexConfig - Configuration for adaptive index switching
// =============================================================================

// AdaptiveIndexConfig controls automatic switching between BruteForce and HNSW.
type AdaptiveIndexConfig struct {
	// Threshold is the number of vectors at which to switch from BruteForce to HNSW.
	Threshold int

	// Enabled controls whether adaptive indexing is active.
	Enabled bool
}

// DefaultAdaptiveIndexConfig returns sensible defaults for adaptive indexing.
func DefaultAdaptiveIndexConfig() AdaptiveIndexConfig {
	return AdaptiveIndexConfig{
		Threshold: 1000,
		Enabled:   true,
	}
}

// Validate checks that the configuration is valid.
func (c AdaptiveIndexConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.Threshold <= 0 {
		return errors.New("adaptive index threshold must be positive when enabled")
	}
	return nil
}

// =============================================================================
// AdaptiveIndex - Wrapper that switches between BruteForce and HNSW
// =============================================================================

// AdaptiveIndex automatically switches between BruteForce and HNSW based on size.
type AdaptiveIndex struct {
	mu             sync.RWMutex
	dataset        types.IndexDataProvider
	config         AdaptiveIndexConfig
	bruteForce     VectorIndex
	hnsw           VectorIndex
	usingHNSW      atomic.Bool
	migrating      atomic.Bool
	migrationCount atomic.Int64
	vectorCount    atomic.Int64
}

// NewAdaptiveIndex creates an adaptive index starting with BruteForce.
func NewAdaptiveIndex(ds types.IndexDataProvider, cfg AdaptiveIndexConfig) VectorIndex {
	bf := NewBruteForceIndex(ds)
	a := &AdaptiveIndex{
		dataset:    ds,
		config:     cfg,
		bruteForce: bf,
	}
	return a
}

// activeIndex returns the currently active underlying index.
// Callers must hold at least idx.mu.RLock.
func (idx *AdaptiveIndex) activeIndex() VectorIndex {
	if idx.usingHNSW.Load() {
		return idx.hnsw
	}
	return idx.bruteForce
}

// IsSharded returns true if the adaptive index is currently using a sharded HNSW index.
func (idx *AdaptiveIndex) IsSharded() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.hnsw != nil {
		return idx.hnsw.IsSharded()
	}
	return false
}

// ReleaseMonolithicChunk releases a monolithic chunk from memory.
func (idx *AdaptiveIndex) ReleaseMonolithicChunk(cID int) error {
	// AdaptiveIndex delegates if needed, but usually not used for BruteForce
	return nil
}

// =============================================================================
// BruteForceIndex - Linear scan index for small datasets
// =============================================================================

// BruteForceIndex implements VectorIndex using linear scan O(N) search.
// AdaptiveIndex methods

// AddByLocation adds a vector to the adaptive index, triggering migration if needed.
func (idx *AdaptiveIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	idx.mu.RLock()
	hnsw := idx.hnsw
	idx.mu.RUnlock()

	if hnsw != nil {
		return hnsw.AddByLocation(ctx, batchIdx, rowIdx)
	}

	start := time.Now()
	idx.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()

	// Double-check hnsw under the lock to avoid race during migration
	if idx.hnsw != nil {
		return idx.hnsw.AddByLocation(ctx, batchIdx, rowIdx)
	}

	if idx.bruteForce == nil {
		return 0, errors.New("index is closed or uninitialized")
	}

	id, err := idx.bruteForce.AddByLocation(ctx, batchIdx, rowIdx)
	if err == nil {
		newCount := idx.vectorCount.Add(1)
		if idx.config.Enabled && newCount >= int64(idx.config.Threshold) && idx.hnsw == nil && !idx.usingHNSW.Load() {
			idx.migrateToHNSW()
		}
	}
	return id, err
}

// AddByRecord adds a vector from a record batch.
func (idx *AdaptiveIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return idx.AddByLocation(ctx, batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (idx *AdaptiveIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	idx.mu.RLock()
	hnsw := idx.hnsw
	usingHNSW := idx.usingHNSW.Load()
	idx.mu.RUnlock()

	if hnsw != nil {
		ids, err := hnsw.AddBatch(ctx, recs, rowIdxs, batchIdxs)
		if err != nil {
			return nil, err
		}

		// If we are still migrating, also add to bruteForce to maintain searchability
		if !usingHNSW {
			idx.mu.Lock()
			if idx.bruteForce != nil {
				for i := range rowIdxs {
					_, _ = idx.bruteForce.AddByLocation(ctx, batchIdxs[i], rowIdxs[i])
				}
			}
			idx.mu.Unlock()
		}
		return ids, nil
	}

	start := time.Now()
	idx.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()

	// Double-check hnsw under the lock to avoid race during migration
	if idx.hnsw != nil {
		return idx.hnsw.AddBatch(ctx, recs, rowIdxs, batchIdxs)
	}

	if idx.bruteForce == nil {
		return nil, errors.New("index is closed or uninitialized")
	}

	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		id, err := idx.bruteForce.AddByLocation(ctx, batchIdxs[i], rowIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}

	newCount := idx.vectorCount.Add(int64(len(rowIdxs)))
	if idx.config.Enabled && newCount >= int64(idx.config.Threshold) && idx.hnsw == nil && !idx.usingHNSW.Load() {
		idx.migrateToHNSW()
	}

	return ids, nil
}

// SearchVectorsWithBitmap finds k-nearest neighbors using the active index implementation.
func (idx *AdaptiveIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}
	if idx.bruteForce != nil {
		return idx.bruteForce.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}
	return nil, nil
}

// GetLocation retrieves the storage location for a given vector ID.
func (idx *AdaptiveIndex) GetLocation(id uint32) (any, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetLocation(id)
}

// GetDimension returns the vector dimension of the index.
func (idx *AdaptiveIndex) GetDimension() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetDimension()
}

// Warmup pre-loads index data into memory.
func (idx *AdaptiveIndex) Warmup() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().Warmup()
}

// SetIndexedColumns sets the columns to be indexed.
func (idx *AdaptiveIndex) SetIndexedColumns(cols []string) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	idx.activeIndex().SetIndexedColumns(cols)
}

// Close releases resources for both underlying indexes.
func (idx *AdaptiveIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	var err error
	if idx.bruteForce != nil {
		err = idx.bruteForce.Close()
	}
	if idx.hnsw != nil {
		if herr := idx.hnsw.Close(); herr != nil && err == nil {
			err = herr
		}
	}
	return err
}

// TrainPQ trains a Product Quantizer for the index.
func (idx *AdaptiveIndex) TrainPQ(vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.activeIndex().TrainPQ(vectors)
}

// GetPQEncoder returns the Product Quantizer encoder for the index.
func (idx *AdaptiveIndex) GetPQEncoder() *pq.PQEncoder {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetPQEncoder()
}

// EstimateMemory returns an estimate of the memory used by the index.
func (idx *AdaptiveIndex) EstimateMemory() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().EstimateMemory()
}

// GetRawNeighbors returns internal neighbor IDs, delegating to HNSW if active.
func (idx *AdaptiveIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetRawNeighbors(id)
}

// GetNeighbors returns k-nearest neighbors, delegating to HNSW if active.
func (idx *AdaptiveIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetNeighbors(ctx, id, k)
}

// Search performs a vector search and returns a list of candidates, delegating to the active implementation.
func (idx *AdaptiveIndex) Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error) {
	if idx.usingHNSW.Load() {
		return idx.hnsw.Search(ctx, query, k, filter)
	}
	// Brute force Search
	results, err := idx.SearchVectors(ctx, query, k, nil, types.SearchOptions{})
	if err != nil {
		return nil, err
	}
	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{ID: uint32(r.ID), Dist: r.Distance}
	}
	return candidates, nil
}

// Size returns the total number of vectors in the active index.
func (idx *AdaptiveIndex) Size() int { return idx.Len() }

// GetEntryPoint returns the entry point of the index.
func (idx *AdaptiveIndex) GetEntryPoint() uint32 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetEntryPoint()
}

// GetVectorID returns the ID of a vector given its location.
func (idx *AdaptiveIndex) GetVectorID(loc any) (uint32, bool) {
	return idx.activeIndex().GetVectorID(loc)
}

// DeleteBatch removes a batch of vectors from the active index.
func (idx *AdaptiveIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.activeIndex().DeleteBatch(ctx, ids)
}

// ExportState serializes the active index state.
func (idx *AdaptiveIndex) ExportState() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().ExportState()
}

// ImportState is not supported for AdaptiveIndex.
func (idx *AdaptiveIndex) ImportState(data []byte) error { return nil }

// ExportGraph serializes the active index graph.
func (idx *AdaptiveIndex) ExportGraph(w io.Writer) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().ExportGraph(w)
}

// ImportGraph is not supported for AdaptiveIndex.
func (idx *AdaptiveIndex) ImportGraph(r io.Reader) error { return nil }

// ExportDelta returns changes since a given version from the active index.
func (idx *AdaptiveIndex) ExportDelta(fromVersion uint64) (*DeltaSync, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().ExportDelta(fromVersion)
}

// ApplyDelta applies incremental changes to the active index.
func (idx *AdaptiveIndex) ApplyDelta(delta *DeltaSync) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.activeIndex().ApplyDelta(delta)
}

// SetParallelSearchConfig updates parallel search settings for HNSW if active.
func (idx *AdaptiveIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {
	if h := idx.hnsw; h != nil {
		h.SetParallelSearchConfig(cfg)
	}
}

// GetParallelSearchConfig returns the active parallel search configuration.
func (idx *AdaptiveIndex) GetParallelSearchConfig() ParallelSearchConfig {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetParallelSearchConfig()
}

// RemapLocations updates ID-to-location mappings in the active index.
func (idx *AdaptiveIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.activeIndex().RemapLocations(ctx, mapping)
}

// GetGPUIndex returns the underlying GPU index if available.
func (idx *AdaptiveIndex) GetGPUIndex() any {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetGPUIndex()
}

// PreWarm pre-allocates resources for the active index.
func (idx *AdaptiveIndex) PreWarm(targetSize int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	idx.activeIndex().PreWarm(targetSize)
}

// migrateToHNSW performs the background migration from BruteForceIndex to HNSW.
func (idx *AdaptiveIndex) migrateToHNSW() {
	if idx.usingHNSW.Load() || !idx.migrating.CompareAndSwap(false, true) {
		return
	}
	idx.migrationCount.Add(1)

	go func() {
		defer idx.migrating.Store(false)

		config := DefaultArrowHNSWConfig()
		config.Metric = idx.dataset.GetMetric()
		config.Logger = idx.dataset.GetLogger()
		newHNSW := NewArrowHNSW(idx.dataset, &config, idx.dataset.GetTopo())

		// Snapshot current brute force state
		idx.mu.Lock()
		bf := idx.bruteForce.(*BruteForceIndex)
		snapshotLocations := make([]Location, len(bf.locations))
		copy(snapshotLocations, bf.locations)
		idx.mu.Unlock()

		// Build HNSW from snapshot. Group by batch to use AddBatch efficiency.
		if len(snapshotLocations) > 0 {
			recs := idx.dataset.GetRecords()
			// Simple grouping by batch
			byBatch := make(map[int][]int) // batchIdx -> []rowIdx
			for _, loc := range snapshotLocations {
				byBatch[loc.BatchIdx] = append(byBatch[loc.BatchIdx], loc.RowIdx)
			}

			for bIdx, rows := range byBatch {
				if bIdx >= 0 && bIdx < len(recs) {
					batchRecs := []arrow.RecordBatch{recs[bIdx]}
					batchIdxs := make([]int, len(rows))
					for i := range batchIdxs {
						batchIdxs[i] = bIdx
					}
					_, _ = newHNSW.AddBatch(context.Background(), batchRecs, rows, batchIdxs)
				}
			}
		}

		// Final swap and delta handling
		swapStart := time.Now()
		idx.mu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "write").Observe(time.Since(swapStart).Seconds())
		defer idx.mu.Unlock()

		// Re-snapshot to find any vectors added to bruteForce while we were building HNSW
		bf = idx.bruteForce.(*BruteForceIndex)
		currentLocations := bf.locations
		if len(currentLocations) > len(snapshotLocations) {
			delta := currentLocations[len(snapshotLocations):]
			for _, loc := range delta {
				// Use AddByLocation on the new HNSW before we publish it
				_, _ = newHNSW.AddByLocation(context.Background(), loc.BatchIdx, loc.RowIdx)
			}
		}

		idx.hnsw = newHNSW
		idx.usingHNSW.Store(true)

		metrics.AdaptiveIndexMigrationsTotal.WithLabelValues("brute_force", "hnsw").Inc()
		idx.bruteForce = nil
	}()
}

// SearchVectors finds k-nearest neighbors, delegating to the active implementation.
func (idx *AdaptiveIndex) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()

	if idx.usingHNSW.Load() {
		metrics.HnswSearchesTotal.Inc()
		return idx.hnsw.SearchVectors(ctx, q, k, filters, options)
	}
	metrics.BruteForceSearchesTotal.Inc()
	if idx.bruteForce != nil {
		return idx.bruteForce.SearchVectors(ctx, q, k, filters, options)
	}
	return nil, nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (idx *AdaptiveIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]SearchResult, error) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.GetName(), "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()

	if idx.usingHNSW.Load() {
		return idx.hnsw.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}
	if idx.bruteForce != nil {
		return idx.bruteForce.SearchVectorsInRange(ctx, q, threshold, filters, options)
	}
	return nil, nil
}

// GetIndexType returns the type of the active index ("hnsw" or "brute_force").
func (idx *AdaptiveIndex) GetIndexType() string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().GetIndexType()
}

// GetMigrationCount returns the number of times the index has migrated.
func (idx *AdaptiveIndex) GetMigrationCount() int64 {
	return idx.migrationCount.Load()
}

// Len returns the total number of vectors across all underlying indexes.
func (idx *AdaptiveIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().Len()
}

// GetData returns graph data if HNSW is active.
func (idx *AdaptiveIndex) GetData() *types.GraphData {
	idx.mu.RLock()
	hnsw := idx.hnsw
	idx.mu.RUnlock()

	if hnsw == nil {
		return nil
	}
	if g, ok := hnsw.(interface{ GetData() *types.GraphData }); ok {
		return g.GetData()
	}
	return nil
}

// GetShardedIndex returns the underlying sharded index if active.
func (idx *AdaptiveIndex) GetShardedIndex() *ShardedHNSW {
	idx.mu.RLock()
	hnsw := idx.hnsw
	idx.mu.RUnlock()

	if hnsw == nil {
		return nil
	}
	if s, ok := hnsw.(interface{ GetShardedIndex() *ShardedHNSW }); ok {
		return s.GetShardedIndex()
	}
	return nil
}

// RelocateToOffHeap relocates index structures to off-heap memory.
func (idx *AdaptiveIndex) RelocateToOffHeap() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.activeIndex().RelocateToOffHeap()
}

// RelocateToOffHeap relocates index structures to off-heap memory.
