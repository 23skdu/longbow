package store

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	dataset        *Dataset
	config         AdaptiveIndexConfig
	bruteForce     VectorIndex
	hnsw           VectorIndex
	usingHNSW      atomic.Bool
	migrating      atomic.Bool
	migrationCount atomic.Int64
	vectorCount    atomic.Int64
}

// NewAdaptiveIndex creates an adaptive index starting with BruteForce.
func NewAdaptiveIndex(ds *Dataset, cfg AdaptiveIndexConfig) VectorIndex {
	bf := NewBruteForceIndex(ds)
	a := &AdaptiveIndex{
		dataset:    ds,
		config:     cfg,
		bruteForce: bf,
	}
	return a
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

// =============================================================================
// BruteForceIndex - Linear scan index for small datasets
// =============================================================================

// BruteForceIndex implements VectorIndex using linear scan O(N) search.
type BruteForceIndex struct {
	mu            sync.RWMutex
	locations     []Location
	dataset       *Dataset
	activeReaders atomic.Int64 // Track active zero-copy readers
}

// NewBruteForceIndex creates a new brute force index for the given dataset.
func NewBruteForceIndex(ds *Dataset) VectorIndex {
	return &BruteForceIndex{
		dataset:   ds,
		locations: make([]Location, 0, 64),
	}
}

// IsSharded returns true if the index is sharded. BruteForceIndex is never sharded.
func (b *BruteForceIndex) IsSharded() bool { return false }

// AddByLocation adds a vector to the brute-force index using its storage location.
func (b *BruteForceIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	start := time.Now()
	b.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer b.mu.Unlock()

	id := uint32(len(b.locations)) // #nosec G115
	b.locations = append(b.locations, Location{
		BatchIdx: batchIdx,
		RowIdx:   rowIdx,
	})
	return id, nil
}

// AddByRecord adds a vector from a record batch.
func (b *BruteForceIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return b.AddByLocation(ctx, batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (b *BruteForceIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(rowIdxs))
	for i := range rowIdxs {
		id, _ := b.AddByRecord(ctx, recs[0], rowIdxs[i], batchIdxs[i]) // Note: simplified fallback
		ids[i] = id
	}
	return ids, nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (b *BruteForceIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	h := &bfSearchHeap{}
	heap.Init(h)

	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		// Apply bitmap filter: skip if filter is provided and ID is not in filter
		if filter != nil && !filter.Contains(uint32(i)) {
			continue
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, err := simd.EuclideanDistance(qF32, vec)
		release()

		if err != nil {
			dist = math.MaxFloat32
		}

		if h.Len() < k {
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		} else if dist < (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		}
	}

	results := make([]SearchResult, h.Len())
	for i := len(results) - 1; i >= 0; i-- {
		item := heap.Pop(h).(bfHeapItem)
		results[i] = SearchResult{
			ID:    lbtypes.VectorID(item.id),
			Score: item.score,
		}
	}

	return results, nil
}

// GetLocation returns the physical location of a vector ID.
func (b *BruteForceIndex) GetLocation(id uint32) (any, bool) {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	if int(id) >= len(b.locations) {
		return Location{}, false
	}
	return b.locations[id], true
}

// GetDimension returns the vector dimension of the index.
// GetDimension returns the vector dimension of the index.
func (b *BruteForceIndex) GetDimension() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.dataset.Schema != nil {
		for _, f := range b.dataset.Schema.Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				if fslType, ok := f.Type.(*arrow.FixedSizeListType); ok {
					return uint32(fslType.Len()) // #nosec G115
				}
			}
		}
	}
	return 0
}

// PreWarm pre-allocates resources for a target size. No-op for BruteForceIndex.
func (b *BruteForceIndex) PreWarm(targetSize int) {}

// Warmup is a no-op for BruteForceIndex.
func (b *BruteForceIndex) Warmup() int {
	return 0
}

// SetIndexedColumns is a no-op for BruteForceIndex.
func (b *BruteForceIndex) SetIndexedColumns(cols []string) {}

// Close releases all resources associated with the BruteForceIndex.
func (b *BruteForceIndex) Close() error {
	return nil
}

// TrainPQ is a no-op for BruteForceIndex.
func (b *BruteForceIndex) TrainPQ(vectors [][]float32) error {
	return nil
}

// GetPQEncoder returns nil as PQ is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetPQEncoder() *pq.PQEncoder {
	return nil
}

// Search performs a vector search and returns a list of candidates.
func (b *BruteForceIndex) Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error) {
	results, err := b.SearchVectors(ctx, query, k, nil, SearchOptions{})
	if err != nil {
		return nil, err
	}
	candidates := make([]Candidate, len(results))
	for i, r := range results {
		candidates[i] = Candidate{ID: uint32(r.ID), Dist: r.Distance}
	}
	return candidates, nil
}

// Size returns the number of vectors in the index.
func (b *BruteForceIndex) Size() int { return b.Len() }

// GetEntryPoint returns the entry point of the index. BruteForceIndex always returns 0.
func (b *BruteForceIndex) GetEntryPoint() uint32 { return 0 }

// GetVectorID returns the ID of a vector given its location.
func (b *BruteForceIndex) GetVectorID(loc any) (uint32, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	l, ok := loc.(Location)
	if !ok {
		return 0, false
	}
	for i, existing := range b.locations {
		if existing == l {
			return uint32(i), true
		}
	}
	return 0, false
}

// DeleteBatch removes a batch of vectors from the index. No-op for BruteForceIndex.
func (b *BruteForceIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	return nil
}

// ExportState serializes the current index state.
func (b *BruteForceIndex) ExportState() ([]byte, error) { return nil, nil }

// ImportState restores the index state from a byte array.
func (b *BruteForceIndex) ImportState(data []byte) error { return nil }

// ExportGraph serializes the index graph structure. No-op for BruteForceIndex.
func (b *BruteForceIndex) ExportGraph(w io.Writer) error { return nil }

// ImportGraph restores the index graph from a reader. No-op for BruteForceIndex.
func (b *BruteForceIndex) ImportGraph(r io.Reader) error { return nil }

// ExportDelta returns the changes since a given version.
func (b *BruteForceIndex) ExportDelta(fromVersion uint64) (*DeltaSync, error) { return nil, nil }

// ApplyDelta applies incremental changes to the index.
func (b *BruteForceIndex) ApplyDelta(delta *DeltaSync) error { return nil }

// SetParallelSearchConfig updates the configuration for parallel searches.
func (b *BruteForceIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {}

// GetParallelSearchConfig returns the current parallel search configuration.
func (b *BruteForceIndex) GetParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{}
}

// RemapLocations updates the mapping of vector IDs to storage locations.
func (b *BruteForceIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	return nil
}

// GetGPUIndex returns the underlying GPU index if available.
func (b *BruteForceIndex) GetGPUIndex() any { return nil }

// EstimateMemory returns an estimate of the memory used by the index.
func (b *BruteForceIndex) EstimateMemory() int64 {
	return int64(len(b.locations) * 8)
}

// GetRawNeighbors is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	return nil, errors.New("GetRawNeighbors not supported for BruteForceIndex")
}

// GetNeighbors is not supported for BruteForceIndex.
func (b *BruteForceIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]SearchResult, error) {
	return nil, errors.New("GetNeighbors not supported for BruteForceIndex")
}

// SearchVectors finds k-nearest neighbors using a full linear scan.
func (b *BruteForceIndex) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	h := &bfSearchHeap{}
	heap.Init(h)

	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, err := simd.EuclideanDistance(qF32, vec)
		release()

		if err != nil {
			dist = math.MaxFloat32
		}

		if h.Len() < k {
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		} else if dist < (*h)[0].score {
			heap.Pop(h)
			heap.Push(h, bfHeapItem{
				id:    VectorID(i),
				score: dist,
			})
		}
	}

	results := make([]SearchResult, h.Len())
	for i := len(results) - 1; i >= 0; i-- {
		item := heap.Pop(h).(bfHeapItem)
		results[i] = SearchResult{
			ID:    lbtypes.VectorID(item.id),
			Score: item.score,
		}
	}

	return results, nil
}

// SearchVectorsInRange returns nearest neighbors within a distance threshold.
func (b *BruteForceIndex) SearchVectorsInRange(ctx context.Context, q any, threshold float32, filters []core.Filter, options any) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	var results []SearchResult
	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, _ := simd.EuclideanDistance(qF32, vec)
		release()

		if dist <= threshold {
			results = append(results, SearchResult{
				ID:       VectorID(i),
				Distance: dist,
				Score:    1.0 / (1.0 + dist),
			})
		}
	}

	return results, nil
}

// GetIndexType returns "brute_force".
func (b *BruteForceIndex) GetIndexType() string {
	return "brute_force"
}

// Len returns the number of vectors in the brute-force index.
func (b *BruteForceIndex) Len() int {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	return len(b.locations)
}

// getVector retrieves a vector from the dataset.
func (b *BruteForceIndex) getVector(loc Location) []float32 {
	if b.dataset == nil || loc.BatchIdx >= len(b .dataset.Records.Read()) {
		return nil
	}

	record := b .dataset.Records.Read()[loc.BatchIdx]
	fieldIndices := record.Schema().FieldIndices("vector")
	if len(fieldIndices) == 0 {
		return nil
	}
	vecCol := record.Column(fieldIndices[0])

	list, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil
	}

	values := list.ListValues().(*array.Float32).Float32Values()
	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * listSize

	if start+listSize > len(values) {
		return nil
	}

	// Return a copy to avoid data races
	result := make([]float32, listSize)
	copy(result, values[start:start+listSize])

	// Track metrics
	metrics.VectorAccessCopyTotal.WithLabelValues(b.dataset.Name, "brute_force").Inc()
	metrics.VectorAccessBytesAllocated.WithLabelValues(b.dataset.Name, "brute_force").Add(float64(listSize * 4))

	return result
}

func (b *BruteForceIndex) getVectorUnsafe(loc Location) (vec []float32, release func()) {
	b.enterEpoch()

	if b.dataset == nil || loc.BatchIdx >= len(b .dataset.Records.Read()) {
		b.exitEpoch()
		return nil, nil
	}

	record := b .dataset.Records.Read()[loc.BatchIdx]
	fieldIndices := record.Schema().FieldIndices("vector")
	if len(fieldIndices) == 0 {
		b.exitEpoch()
		return nil, nil
	}

	vecCol := record.Column(fieldIndices[0])
	list, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		b.exitEpoch()
		return nil, nil
	}

	values := list.ListValues().(*array.Float32).Float32Values()
	listSize := int(list.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * listSize

	if start+listSize > len(values) {
		b.exitEpoch()
		return nil, nil
	}

	vec = values[start : start+listSize]
	release = b.exitEpoch

	metrics.VectorAccessZeroCopyTotal.WithLabelValues(b.dataset.Name, "brute_force").Inc()

	return vec, release
}

func (b *BruteForceIndex) enterEpoch() {
	b.activeReaders.Add(1)
}

func (b *BruteForceIndex) exitEpoch() {
	b.activeReaders.Add(-1)
}

type bfHeapItem struct {
	id    VectorID
	score float32
}

type bfSearchHeap []bfHeapItem

func (h bfSearchHeap) Len() int           { return len(h) }
func (h bfSearchHeap) Less(i, j int) bool { return h[i].score > h[j].score }
func (h bfSearchHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *bfSearchHeap) Push(x any) {
	*h = append(*h, x.(bfHeapItem))
}

func (h *bfSearchHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

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
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()

	if idx.usingHNSW.Load() {
		return idx.hnsw.AddByLocation(ctx, batchIdx, rowIdx)
	}

	id, err := idx.bruteForce.AddByLocation(ctx, batchIdx, rowIdx)
	if err == nil {
		newCount := idx.vectorCount.Add(1)
		if idx.config.Enabled && int(newCount) >= idx.config.Threshold {
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
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()

	if idx.usingHNSW.Load() {
		return idx.hnsw.AddBatch(ctx, recs, rowIdxs, batchIdxs)
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
	if idx.config.Enabled && int(newCount) >= idx.config.Threshold {
		idx.migrateToHNSW()
	}

	return ids, nil
}

// SearchVectorsWithBitmap finds k-nearest neighbors using the active index implementation.
func (idx *AdaptiveIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options any) ([]SearchResult, error) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}
	return nil, nil
}

// GetLocation retrieves the storage location for a given vector ID.
func (idx *AdaptiveIndex) GetLocation(id uint32) (any, bool) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetLocation(id)
	}
	return idx.bruteForce.GetLocation(id)
}

// GetDimension returns the vector dimension of the index.
func (idx *AdaptiveIndex) GetDimension() uint32 {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetDimension()
	}
	return idx.bruteForce.GetDimension()
}

// Warmup pre-loads index data into memory.
func (idx *AdaptiveIndex) Warmup() int {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.Warmup()
	}
	return 0
}

// SetIndexedColumns sets the columns to be indexed.
func (idx *AdaptiveIndex) SetIndexedColumns(cols []string) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		idx.hnsw.SetIndexedColumns(cols)
	}
}

// Close releases resources for both underlying indexes.
func (idx *AdaptiveIndex) Close() error {
	start := time.Now()
	idx.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.Close()
	}
	if idx.bruteForce != nil {
		return idx.bruteForce.Close()
	}
	return nil
}

// TrainPQ trains a Product Quantizer for the index.
func (idx *AdaptiveIndex) TrainPQ(vectors [][]float32) error {
	start := time.Now()
	idx.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer idx.mu.Unlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.TrainPQ(vectors)
	}
	return nil
}

// GetPQEncoder returns the Product Quantizer encoder for the index.
func (idx *AdaptiveIndex) GetPQEncoder() *pq.PQEncoder {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetPQEncoder()
	}
	return nil
}

// EstimateMemory returns an estimate of the memory used by the index.
func (idx *AdaptiveIndex) EstimateMemory() int64 {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.EstimateMemory()
	}
	if idx.bruteForce != nil {
		return idx.bruteForce.EstimateMemory()
	}
	return 0
}

// GetRawNeighbors returns internal neighbor IDs, delegating to HNSW if active.
func (idx *AdaptiveIndex) GetRawNeighbors(id uint32) ([]uint32, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetRawNeighbors(id)
	}
	return nil, errors.New("GetRawNeighbors not supported for BruteForceIndex")
}

// GetNeighbors returns k-nearest neighbors, delegating to HNSW if active.
func (idx *AdaptiveIndex) GetNeighbors(ctx context.Context, id uint32, k int) ([]SearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetNeighbors(ctx, id, k)
	}
	return nil, errors.New("GetNeighbors not supported for BruteForceIndex")
}

// Search performs a vector search and returns a list of candidates, delegating to the active implementation.
func (idx *AdaptiveIndex) Search(ctx context.Context, query any, k int, filter any) ([]Candidate, error) {
	if idx.usingHNSW.Load() {
		return idx.hnsw.Search(ctx, query, k, filter)
	}
	// Brute force Search
	results, err := idx.SearchVectors(ctx, query, k, nil, SearchOptions{})
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
func (idx *AdaptiveIndex) Size() int {
	return idx.Len()
}

// GetEntryPoint returns the entry point of the index, delegating to HNSW if active.
func (idx *AdaptiveIndex) GetEntryPoint() uint32 {
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetEntryPoint()
	}
	return 0
}

// GetVectorID returns the ID of a vector given its location.
func (idx *AdaptiveIndex) GetVectorID(loc any) (uint32, bool) {
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetVectorID(loc)
	}
	return idx.bruteForce.GetVectorID(loc)
}

// DeleteBatch removes a batch of vectors from the active index.
func (idx *AdaptiveIndex) DeleteBatch(ctx context.Context, ids []uint32) error {
	if idx.usingHNSW.Load() {
		return idx.hnsw.DeleteBatch(ctx, ids)
	}
	return nil
}

// ExportState serializes the active index state.
func (idx *AdaptiveIndex) ExportState() ([]byte, error) {
	if idx.usingHNSW.Load() {
		return idx.hnsw.ExportState()
	}
	return nil, nil
}

// ImportState is not supported for AdaptiveIndex.
func (idx *AdaptiveIndex) ImportState(data []byte) error {
	// Not supported for adaptive bridge
	return nil
}

// ExportGraph serializes the active index graph.
func (idx *AdaptiveIndex) ExportGraph(w io.Writer) error {
	if idx.usingHNSW.Load() {
		return idx.hnsw.ExportGraph(w)
	}
	return nil
}

// ImportGraph is not supported for AdaptiveIndex.
func (idx *AdaptiveIndex) ImportGraph(r io.Reader) error {
	// Not supported for adaptive bridge
	return nil
}

// ExportDelta returns changes since a given version from the active index.
func (idx *AdaptiveIndex) ExportDelta(fromVersion uint64) (*DeltaSync, error) {
	if idx.usingHNSW.Load() {
		return idx.hnsw.ExportDelta(fromVersion)
	}
	return nil, nil
}

// ApplyDelta applies incremental changes to the active index.
func (idx *AdaptiveIndex) ApplyDelta(delta *DeltaSync) error {
	if idx.usingHNSW.Load() {
		return idx.hnsw.ApplyDelta(delta)
	}
	return nil
}

// SetParallelSearchConfig updates parallel search settings for HNSW if active.
func (idx *AdaptiveIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {
	if h := idx.hnsw; h != nil {
		h.SetParallelSearchConfig(cfg)
	}
}

// GetParallelSearchConfig returns the active parallel search configuration.
func (idx *AdaptiveIndex) GetParallelSearchConfig() ParallelSearchConfig {
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetParallelSearchConfig()
	}
	return ParallelSearchConfig{}
}

// RemapLocations updates ID-to-location mappings in the active index.
func (idx *AdaptiveIndex) RemapLocations(ctx context.Context, mapping map[uint32]any) error {
	if idx.usingHNSW.Load() {
		return idx.hnsw.RemapLocations(ctx, mapping)
	}
	return nil
}

// GetGPUIndex returns the underlying GPU index if available.
func (idx *AdaptiveIndex) GetGPUIndex() any {
	if idx.usingHNSW.Load() {
		return idx.hnsw.GetGPUIndex()
	}
	return nil
}

// PreWarm pre-allocates resources for the active index.
func (idx *AdaptiveIndex) PreWarm(targetSize int) {
	if idx.usingHNSW.Load() {
		idx.hnsw.PreWarm(targetSize)
	}
}

// migrateToHNSW performs the background migration from BruteForceIndex to HNSW.
func (idx *AdaptiveIndex) migrateToHNSW() {
	if idx.usingHNSW.Load() || !idx.migrating.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer idx.migrating.Store(false)

		migStart := time.Now()
		idx.mu.RLock()
		metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(migStart).Seconds())
		if idx.bruteForce == nil {
			idx.mu.RUnlock()
			return
		}
		bf := idx.bruteForce.(*BruteForceIndex)
		// Snapshot existing locations to build HNSW
		snapshotLocations := make([]Location, len(bf.locations))
		copy(snapshotLocations, bf.locations)
		idx.mu.RUnlock()

		config := DefaultArrowHNSWConfig()
		config.Metric = idx.dataset.Metric
		config.Logger = idx.dataset.Logger
		newHNSW := NewArrowHNSW(idx.dataset, &config, idx.dataset.Topo)

		// Set hnsw pointer early so new batches use parallel ingestion path
		idx.mu.Lock()
		idx.hnsw = newHNSW
		idx.mu.Unlock()

		// Build HNSW from snapshot. Group by batch to use AddBatch efficiency.
		if len(snapshotLocations) > 0 {
			recs := idx.dataset.Records.Read()
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

		// Final swap to mark as fully ready for search
		swapStart := time.Now()
		idx.mu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "write").Observe(time.Since(swapStart).Seconds())
		defer idx.mu.Unlock()

		if idx.usingHNSW.Load() || idx.bruteForce == nil {
			return
		}

		idx.usingHNSW.Store(true)
		idx.migrationCount.Add(1)
		metrics.AdaptiveIndexMigrationsTotal.WithLabelValues("brute_force", "hnsw").Inc()
		
		// Note: We don't need a final catch-up loop here because AddBatch 
		// now adds to both indices during migration.
		idx.bruteForce = nil
	}()
}

// SearchVectors finds k-nearest neighbors, delegating to the active implementation.
func (idx *AdaptiveIndex) SearchVectors(ctx context.Context, q any, k int, filters []core.Filter, options any) ([]SearchResult, error) {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
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
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
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
	if idx.usingHNSW.Load() {
		return "hnsw"
	}
	return "brute_force"
}

// GetMigrationCount returns the number of times the index has migrated.
func (idx *AdaptiveIndex) GetMigrationCount() int64 {
	return idx.migrationCount.Load()
}

// Len returns the total number of vectors across all underlying indexes.
func (idx *AdaptiveIndex) Len() int {
	start := time.Now()
	idx.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(idx.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer idx.mu.RUnlock()

	if idx.usingHNSW.Load() {
		return idx.hnsw.Len()
	}
	if idx.bruteForce != nil {
		return idx.bruteForce.Len()
	}
	return 0
}

// GetData returns graph data if HNSW is active.
func (idx *AdaptiveIndex) GetData() *lbtypes.GraphData {
	idx.mu.RLock()
	hnsw := idx.hnsw
	idx.mu.RUnlock()

	if hnsw == nil {
		return nil
	}
	if g, ok := hnsw.(interface{ GetData() *lbtypes.GraphData }); ok {
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
