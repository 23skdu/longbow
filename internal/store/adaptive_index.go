package store

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
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
// BruteForceIndex - Linear scan index for small datasets
// =============================================================================

// BruteForceIndex implements VectorIndex using linear scan O(N) search.
type BruteForceIndex struct {
	mu            sync.RWMutex
	locations     []Location
	dataset       *Dataset
	activeReaders atomic.Int64  // Track active zero-copy readers
	currentEpoch  atomic.Uint64 // Epoch counter for safe reclamation //nolint:unused
}

// NewBruteForceIndex creates a new brute force index for the given dataset.
func NewBruteForceIndex(ds *Dataset) *BruteForceIndex {
	return &BruteForceIndex{
		dataset:   ds,
		locations: make([]Location, 0, 64),
	}
}

// AddByLocation adds a vector from the dataset using batch and row indices.
func (b *BruteForceIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	start := time.Now()
	b.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer b.mu.Unlock()

	id := uint32(len(b.locations))
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
	ids := make([]uint32, len(recs))
	for i := range recs {
		id, _ := b.AddByRecord(ctx, recs[i], rowIdxs[i], batchIdxs[i])
		ids[i] = id
	}
	return ids, nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (b *BruteForceIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options SearchOptions) ([]SearchResult, error) {
	// Not implemented for BruteForce, but needed for interface
	return nil, nil
}

// GetLocation retrieves the storage location for a given vector ID.
func (b *BruteForceIndex) GetLocation(id VectorID) (Location, bool) {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	if int(id) >= len(b.locations) {
		return Location{}, false
	}
	return b.locations[id], true
}

func (b *BruteForceIndex) GetDimension() uint32 {
	// Logic to get dim from dataset
	return 0
}

func (b *BruteForceIndex) Warmup() int {
	return 0
}

func (b *BruteForceIndex) SetIndexedColumns(cols []string) {}

func (b *BruteForceIndex) Close() error {
	return nil
}

func (b *BruteForceIndex) TrainPQ(vectors [][]float32) error {
	// Not implemented for BruteForce
	return nil
}

func (b *BruteForceIndex) GetPQEncoder() *pq.PQEncoder {
	return nil
}

func (b *BruteForceIndex) EstimateMemory() int64 {
	return int64(len(b.locations) * 8)
}

// SearchVectors returns the k nearest neighbors using linear scan.
func (b *BruteForceIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	qF32, ok := q.([]float32)
	if !ok {
		// BruteForce currently only supports float32
		return nil, errors.New("BruteForceIndex only supports []float32 queries")
	}
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()

	if len(b.locations) == 0 {
		return nil, nil
	}

	// Use a max-heap to track k smallest distances
	h := &bfSearchHeap{}
	heap.Init(h)

	for i, loc := range b.locations {
		if i%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		// Use zero-copy access for performance
		vec, release := b.getVectorUnsafe(loc)
		if vec == nil || release == nil {
			continue
		}

		dist, err := simd.EuclideanDistance(qF32, vec)
		release() // Release immediately after distance computation

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

	// Extract results in sorted order (ascending distance)
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

// Len returns the number of indexed vectors.
func (b *BruteForceIndex) Len() int {
	start := time.Now()
	b.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(b.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer b.mu.RUnlock()
	return len(b.locations)
}

// getVector retrieves a vector from the dataset.
func (b *BruteForceIndex) getVector(loc Location) []float32 {
	if b.dataset == nil || loc.BatchIdx >= len(b.dataset.Records) {
		return nil
	}

	record := b.dataset.Records[loc.BatchIdx]
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

// getVectorUnsafe retrieves a zero-copy view of the vector.
// The caller MUST call release() when done to decrement the epoch counter.
// The returned slice is only valid until release() is called.
func (b *BruteForceIndex) getVectorUnsafe(loc Location) (vec []float32, release func()) {
	b.enterEpoch()

	if b.dataset == nil || loc.BatchIdx >= len(b.dataset.Records) {
		b.exitEpoch()
		return nil, nil
	}

	record := b.dataset.Records[loc.BatchIdx]
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

	// Return direct slice view (zero-copy)
	vec = values[start : start+listSize]
	release = b.exitEpoch

	// Track metrics
	metrics.VectorAccessZeroCopyTotal.WithLabelValues(b.dataset.Name, "brute_force").Inc()

	return vec, release
}

// enterEpoch increments the active reader count
func (b *BruteForceIndex) enterEpoch() {
	b.activeReaders.Add(1)
}

// exitEpoch decrements the active reader count
func (b *BruteForceIndex) exitEpoch() {
	b.activeReaders.Add(-1)
}

// bfHeapItem is a heap item for brute force search.
type bfHeapItem struct {
	id    VectorID
	score float32
}

// bfSearchHeap implements a max-heap for k-NN search.
type bfSearchHeap []bfHeapItem

func (h bfSearchHeap) Len() int           { return len(h) }
func (h bfSearchHeap) Less(i, j int) bool { return h[i].score > h[j].score } // Max-heap
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

// =============================================================================
// AdaptiveIndex - Wrapper that switches between BruteForce and HNSW
// =============================================================================

// AdaptiveIndex automatically switches between BruteForce and HNSW based on size.
type AdaptiveIndex struct {
	mu             sync.RWMutex
	dataset        *Dataset
	config         AdaptiveIndexConfig
	bruteForce     *BruteForceIndex
	hnsw           VectorIndex
	usingHNSW      atomic.Bool
	migrating      atomic.Bool
	migrationCount atomic.Int64
	vectorCount    atomic.Int64
}

// NewAdaptiveIndex creates an adaptive index starting with BruteForce.
func NewAdaptiveIndex(ds *Dataset, cfg AdaptiveIndexConfig) *AdaptiveIndex {
	a := &AdaptiveIndex{
		dataset:    ds,
		config:     cfg,
		bruteForce: NewBruteForceIndex(ds),
	}
	return a
}

// AddByLocation delegates to the active index.
func (a *AdaptiveIndex) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	// Fast path: if HNSW is already active, delegate with RLock
	a.mu.RLock()
	hnsw := a.hnsw
	a.mu.RUnlock()

	if hnsw != nil {
		return hnsw.AddByLocation(ctx, batchIdx, rowIdx)
	}

	// If HNSW is not active, acquire a write lock for BruteForce operations and potential migration
	start := time.Now()
	a.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer a.mu.Unlock()

	// Re-check if HNSW became active while waiting for the lock
	if a.usingHNSW.Load() {
		return a.hnsw.AddByLocation(ctx, batchIdx, rowIdx)
	}

	// Use BruteForce
	id, err := a.bruteForce.AddByLocation(ctx, batchIdx, rowIdx)
	if err == nil {
		newCount := a.vectorCount.Add(1)
		if a.config.Enabled && int(newCount) >= a.config.Threshold { //nolint:gosec // G115
			a.migrateToHNSW()
		}
	}
	return id, err
}

// AddByRecord adds a vector from a record batch.
func (a *AdaptiveIndex) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return a.AddByLocation(ctx, batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (a *AdaptiveIndex) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	start := time.Now()
	a.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer a.mu.Unlock()

	ids := make([]uint32, len(recs))
	for i := range recs {
		id, err := a.AddByLocation(ctx, batchIdxs[i], rowIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (a *AdaptiveIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *roaring.Bitmap, options SearchOptions) ([]SearchResult, error) {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}
	return nil, nil
}

func (a *AdaptiveIndex) GetLocation(id VectorID) (Location, bool) {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		val, _ := a.hnsw.GetLocation(uint32(id))
		if loc, ok := val.(Location); ok {
			return loc, true
		}
		return Location{}, false
	}
	return a.bruteForce.GetLocation(id)
}

func (a *AdaptiveIndex) GetDimension() uint32 {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.GetDimension()
	}
	return 0
}

func (a *AdaptiveIndex) Warmup() int {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.Warmup()
	}
	return 0
}

func (a *AdaptiveIndex) SetIndexedColumns(cols []string) {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		a.hnsw.SetIndexedColumns(cols)
	}
}

func (a *AdaptiveIndex) Close() error {
	start := time.Now()
	a.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer a.mu.Unlock()
	if a.usingHNSW.Load() {
		return a.hnsw.Close()
	}
	return a.bruteForce.Close()
}

func (a *AdaptiveIndex) TrainPQ(vectors [][]float32) error {
	start := time.Now()
	a.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "write").Observe(time.Since(start).Seconds())
	defer a.mu.Unlock()
	if a.usingHNSW.Load() {
		return a.hnsw.TrainPQ(vectors)
	}
	return a.bruteForce.TrainPQ(vectors)
}

func (a *AdaptiveIndex) GetPQEncoder() *pq.PQEncoder {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.GetPQEncoder()
	}
	return a.bruteForce.GetPQEncoder()
}

func (a *AdaptiveIndex) EstimateMemory() int64 {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.EstimateMemory()
	}
	return a.bruteForce.EstimateMemory()
}

// migrateToHNSW converts from BruteForce to HNSW asynchronously.
// It builds the index in the background and atomically swaps it.
func (a *AdaptiveIndex) migrateToHNSW() {
	// 1. Check if already migrating or using HNSW (fast path)
	if a.usingHNSW.Load() || !a.migrating.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer a.migrating.Store(false)

		// 2. Snapshot current state (Fast RLock)
		migStart := time.Now()
		a.mu.RLock()
		metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(migStart).Seconds())
		if a.bruteForce == nil { // Already migrated?
			a.mu.RUnlock()
			return
		}
		// Copy locations to separate slice to iterate safely without holding lock
		snapshotLocations := make([]Location, len(a.bruteForce.locations))
		copy(snapshotLocations, a.bruteForce.locations)
		a.mu.RUnlock()

		// 3. Build HNSW from snapshot (Slow, No Lock)
		config := DefaultArrowHNSWConfig()
		config.Metric = a.dataset.Metric
		config.Logger = a.dataset.Logger
		newHNSW := NewArrowHNSW(a.dataset, config)

		for _, loc := range snapshotLocations {
			_, _ = newHNSW.AddByLocation(context.Background(), loc.BatchIdx, loc.RowIdx)
		}

		// 4. Atomic Swap (Stop The World)
		swapStart := time.Now()
		a.mu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "write").Observe(time.Since(swapStart).Seconds())
		defer a.mu.Unlock()

		// Check cancellation/state changes
		if a.usingHNSW.Load() || a.bruteForce == nil {
			_ = newHNSW.Close() // Discard result
			return
		}

		// 5. Catch up (Apply Delta)
		currentLocations := a.bruteForce.locations
		if len(currentLocations) > len(snapshotLocations) {
			delta := currentLocations[len(snapshotLocations):]
			for _, loc := range delta {
				_, _ = newHNSW.AddByLocation(context.Background(), loc.BatchIdx, loc.RowIdx)
			}
		}

		// 6. Swap
		a.hnsw = newHNSW
		a.usingHNSW.Store(true)
		a.migrationCount.Add(1)
		metrics.AdaptiveIndexMigrationsTotal.WithLabelValues("brute_force", "hnsw").Inc()
		a.bruteForce = nil
	}()
}

// SearchVectors delegates to the active index.
func (a *AdaptiveIndex) SearchVectors(ctx context.Context, q any, k int, filters []query.Filter, options SearchOptions) ([]SearchResult, error) {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()

	if a.usingHNSW.Load() {
		metrics.HnswSearchesTotal.Inc()
		return a.hnsw.SearchVectors(ctx, q, k, filters, options)
	}
	metrics.BruteForceSearchesTotal.Inc()
	// BruteForce doesn't support filters yet, ignoring them
	return a.bruteForce.SearchVectors(ctx, q, k, filters, options)
}

// GetIndexType returns the current index type.
func (a *AdaptiveIndex) GetIndexType() string {
	if a.usingHNSW.Load() {
		return "hnsw"
	}
	return "brute_force"
}

// GetMigrationCount returns the number of times migration occurred.
func (a *AdaptiveIndex) GetMigrationCount() int64 {
	return a.migrationCount.Load()
}

// Len returns the number of indexed vectors.
func (a *AdaptiveIndex) Len() int {
	start := time.Now()
	a.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(a.dataset.Name, "read").Observe(time.Since(start).Seconds())
	defer a.mu.RUnlock()

	if a.usingHNSW.Load() {
		return a.hnsw.Len()
	}
	if a.bruteForce != nil {
		return a.bruteForce.Len()
	}
	return 0
}
