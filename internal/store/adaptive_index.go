package store

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/simd"
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
	mu        sync.RWMutex
	locations []Location
	dataset   *Dataset
}

// NewBruteForceIndex creates a new brute force index for the given dataset.
func NewBruteForceIndex(ds *Dataset) *BruteForceIndex {
	return &BruteForceIndex{
		dataset:   ds,
		locations: make([]Location, 0, 64),
	}
}

// AddByLocation adds a vector from the dataset using batch and row indices.
func (b *BruteForceIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := uint32(len(b.locations))
	b.locations = append(b.locations, Location{
		BatchIdx: batchIdx,
		RowIdx:   rowIdx,
	})
	return id, nil
}

// AddByRecord adds a vector from a record batch.
func (b *BruteForceIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return b.AddByLocation(batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (b *BruteForceIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	ids := make([]uint32, len(recs))
	for i := range recs {
		id, _ := b.AddByRecord(recs[i], rowIdxs[i], batchIdxs[i])
		ids[i] = id
	}
	return ids, nil
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (b *BruteForceIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *query.Bitset, options SearchOptions) []SearchResult {
	// Not implemented for BruteForce, but needed for interface
	return nil
}

// GetLocation retrieves the storage location for a given vector ID.
func (b *BruteForceIndex) GetLocation(id VectorID) (Location, bool) {
	b.mu.RLock()
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
	b.mu.RLock()
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
		vec := b.getVector(loc)
		if vec == nil {
			continue
		}

		dist, err := simd.EuclideanDistance(qF32, vec)
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
			ID:    item.id,
			Score: item.score,
		}
	}

	return results, nil
}

// Len returns the number of indexed vectors.
func (b *BruteForceIndex) Len() int {
	b.mu.RLock()
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
	return result
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

// AddByLocation adds a vector and potentially triggers migration to HNSW.
func (a *AdaptiveIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var id uint32
	var err error

	if a.usingHNSW.Load() {
		id, err = a.hnsw.AddByLocation(batchIdx, rowIdx)
	} else {
		id, err = a.bruteForce.AddByLocation(batchIdx, rowIdx)
		if err == nil {
			newCount := a.vectorCount.Add(1)
			if a.config.Enabled && int(newCount) >= a.config.Threshold { //nolint:gosec // G115
				a.migrateToHNSW()
			}
		}
	}

	return id, err
}

// AddByRecord adds a vector from a record batch.
func (a *AdaptiveIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return a.AddByLocation(batchIdx, rowIdx)
}

// AddBatch adds multiple vectors efficiently.
func (a *AdaptiveIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	ids := make([]uint32, len(recs))
	for i := range recs {
		id, err := a.AddByLocation(batchIdxs[i], rowIdxs[i])
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (a *AdaptiveIndex) SearchVectorsWithBitmap(ctx context.Context, q any, k int, filter *query.Bitset, options SearchOptions) []SearchResult {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.SearchVectorsWithBitmap(ctx, q, k, filter, options)
	}
	return nil
}

func (a *AdaptiveIndex) GetLocation(id VectorID) (Location, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.GetLocation(id)
	}
	return a.bruteForce.GetLocation(id)
}

func (a *AdaptiveIndex) GetDimension() uint32 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.GetDimension()
	}
	return 0
}

func (a *AdaptiveIndex) Warmup() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.Warmup()
	}
	return 0
}

func (a *AdaptiveIndex) SetIndexedColumns(cols []string) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		a.hnsw.SetIndexedColumns(cols)
	}
}

func (a *AdaptiveIndex) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.usingHNSW.Load() {
		return a.hnsw.Close()
	}
	return a.bruteForce.Close()
}

func (a *AdaptiveIndex) TrainPQ(vectors [][]float32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.usingHNSW.Load() {
		return a.hnsw.TrainPQ(vectors)
	}
	return a.bruteForce.TrainPQ(vectors)
}

func (a *AdaptiveIndex) GetPQEncoder() *pq.PQEncoder {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.GetPQEncoder()
	}
	return a.bruteForce.GetPQEncoder()
}

func (a *AdaptiveIndex) EstimateMemory() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.usingHNSW.Load() {
		return a.hnsw.EstimateMemory()
	}
	return a.bruteForce.EstimateMemory()
}

// migrateToHNSW converts from BruteForce to HNSW (must hold mu.Lock).
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
		a.mu.RLock()
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
		newHNSW := NewArrowHNSW(a.dataset, config, nil)

		for _, loc := range snapshotLocations {
			_, _ = newHNSW.AddByLocation(loc.BatchIdx, loc.RowIdx)
		}

		// 4. Atomic Swap (Stop The World)
		a.mu.Lock()
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
				_, _ = newHNSW.AddByLocation(loc.BatchIdx, loc.RowIdx)
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
	a.mu.RLock()
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
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.usingHNSW.Load() {
		return a.hnsw.Len()
	}
	if a.bruteForce != nil {
		return a.bruteForce.Len()
	}
	return 0
}
