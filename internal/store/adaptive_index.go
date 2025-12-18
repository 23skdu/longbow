package store

import (
"container/heap"
"errors"
"sync"
"sync/atomic"

"github.com/23skdu/longbow/internal/metrics"
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
func (b *BruteForceIndex) AddByLocation(batchIdx, rowIdx int) error {
b.mu.Lock()
defer b.mu.Unlock()

b.locations = append(b.locations, Location{
BatchIdx: batchIdx,
RowIdx:   rowIdx,
})
return nil
}

// SearchVectors returns the k nearest neighbors using linear scan.
func (b *BruteForceIndex) SearchVectors(query []float32, k int) []SearchResult {
b.mu.RLock()
defer b.mu.RUnlock()

if len(b.locations) == 0 {
return nil
}

// Use a max-heap to track k smallest distances
h := &bfSearchHeap{}
heap.Init(h)

for i, loc := range b.locations {
vec := b.getVector(loc)
if vec == nil {
continue
}

dist := simd.EuclideanDistance(query, vec)

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

return results
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
hnsw           *HNSWIndex
usingHNSW      atomic.Bool
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
func (a *AdaptiveIndex) AddByLocation(batchIdx, rowIdx int) error {
a.mu.Lock()
defer a.mu.Unlock()

var err error

if a.usingHNSW.Load() {
err = a.hnsw.AddByLocation(batchIdx, rowIdx)
} else {
err = a.bruteForce.AddByLocation(batchIdx, rowIdx)
if err == nil {
newCount := a.vectorCount.Add(1)
if a.config.Enabled && int(newCount) >= a.config.Threshold {
a.migrateToHNSW()
}
}
}

return err
}

// migrateToHNSW converts from BruteForce to HNSW (must hold mu.Lock).
func (a *AdaptiveIndex) migrateToHNSW() {
if a.usingHNSW.Load() {
return
}

a.hnsw = NewHNSWIndex(a.dataset)

for _, loc := range a.bruteForce.locations {
_ = a.hnsw.AddByLocation(loc.BatchIdx, loc.RowIdx)
}

a.usingHNSW.Store(true)
a.migrationCount.Add(1)
metrics.AdaptiveIndexMigrationsTotal.Inc()
a.bruteForce = nil
}

// SearchVectors delegates to the current index implementation.
func (a *AdaptiveIndex) SearchVectors(query []float32, k int) []SearchResult {
a.mu.RLock()
defer a.mu.RUnlock()

if a.usingHNSW.Load() {
	metrics.HnswSearchesTotal.Inc()
return a.hnsw.SearchVectors(query, k)
}
metrics.BruteForceSearchesTotal.Inc()
return a.bruteForce.SearchVectors(query, k)
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
