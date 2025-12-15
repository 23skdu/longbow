package store

import (
"fmt"
"math"
"sync"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/array"
"github.com/coder/hnsw"
)

// VectorID represents a unique identifier for a vector in the index.
// It maps to a specific location (Batch, Row) in the Arrow buffers.
type VectorID uint32

// Location points to the physical location of a vector in the Arrow records.
type Location struct {
BatchIdx int
RowIdx   int
}

// HNSWIndex wraps the hnsw.Graph and manages the mapping from ID to Arrow data.
type HNSWIndex struct {
Graph     *hnsw.Graph[VectorID]
mu        sync.RWMutex
locations []Location
dataset   *Dataset
}

// NewHNSWIndex creates a new index for the given dataset.
func NewHNSWIndex(ds *Dataset) *HNSWIndex {
h := &HNSWIndex{
dataset:   ds,
locations: make([]Location, 0),
}
// Initialize the graph with the zero-copy distance function
h.Graph = hnsw.New(h.Dist)
return h
}

// Dist calculates the Euclidean distance between two vectors identified by their IDs.
// It performs a zero-copy lookup directly from the Arrow buffers.
func (h *HNSWIndex) Dist(a, b VectorID) float32 {
vecA := h.getVector(a)
vecB := h.getVector(b)
return euclidean(vecA, vecB)
}

// getVector retrieves the float32 slice for a given ID directly from Arrow memory.
func (h *HNSWIndex) getVector(id VectorID) []float32 {
h.mu.RLock()
loc := h.locations[id]
h.mu.RUnlock()

// Access the record
rec := h.dataset.Records[loc.BatchIdx]

// Find the vector column (assuming it is named "vector")
// In a real implementation, we might cache the column index.
var vecCol arrow.Array
for i, field := range rec.Schema().Fields() {
if field.Name == "vector" {
vecCol = rec.Column(i)
break
}
}

if vecCol == nil {
return nil // Should not happen if schema is validated
}

// Cast to FixedSizeList
listArr := vecCol.(*array.FixedSizeList)

// Get the underlying float32 array
values := listArr.Data().Children()[0]
floatArr := array.NewFloat32Data(values)
defer floatArr.Release()

// Calculate offset and length
width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
start := loc.RowIdx * width
end := start + width

// Return the slice. Note: This is a slice of the underlying array.
// It is valid as long as the record is valid.
// Go slice access is safe here.
return floatArr.Float32Values()[start:end]
}

// Add inserts a new vector location into the index and adds it to the graph.
func (h *HNSWIndex) Add(batchIdx, rowIdx int) error {
h.mu.Lock()
id := VectorID(len(h.locations))
h.locations = append(h.locations, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
h.mu.Unlock()

// Add to HNSW graph (thread-safe)
h.Graph.Add(hnsw.Node[VectorID]{Key: id})
return nil
}

// Search performs a nearest neighbor search.
// Note: This requires a way to inject a query vector which isn't in the store yet.
// The coder/hnsw library typically searches by ID. To search by a raw vector,
// we would need to add it temporarily or use a specialized entry point if available.
// For this implementation, we assume we are searching for similar vectors to an existing ID,
// or we would extend the library to support raw vector search.
// 
// However, to strictly follow "store only int32 RecordIDs", we expose SearchByID.
func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
neighbors := h.Graph.Search(id, k)
res := make([]VectorID, len(neighbors))
for i, n := range neighbors {
res[i] = n.Key
}
return res
}

func euclidean(a, b []float32) float32 {
var sum float32
for i := range a {
d := a[i] - b[i]
sum += d * d
}
return float32(math.Sqrt(float64(sum)))
}
