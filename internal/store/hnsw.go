package store

import (
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
// Initialize the graph with VectorID as the key type.
h.Graph = hnsw.NewGraph[VectorID]()
// Use Euclidean distance to match previous implementation intent
h.Graph.Distance = hnsw.EuclideanDistance
return h
}

// getVector retrieves the float32 slice for a given ID directly from Arrow memory.
func (h *HNSWIndex) getVector(id VectorID) []float32 {
h.mu.RLock()
// Bounds check
if int(id) >= len(h.locations) {
h.mu.RUnlock()
return nil
}
loc := h.locations[id]
h.mu.RUnlock()

// Access the record
if loc.BatchIdx >= len(h.dataset.Records) {
return nil
}
rec := h.dataset.Records[loc.BatchIdx]

// Find the vector column (assuming it is named "vector")
var vecCol arrow.Array
for i, field := range rec.Schema().Fields() {
if field.Name == "vector" {
vecCol = rec.Column(i)
break
}
}

if vecCol == nil {
return nil
}

// Cast to FixedSizeList
listArr, ok := vecCol.(*array.FixedSizeList)
if !ok {
return nil
}

// Get the underlying float32 array
values := listArr.Data().Children()[0]
floatArr := array.NewFloat32Data(values)
defer floatArr.Release()

// Calculate offset and length
width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
start := loc.RowIdx * width
end := start + width

// Return the slice. This is zero-copy as it points to Arrow memory.
return floatArr.Float32Values()[start:end]
}

// Add inserts a new vector location into the index and adds it to the graph.
func (h *HNSWIndex) Add(batchIdx, rowIdx int) error {
h.mu.Lock()
id := VectorID(len(h.locations))
h.locations = append(h.locations, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
h.mu.Unlock()

// Get the vector slice (zero-copy)
vec := h.getVector(id)
if vec == nil {
return nil
}

// Add to HNSW graph
// We use MakeNode to create the node with the vector slice.
h.Graph.Add(hnsw.MakeNode(id, vec))
return nil
}

// SearchByID performs a nearest neighbor search for an existing vector in the index.
func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
vec := h.getVector(id)
if vec == nil {
return nil
}

// Search using the vector slice
neighbors := h.Graph.Search(vec, k)
res := make([]VectorID, len(neighbors))
for i, n := range neighbors {
res[i] = n.Key
}
return res
}
