package store

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/coder/hnsw"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/simd"
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
	Graph         *hnsw.Graph[VectorID]
	mu            sync.RWMutex
	locations     []Location
	dataset       *Dataset
	scratchPool   sync.Pool     // Pool for temporary vector buffers during search
	dims          int           // Vector dimensions for pool sizing
	currentEpoch  atomic.Uint64 // Current epoch for zero-copy reclamation
	activeReaders atomic.Int32  // Count of readers in current epoch
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
	h.Graph.Distance = simd.EuclideanDistance
	return h
}

// getVector retrieves the float32 slice for a given ID.
// It returns a copy of the vector to ensure safety against concurrent eviction.
func (h *HNSWIndex) getVector(id VectorID) []float32 {
	h.mu.RLock()
	if int(id) >= len(h.locations) {
		h.mu.RUnlock()
		return nil
	}
	loc := h.locations[id]
	h.mu.RUnlock()

	// Lock the dataset to safely access records
	h.dataset.mu.RLock()
	defer h.dataset.mu.RUnlock()

	// Check if records still exist
	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
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
	if len(listArr.Data().Children()) == 0 {
		return nil
	}
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	// Calculate offset and length
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return nil
	}

	// Return a copy of the slice
	src := floatArr.Float32Values()[start:end]
	dst := make([]float32, len(src))
	copy(dst, src)
	return dst
}

// enterEpoch marks a reader as active in the current epoch.
// Must be paired with exitEpoch to release.
func (h *HNSWIndex) enterEpoch() {
	h.activeReaders.Add(1)
}

// exitEpoch marks a reader as finished with the current epoch.
func (h *HNSWIndex) exitEpoch() {
	h.activeReaders.Add(-1)
}

// advanceEpoch increments the epoch counter after waiting for all readers to exit.
// Used during data structure modifications that invalidate unsafe references.
func (h *HNSWIndex) advanceEpoch() {
	// Wait for all active readers in current epoch to finish
	for h.activeReaders.Load() > 0 {
		// Spin - in production could use runtime.Gosched() or brief sleep
	}
	h.currentEpoch.Add(1)
}

// getScratch retrieves a scratch buffer from the pool, sized for current dimensions.
func (h *HNSWIndex) getScratch() []float32 {
	if h.dims == 0 {
		return nil
	}
	if ptr := h.scratchPool.Get(); ptr != nil {
		return *(ptr.(*[]float32))
	}
	return make([]float32, h.dims)
}

// putScratch returns a scratch buffer to the pool.
func (h *HNSWIndex) putScratch(buf []float32) {
	if buf != nil && len(buf) == h.dims {
		h.scratchPool.Put(&buf)
	}
}

// getVectorInto copies vector data into the provided buffer.
// Returns false if vector not found or buffer too small.
func (h *HNSWIndex) getVectorInto(id VectorID, dst []float32) bool {
	h.mu.RLock()
	if int(id) >= len(h.locations) {
		h.mu.RUnlock()
		return false
	}
	loc := h.locations[id]
	h.mu.RUnlock()

	h.dataset.mu.RLock()
	defer h.dataset.mu.RUnlock()

	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		return false
	}
	rec := h.dataset.Records[loc.BatchIdx]

	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}

	if vecCol == nil {
		return false
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return false
	}

	if len(listArr.Data().Children()) == 0 {
		return false
	}
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() || len(dst) < width {
		return false
	}

	copy(dst, floatArr.Float32Values()[start:end])
	return true
}

// getVectorUnsafe returns a direct reference to the vector data without copying.
// The caller MUST call the returned release function when done accessing the data.
// The returned slice is only valid until release() is called.
// This provides zero-copy access for read-only search hot paths.
func (h *HNSWIndex) getVectorUnsafe(id VectorID) (vec []float32, release func()) {
	h.enterEpoch()

	h.mu.RLock()
	if int(id) >= len(h.locations) {
		h.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	loc := h.locations[id]
	h.mu.RUnlock()

	h.dataset.mu.RLock()

	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	rec := h.dataset.Records[loc.BatchIdx]

	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}

	if vecCol == nil {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	if len(listArr.Data().Children()) == 0 {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		floatArr.Release()
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	// Return direct slice - caller must not hold reference after release
	vec = floatArr.Float32Values()[start:end]

	// Release function cleans up in reverse order
	release = func() {
		floatArr.Release()
		h.dataset.mu.RUnlock()
		h.exitEpoch()
	}

	return vec, release
}

// Add inserts a new vector location into the index and adds it to the graph.
func (h *HNSWIndex) Add(batchIdx, rowIdx int) error {
	h.mu.Lock()
	id := VectorID(len(h.locations))
	h.locations = append(h.locations, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	h.mu.Unlock()

	// Get the vector slice (copy)
	vec := h.getVector(id)
	if vec == nil {
		return nil
	}

	// Initialize dims for pool on first vector
	if h.dims == 0 {
		h.dims = len(vec)
	}
	// Add to HNSW graph
	h.Graph.Add(hnsw.MakeNode(id, vec))
	return nil
}

// SearchByID performs a nearest neighbor search for an existing vector in the index.
func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
	// Get scratch buffer from pool
	scratch := h.getScratch()
	if scratch == nil {
		// Fall back to allocation if pool not initialized
		vec := h.getVector(id)
		if vec == nil {
			return nil
		}
		neighbors := h.Graph.Search(vec, k)
		res := make([]VectorID, len(neighbors))
		for i, n := range neighbors {
			res[i] = n.Key
		}
		return res
	}
	defer h.putScratch(scratch)

	if !h.getVectorInto(id, scratch) {
		return nil
	}

	neighbors := h.Graph.Search(scratch, k)
	res := make([]VectorID, len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res
}
