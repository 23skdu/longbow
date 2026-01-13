package store

import (
	"math"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// VectorBatch abstracts a batch of vectors for efficient distance computation.
// It allows accumulating vectors of any supported type (float32, float16, int8)
// and computing distances to a query vector in a batch-optimized way.
type VectorBatch interface {
	// Add adds the vector corresponding to the given ID to the batch.
	// Returns true if the vector was found and added, false otherwise.
	Add(id uint32) bool

	// AddVec adds a raw vector slice directly to the batch.
	// vec must be of the appropriate type for the batch implementation.
	AddVec(vec any) bool

	// Len returns the number of vectors in the batch.
	Len() int

	// ComputeDistances calculates distances from the query vector to all vectors in the batch.
	// The results are written to 'dists', which must have capacity >= Len().
	// The query type must match the batch type or be compatible.
	ComputeDistances(query any, dists []float32)

	// Reset clears the batch for reuse.
	Reset()

	// Get returns the vector at index i.
	Get(i int) any

	// Swap swaps vectors at indices i and j.
	Swap(i, j int)

	// Pop removes the last vector from the batch.
	Pop()
}

// float32VectorBatch implements VectorBatch for float32 vectors.
type float32VectorBatch struct {
	h    *ArrowHNSW
	data *GraphData
	vecs [][]float32
}

func (b *float32VectorBatch) Add(id uint32) bool {
	v := b.h.mustGetVectorFromData(b.data, id)
	if v == nil {
		return false
	}
	// Handle both direct []float32 and potentially other types if we want to auto-convert (optional)
	// For now we assume mustGetVectorFromData returns []float32 or something we can cast.
	if vf32, ok := v.([]float32); ok {
		b.vecs = append(b.vecs, vf32)
		return true
	}
	// TODO: if we receive []float16.Num and this is a float32 batch, do we convert?
	// Probably yes if this is a "legacy" batch handling everything as float32.
	return false
}

func (b *float32VectorBatch) AddVec(vec any) bool {
	if vec == nil {
		b.vecs = append(b.vecs, nil)
		return true
	}
	if vf32, ok := vec.([]float32); ok {
		b.vecs = append(b.vecs, vf32)
		return true
	}
	return false
}

func (b *float32VectorBatch) Len() int {
	return len(b.vecs)
}

func (b *float32VectorBatch) ComputeDistances(query any, dists []float32) {
	q, ok := query.([]float32)
	if !ok {
		// Mismatch. Fill with max dist?
		for i := range b.vecs {
			dists[i] = math.MaxFloat32
		}
		return
	}

	// Basic loop dispatch
	for i, v := range b.vecs {
		if v == nil {
			dists[i] = math.MaxFloat32
		} else {
			dists[i] = simd.DistFunc(q, v)
		}
	}
}

func (b *float32VectorBatch) Reset() {
	b.vecs = b.vecs[:0]
}

func (b *float32VectorBatch) Get(i int) any {
	return b.vecs[i]
}

func (b *float32VectorBatch) Swap(i, j int) {
	b.vecs[i], b.vecs[j] = b.vecs[j], b.vecs[i]
}

func (b *float32VectorBatch) Pop() {
	b.vecs = b.vecs[:len(b.vecs)-1]
}

// float16VectorBatch implements VectorBatch for float16 vectors.
type float16VectorBatch struct {
	h    *ArrowHNSW
	data *GraphData
	vecs [][]float16.Num
}

func (b *float16VectorBatch) Add(id uint32) bool {
	// Handling nil vectors: if we fail to get a vector, we should probably add nil to maintain sync
	// if the caller expects sync. Reference implementation behavior:
	// float32VectorBatch.Add returns false and DOES NOT append if nil (old behavior).
	// But my updated Add logic handles nil at AddVec level.
	// If mustGetVectorFromData returns nil, we return false.
	// Caller (selectNeighbors) handles false by calling AddVec(nil).
	// So we just need to return correct bool.

	v := b.h.mustGetVectorFromData(b.data, id)
	if v == nil {
		return false
	}
	if vf16, ok := v.([]float16.Num); ok {
		b.vecs = append(b.vecs, vf16)
		return true
	}
	return false
}

func (b *float16VectorBatch) AddVec(vec any) bool {
	if vec == nil {
		b.vecs = append(b.vecs, nil)
		return true
	}
	if vf16, ok := vec.([]float16.Num); ok {
		b.vecs = append(b.vecs, vf16)
		return true
	}
	return false
}

func (b *float16VectorBatch) Len() int {
	return len(b.vecs)
}

func (b *float16VectorBatch) ComputeDistances(query any, dists []float32) {
	q, ok := query.([]float16.Num)
	if !ok {
		// Mismatch. Fill with max dist?
		for i := range b.vecs {
			dists[i] = math.MaxFloat32
		}
		return
	}

	// Basic loop dispatch
	for i, v := range b.vecs {
		if v == nil {
			dists[i] = math.MaxFloat32
		} else {
			dists[i] = simd.EuclideanDistanceF16(q, v)
		}
	}
}

func (b *float16VectorBatch) Reset() {
	// To avoid retaining memory, nil out?
	// Batch is per-search-context usually.
	// slice [:0] doesn't release underlying array, but elements are float16 (values).
	// []float16.Num is slice header (pointer, len, cap).
	// If we just reset len, we keep pointers to underlying array chunks?
	// vecs is [][]float16.Num. Yes, it holds pointers to chunks.
	// So we SHOULD nil out elements if we want GC to collect vectors.
	// But Reset() is for reuse within same search op.
	// Put() calls Reset? No, Put() calls Clear/nil logic.
	// Reset() relies on simple reset.
	// For safety against memory leaks in long-lived batch:
	for i := range b.vecs {
		b.vecs[i] = nil
	}
	b.vecs = b.vecs[:0]
}

func (b *float16VectorBatch) Get(i int) any {
	return b.vecs[i]
}

func (b *float16VectorBatch) Swap(i, j int) {
	b.vecs[i], b.vecs[j] = b.vecs[j], b.vecs[i]
}

func (b *float16VectorBatch) Pop() {
	b.vecs[len(b.vecs)-1] = nil // Avoid memory leak
	b.vecs = b.vecs[:len(b.vecs)-1]
}

// newVectorBatch creates a VectorBatch appropriate for the data type.
func (h *ArrowHNSW) newVectorBatch(data *GraphData) VectorBatch {
	if h.config.Float16Enabled || data.Type == VectorTypeFloat16 {
		return &float16VectorBatch{
			h:    h,
			data: data,
			vecs: make([][]float16.Num, 0, h.mMax0),
		}
	}
	// Current default: float32 batch which handles float32 data.
	return &float32VectorBatch{
		h:    h,
		data: data,
		vecs: make([][]float32, 0, h.mMax0),
	}
}
