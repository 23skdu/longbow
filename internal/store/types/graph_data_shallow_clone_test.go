package types

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGraphData_ShallowStructuralClone_PreservesData verifies that after
// ShallowStructuralClone, reading the new GraphData's per-chunk data via
// the modern path (VectorsF32 offset + Float32Arena.Get) returns the
// same data as the original. This is the functional contract — the
// shallow clone must be indistinguishable from the original for read
// paths.
func TestGraphData_ShallowStructuralClone_PreservesData(t *testing.T) {
	dims := 128
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-ssc-1", nil, false)
	require.NotNil(t, g)

	// Allocate chunk 0 and write a known vector
	require.NoError(t, g.EnsureChunk(0, 0, dims))
	chunk := g.GetVectorsChunk(0)
	require.NotNil(t, chunk)
	for i := 0; i < dims; i++ {
		chunk[i] = float32(i) * 1.5
	}

	// Clone
	clone := g.ShallowStructuralClone()
	require.NotNil(t, clone)
	defer clone.Release()
	defer g.Release()

	// Verify chunk 0 data is the same
	cloneChunk := clone.GetVectorsChunk(0)
	require.NotNil(t, cloneChunk)
	assert.Equal(t, len(chunk), len(cloneChunk), "clone chunk length must match")
	for i := 0; i < dims; i++ {
		assert.Equal(t, chunk[i], cloneChunk[i], "clone chunk[%d] must match original", i)
	}
}

// TestGraphData_ShallowStructuralClone_SharesSliceHeaders verifies that
// the new GraphData's Vectors[i] slice header points to the SAME
// memory as the original's (the optimization that saves heap). Uses
// unsafe.Pointer to compare underlying array addresses.
func TestGraphData_ShallowStructuralClone_SharesSliceHeaders(t *testing.T) {
	dims := 64
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-ssc-2", nil, false)
	require.NotNil(t, g)

	// SetExternalVectorsChunk populates g.Vectors[chunkID] (the legacy
	// slice header that ShallowStructuralClone shares).
	extSlice := make([]float32, ChunkSize*dims)
	for i := range extSlice {
		extSlice[i] = float32(i)
	}
	require.NoError(t, g.SetExternalVectorsChunk(0, extSlice, nil))

	clone := g.ShallowStructuralClone()
	require.NotNil(t, clone)
	defer clone.Release()
	defer g.Release()

	// Both Vectors[0] must point to the same memory
	require.Len(t, g.Vectors, 1)
	require.Len(t, clone.Vectors, 1)
	require.NotNil(t, g.Vectors[0])
	require.NotNil(t, clone.Vectors[0])
	assert.Equal(t,
		uintptr(unsafe.Pointer(&g.Vectors[0][0])),
		uintptr(unsafe.Pointer(&clone.Vectors[0][0])),
		"ShallowStructuralClone must share the Vectors[0] underlying array (saves ~%d bytes)",
		ChunkSize*dims*4,
	)
	assert.Equal(t, len(g.Vectors[0]), len(clone.Vectors[0]))
}

// TestGraphData_Clone_DoesNotCopyVectors is the roadmap Item 33 gate for the
// snapshot clone.
//
// Clone used to deep-copy every chunk's vector array. A bulk ingest clones
// once per sub-batch, so a 100k float32 dim-128 build paid 756 MB to copy
// bytes that are immutable after publication - the second-largest allocation
// site in the whole ingest. Both clones now share those arrays, and this
// measures it directly rather than by comparing the two clones to each other:
// a single 64 MB external chunk, and a clone that must cost a small fraction
// of the payload.
func TestGraphData_Clone_DoesNotCopyVectors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping clone heap test in -short mode")
	}

	const (
		dims    = 256
		rows    = ChunkSize * 4 // 4 chunks' worth of vectors in one array
		perIter = 5
	)
	payload := rows * dims * 4

	g := NewGraphData(rows, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-clone-heap", nil, false)
	require.NotNil(t, g)
	extSlice := make([]float32, rows*dims)
	for i := range extSlice {
		extSlice[i] = float32(i)
	}
	require.NoError(t, g.SetExternalVectorsChunk(0, extSlice, nil))
	defer g.Release()

	measure := func(clone func(*GraphData) *GraphData) uint64 {
		var total uint64
		for i := 0; i < perIter; i++ {
			runtime.GC()
			before := heapAlloc()
			c := clone(g)
			total += heapAlloc() - before
			c.Release()
		}
		return total / perIter
	}

	full := measure((*GraphData).Clone)
	shallow := measure((*GraphData).ShallowStructuralClone)

	t.Logf("Clone bytes/iter: %d, ShallowStructuralClone bytes/iter: %d, vector payload: %d",
		full, shallow, uint64(payload))

	// 10% of the payload leaves room for the structural slices and the
	// allocator's slack while failing loudly if the vectors are copied again.
	limit := uint64(payload) / 10
	assert.Less(t, full, limit,
		"Clone must share the immutable per-chunk vector arrays, not copy them")
	assert.Less(t, shallow, limit,
		"ShallowStructuralClone must share the immutable per-chunk vector arrays, not copy them")
}

// TestGraphData_Clone_SharesVectorArrays pins the sharing itself: a snapshot
// taken between sub-batches must point at the same vector bytes, or the heap
// assertion above could be satisfied by an empty graph.
func TestGraphData_Clone_SharesVectorArrays(t *testing.T) {
	dims := 64
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-clone-share", nil, false)
	require.NotNil(t, g)

	extSlice := make([]float32, ChunkSize*dims)
	for i := range extSlice {
		extSlice[i] = float32(i)
	}
	require.NoError(t, g.SetExternalVectorsChunk(0, extSlice, nil))

	for name, clone := range map[string]func(*GraphData) *GraphData{
		"Clone":                  (*GraphData).Clone,
		"ShallowStructuralClone": (*GraphData).ShallowStructuralClone,
	} {
		t.Run(name, func(t *testing.T) {
			c := clone(g)
			require.NotNil(t, c)
			defer c.Release()
			require.Len(t, c.Vectors, 1)
			require.NotNil(t, c.Vectors[0])
			assert.Equal(t, uintptr(unsafe.Pointer(&g.Vectors[0][0])),
				uintptr(unsafe.Pointer(&c.Vectors[0][0])),
				"%s must share the Vectors[0] underlying array (~%d bytes)", name, ChunkSize*dims*4)
			assert.Equal(t, len(g.Vectors[0]), len(c.Vectors[0]))
			// The outer slice is still private, so replacing a chunk on the
			// clone cannot be seen by the original.
			assert.NotSame(t, unsafe.SliceData(g.Vectors), unsafe.SliceData(c.Vectors),
				"%s must keep its own [][]float32 header array", name)
		})
	}
}

func heapAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestGraphData_ShallowStructuralClone_DeepCopiesStructuralSlices
// verifies that the structural slices (Levels, Neighbors, Counts,
// Versions) are deep-copied, not shared. This is necessary for COW
// correctness because these elements are mutated via atomic CAS.
func TestGraphData_ShallowStructuralClone_DeepCopiesStructuralSlices(t *testing.T) {
	dims := 32
	g := NewGraphData(10, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-ssc-3", nil, false)
	require.NotNil(t, g)

	// Pre-allocate via PreAllocate to set up the structural slices
	require.NoError(t, g.PreAllocate(10))
	require.NoError(t, g.EnsureChunk(0, 0, dims))

	clone := g.ShallowStructuralClone()
	require.NotNil(t, clone)
	defer clone.Release()
	defer g.Release()

	// Levels: must be different array addresses
	if len(g.Levels) > 0 && len(g.Levels[0]) > 0 {
		require.Len(t, clone.Levels, len(g.Levels))
		assert.NotSame(t,
			unsafe.SliceData(g.Levels[0]),
			unsafe.SliceData(clone.Levels[0]),
			"Levels[0] must be deep-copied (different underlying arrays)")
	}

	// Neighbors: must be different array addresses
	if len(g.Neighbors) > 0 && len(g.Neighbors[0]) > 0 {
		assert.NotSame(t,
			unsafe.SliceData(g.Neighbors[0]),
			unsafe.SliceData(clone.Neighbors[0]),
			"Neighbors[0] must be deep-copied")
	}

	// VectorsF32 (offset slice): must be different array addresses
	if len(g.VectorsF32) > 0 {
		assert.NotSame(t,
			unsafe.SliceData(g.VectorsF32),
			unsafe.SliceData(clone.VectorsF32),
			"VectorsF32 (offset slice) must be deep-copied")
	}
}
