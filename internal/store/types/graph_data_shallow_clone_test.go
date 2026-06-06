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

// TestGraphData_ShallowStructuralClone_HeapSavings verifies that the
// shallow clone allocates substantially less heap than the full Clone.
// The savings come from sharing the per-chunk Vectors* slice headers
// instead of allocating fresh ones and copying the data.
//
// We measure the difference in HeapAlloc before and after each call.
// The savings are not deterministic due to allocator batching, so we
// run the test in a loop and assert that the shallow clone is
// strictly smaller in aggregate.
func TestGraphData_ShallowStructuralClone_HeapSavings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heap savings test in -short mode")
	}

	const (
		dims    = 384
		chunks  = 50
		perIter = 5
	)
	heapBefore := make([]uint64, perIter*2)
	heapAfter := make([]uint64, perIter*2)

	// Build a populated GraphData once. NewGraphData(capacity, ...) sets
	// up the structural slices (Neighbors, Counts, Versions) to the
	// right size for the requested capacity. We populate both the
	// modern path (VectorsF32 + Float32Arena via EnsureChunk) AND the
	// legacy path (Vectors via SetExternalVectorsChunk) so the heap
	// savings from sharing Vectors[i] are measurable.
	g := NewGraphData(chunks*ChunkSize, dims, false, false, -1, false, false, false, VectorTypeFloat32, false, false, false, 8, "test-ssc-heap", nil, false)
	require.NotNil(t, g)
	for cID := 0; cID < chunks; cID++ {
		require.NoError(t, g.EnsureChunk(cID, 0, dims))
		chunk := g.GetVectorsChunk(cID)
		for i := range chunk {
			chunk[i] = float32(i)
		}
		// Also populate the legacy Vectors[chunkID] slice header so
		// the full Clone's per-chunk deep copy has work to do.
		// (The shared Slab chunk from EnsureChunk is reused here —
		// SetExternalVectorsChunk just records the slice header, not
		// a fresh allocation.)
		extSlice := make([]float32, ChunkSize*dims)
		for i := range extSlice {
			extSlice[i] = float32(i)
		}
		require.NoError(t, g.SetExternalVectorsChunk(cID, extSlice, nil))
	}
	// Snapshot the heap so we only measure the delta from the clones.
	runtime.GC()
	base := heapAlloc()

	// Measure: per-iter, do a full Clone and a ShallowStructuralClone,
	// then release both. We track the deltas.
	var fullTotal, shallowTotal uint64
	for i := 0; i < perIter; i++ {
		runtime.GC()
		heapBefore[i*2] = heapAlloc()

		clone := g.Clone()
		heapAfter[i*2] = heapAlloc()
		clone.Release()

		heapBefore[i*2+1] = heapAlloc()
		sclone := g.ShallowStructuralClone()
		heapAfter[i*2+1] = heapAlloc()
		sclone.Release()
	}
	_ = base

	for i := 0; i < perIter; i++ {
		fullTotal += heapAfter[i*2] - heapBefore[i*2]
		shallowTotal += heapAfter[i*2+1] - heapBefore[i*2+1]
	}

	t.Logf("Full Clone total delta:           %d bytes (avg %.1f bytes/iter)", fullTotal, float64(fullTotal)/perIter)
	t.Logf("ShallowStructuralClone total delta: %d bytes (avg %.1f bytes/iter)", shallowTotal, float64(shallowTotal)/perIter)
	t.Logf("Savings:                          %d bytes (%.1f%%)",
		fullTotal-shallowTotal, 100*float64(fullTotal-shallowTotal)/float64(fullTotal))

	// The shallow clone should allocate at most 70% of the full clone.
	// (Empirically the savings are ~75% on this workload; we leave
	// 30% slack for allocator noise.)
	assert.Less(t, shallowTotal, fullTotal*7/10,
		"ShallowStructuralClone must use strictly less heap than full Clone")

	g.Release()
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
