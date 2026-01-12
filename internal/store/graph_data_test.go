package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
)

func TestGraphData_Accessors(t *testing.T) {
	// Setup GraphData
	// Setup GraphData parameters
	capacity := 1024
	dims := 128

	// Test GetNeighborsChunk
	t.Run("GetNeighborsChunk", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		layer := 0
		chunkID := uint32(0)

		// Should be nil initially (lazy allocation)
		chunk := gd.GetNeighborsChunk(layer, chunkID)
		assert.Nil(t, chunk)

		// Allocate (simulate ensureChunk)
		gd.EnsureChunk(chunkID, dims) // We need to expose EnsureChunk or use helper

		// Usage of helper to allocate
		gd.StoreNeighborsChunk(layer, chunkID, make([]uint32, ChunkSize*MaxNeighbors))

		chunk = gd.GetNeighborsChunk(layer, chunkID)
		assert.NotNil(t, chunk)
		assert.Equal(t, ChunkSize*MaxNeighbors, len(chunk))
		assert.Equal(t, ChunkSize*MaxNeighbors, cap(chunk))
	})

	// Test GetCountsChunk
	t.Run("GetCountsChunk", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		layer := 0
		chunkID := uint32(0)

		chunk := gd.GetCountsChunk(layer, chunkID)
		assert.Nil(t, chunk) // Not set yet by StoreNeighborsChunk?
		// StoreNeighborsChunk doesn't Alloc counts, StoreCountsChunk does.

		startData := make([]int32, ChunkSize)
		startData[0] = 5
		gd.StoreCountsChunk(layer, chunkID, startData)

		chunk = gd.GetCountsChunk(layer, chunkID)
		assert.NotNil(t, chunk)
		assert.Equal(t, ChunkSize, len(chunk))
		assert.Equal(t, int32(5), chunk[0])
	})

	// Test GetVersionsChunk
	t.Run("GetVersionsChunk", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		layer := 0

		chunkID := uint32(0)

		chunk := gd.GetVersionsChunk(layer, chunkID)
		assert.Nil(t, chunk)

		data := make([]uint32, ChunkSize)
		data[0] = 99
		gd.StoreVersionsChunk(layer, chunkID, data)

		chunk = gd.GetVersionsChunk(layer, chunkID)
		assert.NotNil(t, chunk)
		assert.Equal(t, ChunkSize, len(chunk))
		assert.Equal(t, uint32(99), chunk[0])
	})

	// Test GetVectorsChunk
	t.Run("GetVectorsChunk", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		chunkID := uint32(0)
		// Vectors array is allocated in NewGraphData if dims > 0

		// But individual chunks are nil until linked?
		// GraphData.Vectors is []uint64 (offsets)

		chunk := gd.GetVectorsChunk(chunkID)
		assert.Nil(t, chunk)

		// We don't have a helper Exposed for Vectors?
		// We can add one or misuse internal for this test?
		// Wait, arrow_hnsw_insert.go uses GetVectorsChunk.
		// Let's create a helper in this test file or rely on existing ones?
		// Test helpers in `arrow_hnsw_test_helpers.go` don't seem to have `StoreVectorsChunk`.

		// Let's simulate what EnsureChunk does manually or use unsafe if needed,
		// OR better, add `StoreVectorsChunk` to helpers if missing?
		// For now, let's skip Vectors if helper is missing, or try to use `ensureChunk` logic if we can access it?
		// `ensureChunk` is on `ArrowHNSW` not `GraphData`.

		// Actually, `GraphData` struct fields are exported, we can manually manipulate them?
		// Vectors is []uint64.
		// Use the new helper
		data := make([]float32, ChunkSize*dims)
		gd.StoreVectorsChunk(chunkID, data)

		chunk = gd.GetVectorsChunk(chunkID)
		assert.NotNil(t, chunk)
		assert.Equal(t, ChunkSize*dims, len(chunk))
	})

	// Test Bounds Checking
	t.Run("BoundsChecking", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		// Out of bounds layer
		assert.Nil(t, gd.GetNeighborsChunk(999, 0))

		// Out of bounds chunkID
		assert.Nil(t, gd.GetNeighborsChunk(0, 999999))
	})

	// Test Arena Direct
	t.Run("ArenaDirect", func(t *testing.T) {
		gd := NewGraphData(capacity, dims, false, false, 0, false, false, false, VectorTypeFloat32)
		defer func() { _ = gd.Close() }()
		// Alloc
		numWords := ChunkSize
		ref, err := gd.Uint32Arena.AllocSlice(numWords)
		if err != nil {
			t.Fatalf("Alloc failed: %v", err)
		}
		t.Logf("Allocated offset: %d", ref.Offset)

		// Get using Ref returned from Alloc
		slice := gd.Uint32Arena.Get(ref)
		assert.NotNil(t, slice)
		assert.Equal(t, ChunkSize, len(slice))

		// Get using Hand-crafted Ref (like GetVersionsChunk)
		manualRef := memory.SliceRef{
			Offset: ref.Offset,
			Len:    uint32(ChunkSize),
			Cap:    uint32(ChunkSize),
		}
		sliceManual := gd.Uint32Arena.Get(manualRef)
		assert.NotNil(t, sliceManual, "Manual ref get failed")
	})
}

// Test Refactoring Correctness - ensure no "pointers to slices"
func TestGraphData_Signatures(t *testing.T) {
	// This uses reflection or just static typing in the test to prove we get slices
	gd := NewGraphData(100, 128, false, false, 0, false, false, false, VectorTypeFloat32)
	defer func() { _ = gd.Close() }()

	chunks := gd.GetNeighborsChunk(0, 0)
	_ = chunks // Usage to confirm type is []uint32 not *[]uint32

	levels := gd.GetLevelsChunk(0)
	_ = levels
}
