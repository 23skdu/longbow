package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHNSW_DiskStorage_Integration(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "vectors.bin")
	dims := 128

	// 1. Create DiskVectorStore
	dvs, err := NewDiskVectorStore(path, dims)
	assert.NoError(t, err)

	// 2. Create GraphData with DiskStore attached
	gd := NewGraphData(100, dims, false, false, 0, false, false, false, VectorTypeFloat32)
	gd.DiskStore = dvs

	// 3. Populate DiskStore manually (simulating ingestion)
	vec1 := make([]float32, dims)
	vec1[0] = 1.0
	id1, err := dvs.Append(vec1)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), id1)

	vec2 := make([]float32, dims)
	vec2[0] = 2.0
	id2, err := dvs.Append(vec2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id2)

	// 4. Verify GetVectorAsFloat32 reads from DiskStore
	// Note: RAM vectors are nil because we didn't set them (and didn't ensureChunk them)

	// Case A: RAM Chunk is nil (simulation of ensureChunk skipping it)
	// GetVectorsChunk(0) should return nil
	assert.Nil(t, gd.GetVectorsChunk(0))

	// GetVectorAsFloat32 should fallback to DiskStore
	v1, err := gd.GetVectorAsFloat32(0)
	assert.NoError(t, err)
	assert.Equal(t, vec1, v1)

	v2, err := gd.GetVectorAsFloat32(1)
	assert.NoError(t, err)
	assert.Equal(t, vec2, v2)

	// 5. Verify SetVectorFromFloat32 doesn't crash but might fail or work depending on implementation
	// If we call SetVectorFromFloat32, it tries to get the chunk. If chunk is nil, it can't write.
	// This confirms that if we skip allocation, we effectively prevent RAM usage even if someone tries to write.
	err = gd.SetVectorFromFloat32(0, vec1)
	// It should fail silently or return error? Current impl returns generic error if chunk not found?
	// Let's check SetVectorFromFloat32 implementation.
	// output: "storage not available or type not supported..."
	// Wait, generic SetVectorFromFloat32 checks "if chunk != nil". If nil, it falls through to error.
	// So it should error.
	assert.Error(t, err)
}

// TestHNSW_Insert_SkipRAM checks if standard Insert path respects DiskStore presence
// We can't easily test arrow_hnsw_insert.go without full setup, but we can verify
// if we can construct an HNSW usage that relies on disk.
func TestHNSW_FullFlow_DiskOffload(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "hnsw_disk.bin")
	dims := 4

	dvs, _ := NewDiskVectorStore(path, dims)
	defer dvs.Close()

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.M = 4
	config.EfConstruction = 10

	h := NewArrowHNSW(nil, config, nil)

	// Manually inject DiskStore
	data := h.data.Load()
	data.DiskStore = dvs
	h.data.Store(data)

	// Insert Vectors
	// We need to simulate the pre-write to disk that StoreActions does.
	vec := []float32{1, 0, 0, 0}
	dvs.Append(vec) // ID 0

	// Insert into HNSW
	// This calls InsertWithVector -> ensureChunk -> skips RAM alloc -> skips SetVector -> writes Graph
	err := h.InsertWithVector(0, vec, 0)
	assert.NoError(t, err)

	// Verify RAM vector is missing
	data = h.data.Load()
	chunk := data.GetVectorsChunk(0)
	assert.Nil(t, chunk, "RAM Vector chunk should be nil")

	// Verify we can still retrieve logic via GetVectorAsFloat32 (which uses DiskStore)
	// Access via GraphData directly as HNSW doesn't expose GetVector(id) except via interface?
	// HNSW has GetLocation, GetNeighbors. It doesn't have GetVector(id) method in interface usually?
	// But we can check via data.
	retrieved, err := data.GetVectorAsFloat32(0)
	assert.NoError(t, err)
	assert.Equal(t, vec, retrieved)
}
