package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkedGrowth(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.InitialCapacity = 10 // Start small

	// Create HNSW
	h := NewArrowHNSW(&Dataset{Name: "test"}, config, nil)

	// Verify initial chunks
	data := h.data.Load()

	// Default min capacity is 1000 in NewArrowHNSW logic, so we expect 1 chunk (if ChunkSize=1024)
	// Let's verify expectations
	assert.GreaterOrEqual(t, data.Capacity, 1000)

	// We want to force growth.
	// Let's manually manipulate capacity for testing, OR just Grow huge.

	targetCap := 5000
	h.Grow(targetCap, 0)

	newData := h.data.Load()
	assert.GreaterOrEqual(t, newData.Capacity, targetCap)

	// Check num chunks
	// Grow adds ChunkSize headroom and then rounds up
	// Grow ensures capacity >= targetCap. It rounds up to ChunkSize.
	// We expect capacity to be 5000 (if exact) rounded up to ChunkSize.
	// 5000 / 1024 = 4.88 -> 5 chunks.
	expectedCap := targetCap
	if expectedCap%ChunkSize != 0 {
		expectedCap = ((expectedCap / ChunkSize) + 1) * ChunkSize
	}
	expectedChunks := expectedCap / ChunkSize
	assert.Equal(t, expectedChunks, len(newData.Levels))

	// Ensure Chunk 0 is allocated for testing
	// Use ensureChunk which handles Arena allocation
	var err error
	newData, err = h.ensureChunk(newData, 0, 0, 128)
	if err != nil {
		t.Fatal(err)
	}

	// Test Data Persistence across Grow
	// Write something to chunk 0
	cID := chunkID(0)
	cOff := chunkOffset(0)
	levels := newData.GetLevelsChunk(cID)
	if levels != nil {
		levels[cOff] = 99
	}
	neighbors := newData.GetNeighborsChunk(0, cID)
	if neighbors != nil {
		neighbors[int(cOff)*MaxNeighbors] = 999
	}

	// Grow again
	h.Grow(targetCap+ChunkSize, 0) // Add one more chunk
	grownData := h.data.Load()

	// Check old data preserved
	gLevels2 := grownData.GetLevelsChunk(0)
	if gLevels2 == nil || len(gLevels2) <= int(cOff) || gLevels2[cOff] != 99 {
		t.Error("Level data lost after grow")
	}
	gNeighbors2 := grownData.GetNeighborsChunk(0, 0)
	if gNeighbors2 == nil || len(gNeighbors2) <= int(cOff)*MaxNeighbors || gNeighbors2[int(cOff)*MaxNeighbors] != 999 {
		t.Error("Neighbor data lost after grow")
	}
}

func TestChunkHelpers(t *testing.T) {
	// Verify chunkID/Offset logic matches ChunkSize constants
	id := uint32(ChunkSize + 5)

	cID := chunkID(id)
	cOff := chunkOffset(id)

	assert.Equal(t, uint32(1), cID)
	assert.Equal(t, uint32(5), cOff)

	id2 := uint32(5)
	assert.Equal(t, uint32(0), chunkID(id2))
	assert.Equal(t, uint32(5), chunkOffset(id2))
}
