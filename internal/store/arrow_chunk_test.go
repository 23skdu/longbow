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
	expectedChunks := (targetCap + ChunkSize - 1) / ChunkSize
	assert.Equal(t, expectedChunks, len(newData.Levels))

	// Ensure Chunk 0 is allocated for testing
	if newData.Levels[0] == nil {
		chunk := make([]uint8, ChunkSize)
		newData.Levels[0] = &chunk

		// Also Neighbors
		nChunk := make([]uint32, ChunkSize*MaxNeighbors)
		newData.Neighbors[0][0] = &nChunk
	}

	// Test Data Persistence across Grow
	// Write something to chunk 0
	cID := chunkID(0)
	cOff := chunkOffset(0)
	(*newData.Levels[cID])[cOff] = 99
	(*newData.Neighbors[cID][0])[int(cOff)*MaxNeighbors] = 999

	// Grow again
	h.Grow(targetCap+ChunkSize, 0) // Add one more chunk
	grownData := h.data.Load()

	// Check old data preserved
	if (*grownData.Levels[0])[cOff] != 99 {
		t.Error("Level data lost after grow")
	}
	if (*grownData.Neighbors[0][0])[int(cOff)*MaxNeighbors] != 999 {
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
