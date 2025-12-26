package hnsw2

import (
	"testing"
	
	"github.com/23skdu/longbow/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestChunkedGrowth(t *testing.T) {
	config := DefaultConfig()
	config.InitialCapacity = 10 // Start small
	
	// Create HNSW
	h := NewArrowHNSW(&store.Dataset{Name: "test"}, config)
	
	// Verify initial chunks
	data := h.data.Load()
	
	// Default min capacity is 1000 in NewArrowHNSW logic, so we expect 1 chunk (if ChunkSize=1024)
	// Let's verify expectations
	assert.GreaterOrEqual(t, data.Capacity, 1000)
    
    // We want to force growth.
    // Let's manually manipulate capacity for testing, OR just Grow huge.
    
    targetCap := 5000
    h.Grow(targetCap)
    
    newData := h.data.Load()
    assert.GreaterOrEqual(t, newData.Capacity, targetCap)
    
    // Check num chunks
    expectedChunks := (targetCap + ChunkSize - 1) / ChunkSize
    assert.Equal(t, expectedChunks, len(newData.Levels))
    assert.Equal(t, expectedChunks, len(newData.Levels))
    
    // Verify Chunks are distinct and initialized
    for i := 0; i < expectedChunks; i++ {
        assert.Equal(t, ChunkSize, len(newData.Levels[i]))
        // assert.Equal(t, ChunkSize, len(newData.VectorPtrs[i])) // VectorPtrs removed
        assert.Equal(t, ChunkSize*MaxNeighbors, len(newData.Neighbors[0][i]))
    }
    
    // Test Data Persistence across Grow
    // Write something to chunk 0
    cID := chunkID(0)
    cOff := chunkOffset(0)
    newData.Levels[cID][cOff] = 123
    
    // Grow again
    h.Grow(10000)
    grownData := h.data.Load()
    
    assert.Equal(t, uint8(123), grownData.Levels[cID][cOff])
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
