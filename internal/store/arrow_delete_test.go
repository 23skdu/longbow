package store

import (
	"context"
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	config := DefaultArrowHNSWConfig()
	config.InitialCapacity = 100
	config.M = 16
	config.EfConstruction = 100

	// Create index with dimensions set
	index := NewArrowHNSW(nil, config)
	index.dims.Store(128)

	// Initialize GraphData manually with dimensions
	data := lbtypes.NewGraphData(100, 128, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32)
	index.data.Store(data)

	// Manually allocate chunks for testing using ensureChunk
	// We need to ensure chunks exist for the capacity we created (64 => 1 chunk if ChunkSize=1024)
	numChunks := (64 + ChunkSize - 1) / ChunkSize
	for i := 0; i < numChunks; i++ {
		// Mock dimensions
		dims := 128
		var err error
		data, err = index.ensureChunk(data, i, 0, dims)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Insert 10 vectors
	for i := 0; i < 10; i++ {
		// Mock vector data in dense storage
		cID := chunkID(uint32(i))
		cOff := chunkOffset(uint32(i))

		vecChunk := data.GetVectorsChunk(cID)
		if vecChunk != nil {
			vec1 := vecChunk[int(cOff)*128 : int(cOff+1)*128]
			vec1[0] = float32(i) // Use first element as value
		}

		// Set level 0 for simplicity
		lvlChunk := data.GetLevelsChunk(cID)
		if lvlChunk != nil {
			lvlChunk[cOff] = 0
			if lvlChunk[cOff] != 0 {
				t.Errorf("Level should be 0")
			}
		}

		// Normally Insert would do more, but we just want to test Search filtering
		// Insert needs nodeCount to be updated
		index.nodeCount.Add(1)
	}

	// Set entry point to 0
	index.entryPoint.Store(0)
	index.maxLevel.Store(0)

	// Verify they all exist in search
	query := make([]float32, 128)
	query[0] = 5.0
	_, err := index.Search(context.Background(), query, 10, nil)
	assert.NoError(t, err)

	// Wait, if neighbors are not connected, Search will only find entry point.
	// For Delete test, we just need to verify that if a node IS found, it's filtered.
	// Let's connect them all to node 0.
	ctx := index.searchPool.Get()
	defer index.searchPool.Put(ctx)
	for i := 1; i < 10; i++ {
		index.AddConnection(ctx, data, 0, uint32(i), 0, 16, 0.0)
	}

	results, err := index.Search(context.Background(), query, 10, nil)
	assert.NoError(t, err)
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	// Delete node 5
	_ = index.Delete(5)

	// Search again
	results, err = index.Search(context.Background(), query, 10, nil)
	assert.NoError(t, err)
	// Node 5 should be missing
	found5 := false
	for _, res := range results {
		if uint32(res.ID) == 5 {
			found5 = true
		}
	}
	if found5 {
		t.Errorf("node 5 should have been deleted")
	}

	// Delete all
	for i := 0; i < 10; i++ {
		_ = index.Delete(uint32(i))
	}

	results, err = index.Search(context.Background(), query, 10, nil)
	assert.NoError(t, err)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
