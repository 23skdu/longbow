package hnsw2

import (
	"testing"
)

func TestDelete(t *testing.T) {
	config := DefaultConfig()
	config.InitialCapacity = 100
	config.M = 16
	config.EfConstruction = 100
	
	// Create index with dimensions set
	index := NewArrowHNSW(nil, config)
	index.dims = 128
	
	// Initialize GraphData manually with dimensions
	data := NewGraphData(100, 128)
	index.data.Store(data)
	
	// Insert 10 vectors
	for i := 0; i < 10; i++ {
		// Mock vector data in dense storage
		cID := chunkID(uint32(i))
		cOff := chunkOffset(uint32(i))
		idx := int(cOff) * 128
		data.Vectors[cID][idx] = float32(i) // Use first element as value
        
		// Set level 0 for simplicity
		data.Levels[cID][cOff] = 0
		
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
	results, _ := index.Search(query, 10, 20, nil)
	
	// Wait, if neighbors are not connected, Search will only find entry point.
	// For Delete test, we just need to verify that if a node IS found, it's filtered.
	// Let's connect them all to node 0.
	ctx := index.searchPool.Get()
	defer index.searchPool.Put(ctx)
	for i := 1; i < 10; i++ {
		index.addConnection(ctx, data, 0, uint32(i), 0, 16)
	}
	
	results, _ = index.Search(query, 10, 20, nil)
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
	
	// Delete node 5
	index.Delete(5)
	
	// Search again
	results, _ = index.Search(query, 10, 20, nil)
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
		index.Delete(uint32(i))
	}
	
	results, _ = index.Search(query, 10, 20, nil)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
