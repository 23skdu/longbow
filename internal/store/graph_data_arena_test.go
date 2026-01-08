package store

import (
	"testing"
)

func TestGraphData_ArenaNeighbors(t *testing.T) {
	// Initialize GraphData with Arena enabled
	// Since we are refactoring, we might need a flag or just defaults.
	// NewGraphData signature: func NewGraphData(capacity, dims int, sq8, pq, bq bool) *GraphData
	gd := NewGraphData(100, 128, false, false, false)

	// Simulate node allocation
	// We need to ensure chunks are allocated for ID 0
	gd.EnsureChunk(0, 128)

	// Simulate adding neighbors for Node 0 at Layer 0
	// We expect a new method or modified usage.
	// Let's assume we add a helper 'AllocNeighbors' or 'SetNeighbors' that uses arena internally.
	// For this test to compile before refactoring, we might need to cast or use what's available.
	// BUT, as per TDD, we write the test for the NEW API we want.

	// Proposed API:
	// gd.SetNeighbors(layer, nodeID, neighbors []uint32)
	// neighbors := gd.GetNeighbors(layer, nodeID)

	layer := 0
	nodeID := uint32(0)
	targets := []uint32{1, 2, 3, 5, 8}

	// This method doesn't exist yet, it will fail compilation if run.
	// That's fine for TDD step 1.
	gd.SetNeighbors(layer, nodeID, targets)

	readBack := gd.GetNeighbors(layer, nodeID, nil)
	if len(readBack) != len(targets) {
		t.Fatalf("Expected len %d, got %d", len(targets), len(readBack))
	}

	for i, v := range readBack {
		if v != targets[i] {
			t.Errorf("Idx %d: expected %d, got %d", i, targets[i], v)
		}
	}
}

func TestGraphData_ArenaGrowth(t *testing.T) {
	// Verify that we can store widespread IDs (triggering multiple chunks/slabs)
	gd := NewGraphData(10000, 16, false, false, false)

	// Add neighbors for node 5000
	id := uint32(5000)
	gd.EnsureChunk(chunkID(id), 16)

	data := []uint32{100, 200, 300}
	gd.SetNeighbors(0, id, data)

	res := gd.GetNeighbors(0, id, nil)
	if len(res) != 3 || res[1] != 200 {
		t.Error("Failed to retrieve neighbors for high ID")
	}
}
