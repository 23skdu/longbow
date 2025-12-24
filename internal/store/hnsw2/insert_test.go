package hnsw2

import (
	"testing"
	
	"github.com/23skdu/longbow/internal/store"
)

func TestLevelGenerator(t *testing.T) {
	lg := NewLevelGenerator(1.44269504089) // 1/ln(2)
	
	// Generate 1000 levels and check distribution
	levels := make(map[int]int)
	for i := 0; i < 1000; i++ {
		level := lg.Generate()
		levels[level]++
		
		if level < 0 || level >= MaxLayers {
			t.Errorf("level %d out of bounds [0, %d)", level, MaxLayers)
		}
	}
	
	// Most should be at level 0 (exponential decay)
	if levels[0] < 400 {
		t.Errorf("expected >400 at level 0, got %d", levels[0])
	}
	
	t.Logf("Level distribution: %v", levels)
}

func TestInsert_SingleNode(t *testing.T) {
	// TODO: Implement when we have proper vector storage
	t.Skip("Skipping until vector storage is integrated")
}

func TestInsert_MultipleNodes(t *testing.T) {
	// TODO: Implement when we have proper vector storage
	t.Skip("Skipping until vector storage is integrated")
}

func TestAddConnection(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	// Add two nodes
	index.nodes = make([]GraphNode, 2)
	index.nodes[0].ID = 0
	index.nodes[1].ID = 1
	
	// Add connection 0 -> 1 at layer 0
	index.addConnection(0, 1, 0)
	
	if index.nodes[0].NeighborCounts[0] != 1 {
		t.Errorf("expected 1 neighbor, got %d", index.nodes[0].NeighborCounts[0])
	}
	
	if index.nodes[0].Neighbors[0][0] != 1 {
		t.Errorf("expected neighbor 1, got %d", index.nodes[0].Neighbors[0][0])
	}
	
	// Adding same connection again should be idempotent
	index.addConnection(0, 1, 0)
	
	if index.nodes[0].NeighborCounts[0] != 1 {
		t.Errorf("expected 1 neighbor after duplicate add, got %d", index.nodes[0].NeighborCounts[0])
	}
}

func TestPruneConnections(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	// Add node with many connections
	index.nodes = make([]GraphNode, 1)
	index.nodes[0].ID = 0
	
	// Add 10 connections
	for i := 0; i < 10; i++ {
		index.nodes[0].Neighbors[0][i] = uint32(i + 1)
	}
	index.nodes[0].NeighborCounts[0] = 10
	
	// Prune to 5
	index.pruneConnections(0, 5, 0)
	
	if index.nodes[0].NeighborCounts[0] != 5 {
		t.Errorf("expected 5 neighbors after pruning, got %d", index.nodes[0].NeighborCounts[0])
	}
}

func BenchmarkInsert(b *testing.B) {
	// TODO: Implement when vector storage is ready
	b.Skip("Skipping until vector storage is integrated")
}
