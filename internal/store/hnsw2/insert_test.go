package hnsw2

import (
	"sync/atomic"
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
	// TODO: Implement when we have proper vector storage integration in unit tests
	t.Skip("Skipping until vector storage is integrated")
}

func TestInsert_MultipleNodes(t *testing.T) {
	// TODO: Implement when we have proper vector storage integration in unit tests
	t.Skip("Skipping until vector storage is integrated")
}

func TestAddConnection(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	config := DefaultConfig()
	index := NewArrowHNSW(dataset, config)
	
	// Initialize GraphData manually
	data := NewGraphData(10, 0)
	index.data.Store(data)
	
	// Must have search context for pruning
	ctx := index.searchPool.Get()
	defer index.searchPool.Put(ctx)

	// Add connection 0 -> 1 at layer 0
	index.addConnection(ctx, data, 0, 1, 0, 10)
	
	// Check count
	cID := chunkID(0)
	cOff := chunkOffset(0)
	count := atomic.LoadInt32(&data.Counts[0][cID][cOff])
	if count != 1 {
		t.Errorf("expected 1 neighbor, got %d", count)
	}
	
	// Check neighbor
	if data.Neighbors[0][cID][int(cOff)*MaxNeighbors] != 1 {
		t.Errorf("expected neighbor 1, got %d", data.Neighbors[0][cID][int(cOff)*MaxNeighbors])
	}
	
	// Adding same connection again should be idempotent
	index.addConnection(ctx, data, 0, 1, 0, 10)
	
	count = atomic.LoadInt32(&data.Counts[0][cID][cOff])
	if count != 1 {
		t.Errorf("expected 1 neighbor after duplicate add, got %d", count)
	}
}

func TestPruneConnections(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	config := DefaultConfig()
	// Strict alpha to force pruning based on distance
	config.Alpha = 1.0 
	index := NewArrowHNSW(dataset, config)
	
	// Initialize GraphData manually
	data := NewGraphData(20, 11)
	index.data.Store(data)
	
	// Setup vectors for distance calculation
	// Node 0 at Origin [0...]
	// Neighbors 1..10 are orthogonal unit vectors [1,0..], [0,1..]
	// Dist(0, i) = 1.0
	// Dist(i, j) = sqrt(2) = 1.41
	
	dim := 11
	index.dims = dim
	vecs := make([][]float32, 11)
	
	// Vec 0: Origin
	vecs[0] = make([]float32, dim)
	
	// Vecs 1..10: Orthogonal basis
	for i := 1; i <= 10; i++ {
		vecs[i] = make([]float32, dim)
		vecs[i][i-1] = 1.0 // Orthogonal
	}
	
	// Point VectorPtrs to these slices
	// Copy vectors to Dense Storage
	for i := 0; i <= 10; i++ {
		cID := chunkID(uint32(i))
		cOff := chunkOffset(uint32(i))
		copy(data.Vectors[cID][int(cOff)*dim:], vecs[i])
	}
	
	// Add 10 connections to Node 0
	// Neighbors are 1..10. All dist 1.0. Sorted by ID (impl detail).
	// Node 0 is at chunk 0, offset 0
	baseIdx := 0 // Node 0
	for i := 1; i <= 10; i++ {
		idx := baseIdx + (i - 1)
		// Access Chunk 0 of Neighbors
		data.Neighbors[0][0][idx] = uint32(i)
	}
	atomic.StoreInt32(&data.Counts[0][0][0], 10)
	
	// Prune to 5
	// HNSW Heuristic:
	// Select 1 (Dist 1).
	// Check 2. Dist(2,1)=1.41. Dist(2,0)=1. 1.41 * 1.0 > 1. Keep!
	// So orthogonal neighbors should be preserved up to M.
	
	ctx := index.searchPool.Get()
	defer index.searchPool.Put(ctx)
	
	index.pruneConnections(ctx, data, 0, 5, 0)
	
	count := atomic.LoadInt32(&data.Counts[0][0][0])
	if count != 5 {
		t.Errorf("expected 5 neighbors after pruning, got %d", count)
	}
}

func BenchmarkInsert(b *testing.B) {
	// TODO: Implement when vector storage is ready
	b.Skip("Skipping until vector storage is integrated")
}
