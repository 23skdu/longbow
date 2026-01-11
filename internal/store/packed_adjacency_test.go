package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
)

func TestPackedAdjacency_Basic(t *testing.T) {
	// Setup Arena - need enough space for pages (1024 uint64 = 8192 bytes)
	arena := memory.NewSlabArena(1024 * 1024)
	adj := NewPackedAdjacency(arena, 100)

	// Add neighbors for Node 1
	adj.EnsureCapacity(1)
	neighbors := []uint32{10, 20, 30}
	err := adj.SetNeighbors(1, neighbors)
	assert.NoError(t, err)

	// Retrieve
	got, ok := adj.GetNeighbors(1)
	assert.True(t, ok)
	assert.Equal(t, neighbors, got)
}

func TestPackedAdjacency_Resize(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024) // Increased from 1024
	adj := NewPackedAdjacency(arena, 10)

	// Initial set
	adj.EnsureCapacity(1)
	err := adj.SetNeighbors(1, []uint32{1, 2})
	assert.NoError(t, err)

	// Resize (Update for same node, no new capacity needed but check growth logic usage)
	// Actually testing EnsureCapacity growth:
	adj.EnsureCapacity(5000) // Force growth

	newNeighbors := []uint32{1, 2, 3, 4, 5}
	err = adj.SetNeighbors(1, newNeighbors)
	assert.NoError(t, err)

	got, _ := adj.GetNeighbors(1)
	assert.Equal(t, newNeighbors, got)

	// Test high ID
	adj.EnsureCapacity(5000)
	err = adj.SetNeighbors(5000, []uint32{99})
	assert.NoError(t, err)
	got2, _ := adj.GetNeighbors(5000)
	assert.Equal(t, []uint32{99}, got2)
}

func TestPackedAdjacency_MultipleNodes(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024) // Increased from 1024
	adj := NewPackedAdjacency(arena, 100)

	adj.EnsureCapacity(2)
	err := adj.SetNeighbors(1, []uint32{10, 11})
	assert.NoError(t, err)
	err = adj.SetNeighbors(2, []uint32{20, 21})
	assert.NoError(t, err)

	got1, _ := adj.GetNeighbors(1)
	got2, _ := adj.GetNeighbors(2)

	assert.Equal(t, []uint32{10, 11}, got1)
	assert.Equal(t, []uint32{20, 21}, got2)
}
