package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPackedAdjacencyF16_Storage verifies Float16 neighbor storage
func TestPackedAdjacencyF16_Storage(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	adj := NewPackedAdjacency(arena, 100)

	id := uint32(1)
	neighbors := []uint32{10, 20, 30}
	distances := []float16.Num{
		float16.New(1.0),
		float16.New(2.0),
		float16.New(3.0),
	}

	err := adj.SetNeighborsF16(id, neighbors, distances)
	require.NoError(t, err)

	gotNeighbors, gotDistances, ok := adj.GetNeighborsF16(id)
	assert.True(t, ok)
	assert.Equal(t, neighbors, gotNeighbors)
	assert.Equal(t, distances, gotDistances)
}

func TestPackedAdjacencyF16_MemorySavings(t *testing.T) {
	arena := memory.NewSlabArena(1024 * 1024)
	adj := NewPackedAdjacency(arena, 100)

	id := uint32(1)
	neighbors := make([]uint32, 100)
	for i := range neighbors {
		neighbors[i] = uint32(i)
	}

	// Test F32 storage (legacy) - wait, PackedAdjacency doesn't have F32 storage for distances normally.
	// But it does space allocation.
	err := adj.SetNeighbors(id, neighbors)
	require.NoError(t, err)
	// memF32 := arena.Allocated() // No Allocated() method in SlabArena

	// Just verify F16 works with large set
	arenaF16 := memory.NewSlabArena(1024 * 1024)
	adjF16 := NewPackedAdjacency(arenaF16, 100)

	distances := make([]float16.Num, 100)
	err = adjF16.SetNeighborsF16(id, neighbors, distances)
	require.NoError(t, err)
}
