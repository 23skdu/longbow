package store

import (
	"testing"
	"unsafe"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackedAdjacency_Alignment(t *testing.T) {
	// Arena (large enough)
	arena := memory.NewSlabArena(1024 * 1024)
	adj := NewPackedAdjacency(arena, 100)

	// Test Neighbor Alignment (64 bytes)
	// We do multiple allocations to ensure padding is working (offset shift)
	for i := uint32(0); i < 10; i++ {
		adj.EnsureCapacity(i)
		err := adj.SetNeighbors(i, []uint32{1, 2, 3})
		require.NoError(t, err)

		nbrs, ok := adj.GetNeighbors(i)
		require.True(t, ok)
		if len(nbrs) > 0 {
			addr := uintptr(unsafe.Pointer(&nbrs[0]))
			assert.Equal(t, uint64(0), uint64(addr)%64, "Address %d not 64-byte aligned for node %d", addr, i)
		}
	}
}
