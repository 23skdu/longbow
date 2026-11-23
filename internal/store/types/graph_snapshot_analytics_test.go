package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCloneForAnalyticsDoesNotAllocateArenas pins the fix for the GraphRAG
// memory leak: repeated analytics snapshots must copy topology onto the Go
// heap instead of appending chunks to the live off-heap arenas.
func TestCloneForAnalyticsDoesNotAllocateArenas(t *testing.T) {
	g := NewGraphData(1024, 4, false, false, -1, false, false, false,
		VectorTypeFloat32, false, false, false, 8, "test-analytics", nil, false)
	require.NotNil(t, g)
	require.NoError(t, g.SetNeighborsAtLayer(0, 1, []uint32{2, 3}))

	usedBefore := g.Uint32Arena.TotalAllocated()

	for range 20 {
		snap := g.CloneForAnalytics()
		require.NotNil(t, snap)
		require.Nil(t, snap.PackedNeighbors, "analytics snapshots must not retain packed adjacency")
		require.Equal(t, []uint32{2, 3}, snap.GetNeighbors(0, 1, nil))
	}

	require.Equal(t, usedBefore, g.Uint32Arena.TotalAllocated(),
		"analytics snapshots must not allocate from the live topology arena")

	// Persistence snapshots intentionally keep arena-backed offsets.
	_ = g.CloneForSnapshot()
	require.Greater(t, g.Uint32Arena.TotalAllocated(), usedBefore,
		"persistence snapshots should still copy topology into the arena")
}
