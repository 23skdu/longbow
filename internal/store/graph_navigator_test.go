package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestGraph(t *testing.T) *types.GraphData {
	// Create a simple graph with 10 nodes, 2 dimensions
	g := types.NewGraphData(100, 2, false, false, 0, false, false, false, types.VectorTypeFloat32)
	err := g.EnsureChunk(0, 0, 2)
	require.NoError(t, err)

	// Set some vectors (2D)
	for i := uint32(0); i < 10; i++ {
		_ = g.SetVector(i, []float32{float32(i), 0})
	}

	return g
}

func TestGraphNavigator_FindPath_Linear(t *testing.T) {
	g := createTestGraph(t)
	// Create linear graph: 0 -> 1 -> 2 -> 3
	_ = g.SetNeighbors(0, []uint32{1})
	_ = g.SetNeighbors(1, []uint32{2})
	_ = g.SetNeighbors(2, []uint32{3})

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 3,
		MaxHops:  5,
	}

	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{MaxHops: 10}, nil)
	err := nav.Initialize()
	require.NoError(t, err)

	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.True(t, path.Found)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path.Path)
	assert.Equal(t, 3, path.Hops)
}

func TestGraphNavigator_FindPath_Star(t *testing.T) {
	g := createTestGraph(t)
	// Create star graph: 0 -> {1, 2, 3}, 1 -> {4}, 2 -> {4}, 3 -> {4}
	_ = g.SetNeighbors(0, []uint32{1, 2, 3})
	_ = g.SetNeighbors(1, []uint32{4})
	_ = g.SetNeighbors(2, []uint32{4})
	_ = g.SetNeighbors(3, []uint32{4})
	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{MaxHops: 10}, nil)
	err := nav.Initialize()
	require.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 4,
		MaxHops:  5,
	}

	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.True(t, path.Found)
	assert.Equal(t, 2, path.Hops)
	assert.Contains(t, [][]uint32{{0, 1, 4}, {0, 2, 4}, {0, 3, 4}}, path.Path)
}

func TestGraphNavigator_MaxHops(t *testing.T) {
	g := createTestGraph(t)
	// 0 -> 1 -> 2 -> 3
	_ = g.SetNeighbors(0, []uint32{1})
	_ = g.SetNeighbors(1, []uint32{2})
	_ = g.SetNeighbors(2, []uint32{3})

	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{MaxHops: 2}, nil)
	err := nav.Initialize()
	require.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 3,
		MaxHops:  2, // Limit hops to 2, target is at 3
	}

	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.False(t, path.Found)
}

func TestGraphNavigator_DistancePruning(t *testing.T) {
	g := createTestGraph(t)
	// 0 -> 1 -> 2 -> 3
	// Dist 0-1=1, 1-2=1, 2-3=1
	// Total 0-3=3
	_ = g.SetNeighbors(0, []uint32{1})
	_ = g.SetNeighbors(1, []uint32{2})
	_ = g.SetNeighbors(2, []uint32{3})

	// Thresholds:
	// node 1 (hops 1): no prune (hops < 2)
	// node 2 (hops 2): dist(2, 3) = 1.0. If threshold = 0.5, it should prune.
	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{
		MaxHops:           10,
		EarlyTerminate:    true,
		DistanceThreshold: 0.5,
	}, nil)
	err := nav.Initialize()
	require.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 3,
		MaxHops:  10,
	}

	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.False(t, path.Found)
}

func TestGraphNavigator_Metrics(t *testing.T) {
	g := createTestGraph(t)
	_ = g.SetNeighbors(0, []uint32{1})

	reg := prometheus.NewRegistry()
	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{MaxHops: 10}, reg)
	err := nav.Initialize()
	require.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 1,
		MaxHops:  5,
	}

	_, err = nav.FindPath(ctx, query)
	require.NoError(t, err)

	// Check metrics
	metrics := nav.GetMetrics()
	assert.NotNil(t, metrics)
	assert.True(t, nav.IsInitialized())
}

func TestGraphNavigator_SearchRadius(t *testing.T) {
	g := createTestGraph(t)
	// 0 -> 1 -> 2 -> 3
	// We want to terminate at node 2 (hops=2).
	// Node 2 has 1 neighbor (3). If Radius > 1, it should prune.
	_ = g.SetNeighbors(0, []uint32{1})
	_ = g.SetNeighbors(1, []uint32{2})
	_ = g.SetNeighbors(2, []uint32{3}) // Node 2 has 1 neighbor
	_ = g.SetNeighbors(3, []uint32{})

	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{
		MaxHops:        5,
		EarlyTerminate: true,
		SearchRadius:   2.0,
	}, nil)
	err := nav.Initialize()
	require.NoError(t, err)

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 3,
		MaxHops:  5,
	}

	// Should fail to find path because node 2 terminates early
	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.False(t, path.Found)
}
