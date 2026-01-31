package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphAnalytics_PageRank_Simple(t *testing.T) {
	// A simple 2-node cycle: 1 <-> 2
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32)

	// 1 -> 2
	_ = gd.SetNeighbors(1, []uint32{2})

	// 2 -> 1
	_ = gd.SetNeighbors(2, []uint32{1})

	provider := func() *types.GraphData { return gd }
	ga := NewGraphAnalytics(provider)

	ctx := context.Background()
	config := DefaultPageRankConfig()
	config.Tolerance = 1e-5

	result, err := ga.CalculatePageRank(ctx, config)
	require.NoError(t, err)

	// Expect roughly 0.5 each
	assert.InDelta(t, 0.5, result.Scores[1], 0.01)
	assert.InDelta(t, 0.5, result.Scores[2], 0.01)
}

func TestGraphAnalytics_Communities_Disjoint(t *testing.T) {
	// Two disjoint triangles: {1,2,3} and {4,5,6}
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32)

	// Clip 1
	_ = gd.SetNeighbors(1, []uint32{2, 3})
	_ = gd.SetNeighbors(2, []uint32{1, 3})
	_ = gd.SetNeighbors(3, []uint32{1, 2})

	// Clip 2
	_ = gd.SetNeighbors(4, []uint32{5, 6})
	_ = gd.SetNeighbors(5, []uint32{4, 6})
	_ = gd.SetNeighbors(6, []uint32{4, 5})

	provider := func() *types.GraphData { return gd }
	ga := NewGraphAnalytics(provider)

	res, err := ga.DetectCommunities(context.Background(), 10)
	require.NoError(t, err)

	assert.Equal(t, 2, res.CommunityCount)

	// 1,2,3 should have same label
	l1 := res.Labels[1]
	assert.Equal(t, l1, res.Labels[2])
	assert.Equal(t, l1, res.Labels[3])

	// 4,5,6 should have same label
	l4 := res.Labels[4]
	assert.Equal(t, l4, res.Labels[5])
	assert.Equal(t, l4, res.Labels[6])

	// Labels should differ
	assert.NotEqual(t, l1, l4)
}

func TestGraphAnalytics_StarGraph(t *testing.T) {
	// Center 1, leaves 2,3,4.
	// 1 -> 2,3,4
	// 2,3,4 -> 1 (bidirectional star)
	// PageRank should favor 1.

	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32)
	_ = gd.SetNeighbors(1, []uint32{2, 3, 4})
	_ = gd.SetNeighbors(2, []uint32{1})
	_ = gd.SetNeighbors(3, []uint32{1})
	_ = gd.SetNeighbors(4, []uint32{1})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.CalculatePageRank(context.Background(), DefaultPageRankConfig())
	require.NoError(t, err)

	// 1 should be highest
	s1 := res.Scores[1]
	s2 := res.Scores[2]
	s3 := res.Scores[3]
	s4 := res.Scores[4]

	assert.True(t, s1 > s2, "Center should have higher rank than leaf")
	assert.True(t, s1 > s3)
	assert.True(t, s1 > s4)

	// Leaves should be roughly equal
	assert.InDelta(t, s2, s3, 0.01)
	assert.InDelta(t, s3, s4, 0.01)
}

func TestGraphAnalytics_Properties(t *testing.T) {
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32)
	// Complete graph of 3 nodes: 1->2,3; 2->1,3; 3->1,2
	_ = gd.SetNeighbors(1, []uint32{2, 3})
	_ = gd.SetNeighbors(2, []uint32{1, 3})
	_ = gd.SetNeighbors(3, []uint32{1, 2})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	props, err := ga.AnalyzeProperties(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 3, props.NodeCount)
	assert.Equal(t, 6, props.EdgeCount)
	assert.Equal(t, float32(2.0), props.AvgDegree)
	assert.InDelta(t, 1.0, props.Density, 0.001) // Expect density 1.0 for complete graph
}
