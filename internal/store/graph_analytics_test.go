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
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

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
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

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

	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
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
	gd := types.NewGraphData(10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
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

// FuzzGraphAnalytics_PageRank checks PageRank accuracy invariants on
// arbitrary graphs: scores must be finite, strictly positive, sum to ~1
// (the update preserves normalization), and be deterministic across runs.
func FuzzGraphAnalytics_PageRank(f *testing.F) {
	f.Add(10, 5, float32(0.85))
	f.Add(2, 1, float32(0.5))
	f.Add(100, 3, float32(0.99))
	f.Fuzz(func(t *testing.T, numNodes int, maxNeighbors int, damping float32) {
		if numNodes < 2 || numNodes > 1000 || maxNeighbors < 1 || maxNeighbors > 50 {
			t.Skip()
		}
		if damping <= 0.0 || damping >= 1.0 {
			t.Skip()
		}

		gd := types.NewGraphData(numNodes+10, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

		for i := 1; i <= numNodes; i++ {
			neighbors := make([]uint32, 0, maxNeighbors)
			for j := 0; j < maxNeighbors; j++ {
				nbr := uint32((i+j)%numNodes + 1)
				if nbr != uint32(i) {
					neighbors = append(neighbors, nbr)
				}
			}
			_ = gd.SetNeighbors(uint32(i), neighbors)
		}

		ga := NewGraphAnalytics(func() *types.GraphData { return gd })
		config := DefaultPageRankConfig()
		config.DampingFactor = damping

		res, err := ga.CalculatePageRank(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotEmpty(t, res.Scores)

		var sum float64
		for id, s := range res.Scores {
			if s <= 0 || float64(s) != float64(s) || s > 1e6 { // NaN/Inf/positivity guard
				t.Fatalf("node %d: invalid score %v", id, s)
			}
			sum += float64(s)
		}
		if sum < 0.99 || sum > 1.01 {
			t.Fatalf("scores must sum to ~1, got %v (nodes=%d)", sum, len(res.Scores))
		}

		// Determinism: parallel workers write disjoint ranges, so two runs on
		// the same graph must produce identical scores.
		res2, err := ga.CalculatePageRank(context.Background(), config)
		require.NoError(t, err)
		require.Equal(t, len(res.Scores), len(res2.Scores))
		for id, s := range res.Scores {
			s2, ok := res2.Scores[id]
			require.True(t, ok, "node %d missing on second run", id)
			if diff := s - s2; diff > 1e-6 || diff < -1e-6 {
				t.Fatalf("node %d: nondeterministic score %v vs %v", id, s, s2)
			}
		}
	})
}

// --- Community detection (Label Propagation) accuracy tests ---

// newTestGraphData builds an empty GraphData sized for the given node count.
func newTestGraphData(nodes int) *types.GraphData {
	return types.NewGraphData(nodes+2, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)
}

func TestGraphAnalytics_Communities_PathGraph(t *testing.T) {
	// Bidirectional path 1-2-3-4-5 must converge to a single community.
	gd := newTestGraphData(5)
	_ = gd.SetNeighbors(1, []uint32{2})
	_ = gd.SetNeighbors(2, []uint32{1, 3})
	_ = gd.SetNeighbors(3, []uint32{2, 4})
	_ = gd.SetNeighbors(4, []uint32{3, 5})
	_ = gd.SetNeighbors(5, []uint32{4})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.DetectCommunities(context.Background(), 20)
	require.NoError(t, err)

	require.Equal(t, 1, res.CommunityCount, "path graph must form one community")
	first := res.Labels[1]
	for id := uint32(2); id <= 5; id++ {
		assert.Equal(t, first, res.Labels[id], "node %d must share the community", id)
	}
}

func TestGraphAnalytics_Communities_StarGraph(t *testing.T) {
	gd := newTestGraphData(4)
	_ = gd.SetNeighbors(1, []uint32{2, 3, 4})
	_ = gd.SetNeighbors(2, []uint32{1})
	_ = gd.SetNeighbors(3, []uint32{1})
	_ = gd.SetNeighbors(4, []uint32{1})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.DetectCommunities(context.Background(), 20)
	require.NoError(t, err)

	require.Equal(t, 1, res.CommunityCount)
	assert.Equal(t, res.Labels[1], res.Labels[2])
	assert.Equal(t, res.Labels[1], res.Labels[4])
}

func TestGraphAnalytics_Communities_CompleteGraph(t *testing.T) {
	gd := newTestGraphData(4)
	for i := uint32(1); i <= 4; i++ {
		nbrs := make([]uint32, 0, 3)
		for j := uint32(1); j <= 4; j++ {
			if j != i {
				nbrs = append(nbrs, j)
			}
		}
		_ = gd.SetNeighbors(i, nbrs)
	}

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.DetectCommunities(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, res.CommunityCount, "complete graph must be one community")
}

func TestGraphAnalytics_Communities_EmptyGraph(t *testing.T) {
	gd := newTestGraphData(4)
	ga := NewGraphAnalytics(func() *types.GraphData { return gd })

	res, err := ga.DetectCommunities(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 0, res.CommunityCount)
	assert.Empty(t, res.Labels)
}

func TestGraphAnalytics_Communities_Deterministic(t *testing.T) {
	// LPA skips shuffling, so repeated runs on the same graph must produce
	// bit-identical labelings.
	gd := newTestGraphData(8)
	_ = gd.SetNeighbors(1, []uint32{2, 3})
	_ = gd.SetNeighbors(2, []uint32{1, 3})
	_ = gd.SetNeighbors(3, []uint32{1, 2, 4})
	_ = gd.SetNeighbors(4, []uint32{3, 5})
	_ = gd.SetNeighbors(5, []uint32{4, 6, 7})
	_ = gd.SetNeighbors(6, []uint32{5, 8})
	_ = gd.SetNeighbors(7, []uint32{5, 8})
	_ = gd.SetNeighbors(8, []uint32{6, 7})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	first, err := ga.DetectCommunities(context.Background(), 20)
	require.NoError(t, err)
	for run := 0; run < 5; run++ {
		again, err := ga.DetectCommunities(context.Background(), 20)
		require.NoError(t, err)
		require.Equal(t, first.CommunityCount, again.CommunityCount, "run %d", run)
		require.Equal(t, len(first.Labels), len(again.Labels), "run %d", run)
		for id, l := range first.Labels {
			require.Equal(t, l, again.Labels[id], "run %d: node %d", run, id)
		}
	}
}

func TestGraphAnalytics_Communities_MaxIterations(t *testing.T) {
	gd := newTestGraphData(4)
	_ = gd.SetNeighbors(1, []uint32{2})
	_ = gd.SetNeighbors(2, []uint32{1, 3})
	_ = gd.SetNeighbors(3, []uint32{2, 4})
	_ = gd.SetNeighbors(4, []uint32{3})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })

	// maxIter <= 0 runs zero iterations: every node keeps its own label.
	res, err := ga.DetectCommunities(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, 4, res.CommunityCount)
	for id := uint32(1); id <= 4; id++ {
		require.Equal(t, id, res.Labels[id], "node %d keeps its initial label", id)
	}
}

// TestGraphAnalytics_Communities_LeafTargets ensures nodes whose neighbors are
// all excluded from the label set (out-degree-less leaves) keep their own
// label instead of adopting phantom community 0.
func TestGraphAnalytics_Communities_LeafTargets(t *testing.T) {
	gd := newTestGraphData(3)
	// Node 1 points at leaf 2; node 2 has no outgoing edges (not labeled).
	_ = gd.SetNeighbors(1, []uint32{2})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.DetectCommunities(context.Background(), 10)
	require.NoError(t, err)

	require.Len(t, res.Labels, 1, "only nodes with outgoing edges are labeled")
	require.Equal(t, uint32(1), res.Labels[1], "node must keep its own label")
	require.Equal(t, 1, res.CommunityCount)
	// Structural invariant: every label value must refer to a labeled node.
	for id, l := range res.Labels {
		_, ok := res.Labels[l]
		require.True(t, ok, "node %d has label %d which belongs to no node", id, l)
	}
}

func TestGraphAnalytics_NilGraph(t *testing.T) {
	ga := NewGraphAnalytics(func() *types.GraphData { return nil })

	_, err := ga.AnalyzeProperties(context.Background())
	require.Error(t, err)

	_, err = ga.CalculatePageRank(context.Background(), DefaultPageRankConfig())
	require.Error(t, err)

	_, err = ga.DetectCommunities(context.Background(), 5)
	require.Error(t, err)
}

// --- PageRank accuracy tests ---

func TestGraphAnalytics_PageRank_SumsToOne(t *testing.T) {
	// Directed chain with a dangling sink: 1 -> 2 -> 3 (3 dangles).
	gd := newTestGraphData(3)
	_ = gd.SetNeighbors(1, []uint32{2})
	_ = gd.SetNeighbors(2, []uint32{3})
	_ = gd.SetNeighbors(3, nil)

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	res, err := ga.CalculatePageRank(context.Background(), DefaultPageRankConfig())
	require.NoError(t, err)

	require.Len(t, res.Scores, 3)
	var sum float64
	for _, s := range res.Scores {
		require.Greater(t, s, float32(0), "every valid node must have positive rank")
		sum += float64(s)
	}
	assert.InDelta(t, 1.0, sum, 1e-3, "PageRank mass must be conserved")

	// Flow ordering: the sink accumulates the most rank, the source the least.
	assert.Greater(t, res.Scores[3], res.Scores[2])
	assert.Greater(t, res.Scores[2], res.Scores[1])
}

func TestGraphAnalytics_PageRank_ZeroIterations(t *testing.T) {
	gd := newTestGraphData(4)
	_ = gd.SetNeighbors(1, []uint32{2, 3, 4})
	_ = gd.SetNeighbors(2, []uint32{1})
	_ = gd.SetNeighbors(3, []uint32{1})
	_ = gd.SetNeighbors(4, []uint32{1})

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	cfg := DefaultPageRankConfig()
	cfg.MaxIterations = 0
	res, err := ga.CalculatePageRank(context.Background(), cfg)
	require.NoError(t, err)

	// No iterations run: uniform initialization 1/N for every node.
	require.Len(t, res.Scores, 4)
	for id := uint32(1); id <= 4; id++ {
		assert.InDelta(t, 0.25, res.Scores[id], 1e-6, "node %d", id)
	}
}

// FuzzGraphAnalytics_DetectCommunities checks LPA invariants on arbitrary
// graphs: no errors, deterministic output, community count equals the number
// of unique labels, labels only ever refer to labeled nodes, and exactly the
// nodes with outgoing edges participate.
func FuzzGraphAnalytics_DetectCommunities(f *testing.F) {
	f.Add(uint32(6), uint32(2), uint8(5))
	f.Add(uint32(1), uint32(1), uint8(1))
	f.Add(uint32(30), uint32(4), uint8(10))
	f.Add(uint32(50), uint32(1), uint8(0))

	f.Fuzz(func(t *testing.T, numNodes uint32, maxNeighbors uint32, maxIter uint8) {
		if numNodes < 1 || numNodes > 200 {
			t.Skip()
		}
		if maxNeighbors < 1 || maxNeighbors > 20 {
			t.Skip()
		}

		gd := newTestGraphData(int(numNodes) + 2)
		hasOutEdges := make(map[uint32]bool, numNodes)
		for i := uint32(1); i <= numNodes; i++ {
			neighbors := make([]uint32, 0, maxNeighbors)
			for j := uint32(0); j < maxNeighbors; j++ {
				nbr := (i*31+j*17)%(numNodes+1) + 1
				if nbr != i {
					neighbors = append(neighbors, nbr)
				}
			}
			if len(neighbors) > 0 {
				hasOutEdges[i] = true
				_ = gd.SetNeighbors(i, neighbors)
			}
		}

		ga := NewGraphAnalytics(func() *types.GraphData { return gd })
		res, err := ga.DetectCommunities(context.Background(), int(maxIter))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 1. Community count must equal the number of unique labels.
		unique := make(map[uint32]bool)
		for _, l := range res.Labels {
			unique[l] = true
		}
		if res.CommunityCount != len(unique) {
			t.Fatalf("CommunityCount=%d but %d unique labels", res.CommunityCount, len(unique))
		}

		// 2. Exactly the nodes with outgoing edges participate.
		if len(res.Labels) != len(hasOutEdges) {
			t.Fatalf("expected %d labeled nodes, got %d", len(hasOutEdges), len(res.Labels))
		}
		for id := range hasOutEdges {
			if _, ok := res.Labels[id]; !ok {
				t.Fatalf("node %d has outgoing edges but no label", id)
			}
		}

		// 3. Labels only ever refer to labeled nodes (no phantom communities).
		for id, l := range res.Labels {
			if _, ok := res.Labels[l]; !ok {
				t.Fatalf("node %d carries label %d which belongs to no node", id, l)
			}
		}

		// 4. Determinism: a second run must be identical.
		res2, err := ga.DetectCommunities(context.Background(), int(maxIter))
		if err != nil {
			t.Fatalf("unexpected error on rerun: %v", err)
		}
		if res2.CommunityCount != res.CommunityCount || len(res2.Labels) != len(res.Labels) {
			t.Fatalf("nondeterministic result: %+v vs %+v", res, res2)
		}
		for id, l := range res.Labels {
			if res2.Labels[id] != l {
				t.Fatalf("nondeterministic label for node %d: %d vs %d", id, l, res2.Labels[id])
			}
		}
	})
}

func BenchmarkGraphAnalytics_PageRank_Prefetch(b *testing.B) {
	numNodes := 10000
	maxNeighbors := 20
	gd := types.NewGraphData(numNodes+100, 2, false, false, -1, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

	for i := 1; i <= numNodes; i++ {
		neighbors := make([]uint32, 0, maxNeighbors)
		for j := 0; j < maxNeighbors; j++ {
			// Pseudo-random but deterministic links to cause cache misses
			nbr := uint32((i*j*17)%numNodes + 1)
			if nbr != uint32(i) {
				neighbors = append(neighbors, nbr)
			}
		}
		_ = gd.SetNeighbors(uint32(i), neighbors)
	}

	ga := NewGraphAnalytics(func() *types.GraphData { return gd })
	config := DefaultPageRankConfig()
	config.MaxIterations = 5 // Fast enough for bench

	b.ResetTimer()
	for b.Loop() {
		_, err := ga.CalculatePageRank(context.Background(), config)
		if err != nil {
			b.Fatal(err)
		}
	}
}
