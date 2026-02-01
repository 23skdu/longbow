package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphNavigator_Caching(t *testing.T) {
	g := createTestGraph(t)
	// 0 -> 1 -> 2
	require.NoError(t, g.SetNeighbors(0, []uint32{1}))
	require.NoError(t, g.SetNeighbors(1, []uint32{2}))
	require.NoError(t, g.SetNeighbors(2, []uint32{}))

	reg := prometheus.NewRegistry()
	nav := NewGraphNavigator("test", func() *types.GraphData { return g }, NavigatorConfig{
		MaxHops:       10,
		EnableCaching: true,
	}, reg)
	require.NoError(t, nav.Initialize())

	ctx := context.Background()
	query := NavigatorQuery{
		StartID:  0,
		TargetID: 2,
		MaxHops:  5,
	}

	// First query: Cache Miss
	path, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.True(t, path.Found)

	// Verify metrics
	// We can inspect metrics via gathering or just trust logic if helper not available.
	// Since we passed reg, we can try to gather.
	m, err := reg.Gather()
	require.NoError(t, err)

	// Helper to find metric value
	getMetric := func(name string) float64 {
		for _, fam := range m {
			if fam.GetName() == name {
				return fam.GetMetric()[0].GetCounter().GetValue()
			}
		}
		return 0
	}

	assert.Equal(t, 1.0, getMetric("graph_navigator_cache_misses_total"), "Expected 1 cache miss")
	assert.Equal(t, 0.0, getMetric("graph_navigator_cache_hits_total"), "Expected 0 cache hits")

	// Second query: Cache Hit
	path2, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.True(t, path2.Found)

	m, _ = reg.Gather()
	assert.Equal(t, 1.0, getMetric("graph_navigator_cache_misses_total"), "Expected 1 cache miss")
	assert.Equal(t, 1.0, getMetric("graph_navigator_cache_hits_total"), "Expected 1 cache hit")

	// Modify graph: Change 1 -> 3 (break path to 2)
	// This updates GlobalVersion, invalidating cache.
	require.NoError(t, g.SetNeighbors(1, []uint32{3}))

	// Third query: Cache Miss (invalidated) + Path Not Found
	path3, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.False(t, path3.Found) // Path 0->1->3 (target 2 not found)

	m, _ = reg.Gather()
	assert.Equal(t, 2.0, getMetric("graph_navigator_cache_misses_total"), "Expected 2 cache misses")
	assert.Equal(t, 1.0, getMetric("graph_navigator_cache_hits_total"), "Expected 1 cache hit")

	// Fourth query: Cache Hit (cached "not found" result? No, findPath stores success only?
	// implementation: "if gn.searchConfig.EnableCaching && path.Found".
	// So failed paths are NOT cached.

	// Re-run query - should result in another MISS because "Found=false" wasn't cached.
	path4, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.False(t, path4.Found)

	m, _ = reg.Gather()
	assert.Equal(t, 3.0, getMetric("graph_navigator_cache_misses_total"), "Expected 3 cache misses")

	// Restore path: 1 -> 2
	require.NoError(t, g.SetNeighbors(1, []uint32{2}))

	// Fifth query: Cache Miss (version changed again) + Found
	path5, err := nav.FindPath(ctx, query)
	require.NoError(t, err)
	assert.True(t, path5.Found)

	m, _ = reg.Gather()
	assert.Equal(t, 4.0, getMetric("graph_navigator_cache_misses_total"), "Expected 4 cache misses")

	// Sixth query: Cache Hit
	_, err = nav.FindPath(ctx, query)
	require.NoError(t, err)

	m, _ = reg.Gather()
	assert.Equal(t, 2.0, getMetric("graph_navigator_cache_hits_total"), "Expected 2 cache hits")
}
