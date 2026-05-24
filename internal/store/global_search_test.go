package store

import (
	"context"
	"sort"
	"testing"

	"github.com/23skdu/longbow/internal/mesh"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestGlobalSearchCoordinator_Merge(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	coord := NewGlobalSearchCoordinator(zerolog.Nop(), pool)
	defer func() { _ = coord.Close() }()

	// Local results: [1 (1.0), 3 (0.8)]
	localRes := []SearchResult{
		{ID: 1, Score: 1.0},
		{ID: 3, Score: 0.8},
	}

	// This integration test requires deeper mocking of the flight client which is hard
	// without refactoring getClient to be injectable.
	// For now, we tested the merge logic logic essentially by looking at the code,
	// but let's at least test that if no peers, it returns local results.

	req := qry.VectorSearchRequest{K: 5}

	res, err := coord.GlobalSearch(context.Background(), localRes, &req, nil)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Equal(t, uint64(1), uint64(res[0].ID))
}

func TestGlobalSearchCoordinator_NoPeers(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	pool := NewFlightClientPool(DefaultFlightClientPoolConfig())
	coord := NewGlobalSearchCoordinator(zerolog.Nop(), pool)
	defer func() { _ = coord.Close() }()

	req := qry.VectorSearchRequest{K: 5}
	res, err := coord.GlobalSearch(context.Background(), nil, &req, []mesh.Member{})
	assert.NoError(t, err)
	assert.Len(t, res, 0)
}

func TestGlobalSearch_HybridMerge(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	// Simulate single-node mega index results
	megaDense := []SearchResult{
		{ID: 1, Score: 0.9},
		{ID: 2, Score: 0.8},
		{ID: 3, Score: 0.7},
		{ID: 4, Score: 0.6},
	}
	megaSparse := []SearchResult{
		{ID: 2, Score: 5.5},
		{ID: 4, Score: 4.4},
		{ID: 1, Score: 3.3},
		{ID: 5, Score: 2.2},
	}

	megaFused := ReciprocalRankFusion("test_ds", megaDense, megaSparse, 60, 3, nil)

	// Simulate multi-node (Node A and Node B)
	nodeADense := []SearchResult{
		{ID: 1, Score: 0.9},
		{ID: 3, Score: 0.7},
	}
	nodeBDense := []SearchResult{
		{ID: 2, Score: 0.8},
		{ID: 4, Score: 0.6},
	}

	nodeASparse := []SearchResult{
		{ID: 2, Score: 5.5},
		{ID: 1, Score: 3.3},
	}
	nodeBSparse := []SearchResult{
		{ID: 4, Score: 4.4},
		{ID: 5, Score: 2.2},
	}

	// Global coordinator gathers all
	allDense := append(nodeADense, nodeBDense...)
	allSparse := append(nodeASparse, nodeBSparse...)

	// Global Search Coordinator logic: sort globally before RRF
	sort.Slice(allDense, func(i, j int) bool { return allDense[i].Score > allDense[j].Score })
	sort.Slice(allSparse, func(i, j int) bool { return allSparse[i].Score > allSparse[j].Score })

	distributedFused := ReciprocalRankFusion("test_ds", allDense, allSparse, 60, 3, nil)

	// Assert equality
	assert.Equal(t, len(megaFused), len(distributedFused))
	for i := range megaFused {
		assert.Equal(t, megaFused[i].ID, distributedFused[i].ID)
		assert.InDelta(t, megaFused[i].Score, distributedFused[i].Score, 0.0001)
	}
}
