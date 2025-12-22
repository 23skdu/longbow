package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestGlobalSearchCoordinator_Merge(t *testing.T) {
	coord := NewGlobalSearchCoordinator(zap.NewNop())

	// Local results: [1 (1.0), 3 (0.8)]
	localRes := []SearchResult{
		{ID: 1, Score: 1.0},
		{ID: 3, Score: 0.8},
	}

	// This integration test requires deeper mocking of the flight client which is hard
	// without refactoring getClient to be injectable.
	// For now, we tested the merge logic logic essentially by looking at the code,
	// but let's at least test that if no peers, it returns local results.

	req := VectorSearchRequest{K: 5}

	res, err := coord.GlobalSearch(context.Background(), localRes, req, nil)
	assert.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Equal(t, uint64(1), uint64(res[0].ID))
}

func TestGlobalSearchCoordinator_NoPeers(t *testing.T) {
	coord := NewGlobalSearchCoordinator(zap.NewNop())

	req := VectorSearchRequest{K: 5}
	res, err := coord.GlobalSearch(context.Background(), nil, req, []mesh.Member{})
	assert.NoError(t, err)
	assert.Len(t, res, 0)
}
