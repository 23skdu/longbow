package index

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMortonEncoding(t *testing.T) {
	// 2D Morton tests
	assert.Equal(t, uint64(0), MortonEncode2D(0, 0))
	assert.Equal(t, uint64(1), MortonEncode2D(1, 0))
	assert.Equal(t, uint64(2), MortonEncode2D(0, 1))
	assert.Equal(t, uint64(3), MortonEncode2D(1, 1))

	// 3D Morton tests
	assert.Equal(t, uint64(0), MortonEncode3D(0, 0, 0))
	assert.Equal(t, uint64(1), MortonEncode3D(1, 0, 0))
	assert.Equal(t, uint64(2), MortonEncode3D(0, 1, 0))
	assert.Equal(t, uint64(4), MortonEncode3D(0, 0, 1))
	assert.Equal(t, uint64(7), MortonEncode3D(1, 1, 1))
}

func TestHNSW_CompactGraphLayoutParity(t *testing.T) {
	numVecs := 100
	dims := 32

	dataset, err := makeBenchmarkMockDataset(numVecs, dims)
	require.NoError(t, err)

	config := types.DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 64
	config.EfSearch = 32

	h := NewArrowHNSW(dataset, &config, nil)
	ctx := context.Background()

	for i := 0; i < numVecs; i++ {
		_, err := h.AddByLocation(ctx, 0, i)
		require.NoError(t, err)
	}

	query := make([]float32, dims)
	for d := 0; d < dims; d++ {
		query[d] = 0.5
	}

	// Baseline search results before compaction
	resBefore, err := h.Search(ctx, query, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, resBefore)

	// Run CompactGraphLayout
	err = CompactGraphLayout(h)
	require.NoError(t, err)

	// Search results after compaction
	resAfter, err := h.Search(ctx, query, 10, nil)
	require.NoError(t, err)
	require.Equal(t, len(resBefore), len(resAfter))

	// Verify top-1 matches exactly and overall distance quality is identical
	assert.Equal(t, resBefore[0].ID, resAfter[0].ID)
	assert.InDelta(t, resBefore[0].Dist, resAfter[0].Dist, 1e-4)

	t.Logf("Before compact: top-1 ID=%d dist=%.4f", resBefore[0].ID, resBefore[0].Dist)
	t.Logf("After compact:  top-1 ID=%d dist=%.4f", resAfter[0].ID, resAfter[0].Dist)
}
