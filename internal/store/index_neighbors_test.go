package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/apache/arrow-go/v18/arrow"
)

func TestIndexGetNeighborsStandardized(t *testing.T) {
	ctx := context.Background()
	dim := 128

	// Create a minimal dataset for tests
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := NewDataset("test", schema)

	// Test case: ArrowHNSW
	t.Run("ArrowHNSW", func(t *testing.T) {
		idx := NewTestHNSWIndex(ds)
		idx.dims.Store(int32(dim))

		// Get neighbors for ID 0 (empty index)
		neighbors, err := idx.GetNeighbors(ctx, 0, 5)
		require.NoError(t, err)
		assert.Empty(t, neighbors)
	})

	// Test case: AdaptiveIndex (BruteForce initially)
	t.Run("AdaptiveIndex_BruteForce", func(t *testing.T) {
		idx := NewAdaptiveIndex(ds, DefaultAdaptiveIndexConfig())
		
		// BruteForce doesn't support GetNeighbors
		neighbors, err := idx.GetNeighbors(ctx, 0, 5)
		assert.Error(t, err)
		assert.Nil(t, neighbors)
	})

	// Test case: IVFPQIndex
	t.Run("IVFPQIndex", func(t *testing.T) {
		idx, _ := NewIVFPQIndex(dim, DefaultIVFPQConfig())
		
		// IVFPQ doesn't support GetNeighbors
		neighbors, err := idx.GetNeighbors(ctx, 0, 5)
		assert.Error(t, err)
		assert.Nil(t, neighbors)
	})
}
