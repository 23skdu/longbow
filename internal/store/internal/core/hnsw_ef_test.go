package core_test

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_EfParameter(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create a dataset with multiple vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.9, 0.1, 0.0},
		{0.8, 0.2, 0.0},
		{0.7, 0.3, 0.0},
		{0.6, 0.4, 0.0},
		{0.5, 0.5, 0.0},
		{0.4, 0.6, 0.0},
		{0.3, 0.7, 0.0},
		{0.2, 0.8, 0.0},
		{0.1, 0.9, 0.0},
	}
	dims := 3
	rec := core.MakeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &core.MockDataset{
		Name:    "ef_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	cfg := types.DefaultArrowHNSWConfig()
	cfg.Metric = basecore.MetricEuclidean
	idx := core.NewArrowHNSW(ds, &cfg, nil)

	// Add all vectors
	for i := 0; i < len(vectors); i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search for [1.0, 0.0, 0.0]
	query := []float32{1.0, 0.0, 0.0}
	k := 5

	t.Run("DefaultEf", func(t *testing.T) {
		// Use default ef (from config)
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{})
		require.NoError(t, err)
		require.Len(t, res, k)
		t.Logf("Default ef search results: %v", res)
	})

	t.Run("CustomEf_Larger", func(t *testing.T) {
		// Use larger ef (should search more broadly)
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{Ef: 100})
		require.NoError(t, err)
		require.Len(t, res, k)
		t.Logf("Ef=100 search results: %v", res)
	})

	t.Run("CustomEf_Smaller", func(t *testing.T) {
		// Use smaller ef (should search more narrowly)
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{Ef: 10})
		require.NoError(t, err)
		require.Len(t, res, k)
		t.Logf("Ef=10 search results: %v", res)
	})

	t.Run("EfZeroUsesDefault", func(t *testing.T) {
		// Ef=0 should use default from config
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{Ef: 0})
		require.NoError(t, err)
		require.Len(t, res, k)
		t.Logf("Ef=0 search results: %v", res)
	})

	t.Run("NegativeEfUsesDefault", func(t *testing.T) {
		// Negative ef should use default from config
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{Ef: -1})
		require.NoError(t, err)
		require.Len(t, res, k)
		t.Logf("Ef=-1 search results: %v", res)
	})

	// Verify that results are reasonable (closest vectors should be first)
	t.Run("ResultOrder", func(t *testing.T) {
		res, err := idx.SearchVectors(context.Background(), query, k, nil, types.SearchOptions{Ef: 50})
		require.NoError(t, err)
		require.Len(t, res, k)

		// First result should be the closest vector (ID 0)
		assert.Equal(t, uint32(0), uint32(res[0].ID))
		assert.InDelta(t, 0.0, res[0].Distance, 1e-6)
	})
}
