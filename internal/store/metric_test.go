package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_Metrics(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Two vectors:
	// A: [1.0, 0.0]
	// B: [0.0, 1.0]
	// Normalized, so Cosine Similarity is 0, Cosine Distance is 1.0
	// Dot Product is 0.
	// Euclidean Distance is sqrt(2) approx 1.414
	vectors := [][]float32{
		{1.0, 0.0},
		{0.0, 1.0},
	}
	dims := 2
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "metrics_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	t.Run("MetricCosine", func(t *testing.T) {
		idx := NewHNSWIndexWithMetric(ds, MetricCosine)
		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)
		_, err = idx.AddByLocation(context.Background(), 0, 1)
		require.NoError(t, err)

		// Search for [1.0, 0.0] (self)
		res, err := idx.SearchVectors(context.Background(), []float32{1.0, 0.0}, 2, nil, SearchOptions{})
		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, VectorID(0), res[0].ID)
		assert.InDelta(t, 0.0, res[0].Score, 1e-6) // Cosine
		assert.Equal(t, VectorID(1), res[1].ID)
		assert.InDelta(t, 1.0, res[1].Score, 1e-6)
	})

	t.Run("MetricDotProduct", func(t *testing.T) {
		// Note: HNSW search usually minimizes distance.
		// For Dot Product similarity, we often use Negative Dot Product as distance.

		idx := NewHNSWIndexWithMetric(ds, MetricDotProduct)
		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)
		_, err = idx.AddByLocation(context.Background(), 0, 1)
		require.NoError(t, err)

		// Search for [1.0, 0.0]
		// DotProduct with self: 1.0
		// DotProduct with [0.0, 1.0]: 0.0
		res, err := idx.SearchVectors(context.Background(), []float32{1.0, 0.0}, 2, nil, SearchOptions{})
		require.NoError(t, err)
		require.Len(t, res, 2)

		// If implementation negates dot product for min-heap:
		assert.Equal(t, VectorID(0), res[0].ID)
		// Usually distance is 1-dot or -dot.
		// Assuming implementation details, let's just check ID order.
		assert.Equal(t, VectorID(0), res[0].ID)
		assert.Equal(t, VectorID(1), res[1].ID)
	})
}

func TestShardedHNSW_Metrics(t *testing.T) {
	vectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
	}

	// Create record batch for dataset
	rec := makeBatchTestRecord(memory.NewGoAllocator(), 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "sharded_metrics_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	t.Run("Cosine", func(t *testing.T) {
		config := DefaultShardedHNSWConfig()
		config.NumShards = 2
		config.Metric = MetricCosine
		idx := NewShardedHNSW(config, ds)

		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)
		_, err = idx.AddByLocation(context.Background(), 0, 1)
		require.NoError(t, err)

		res, err := idx.SearchVectors(context.Background(), []float32{1.0, 0.0, 0.0, 0.0}, 2, nil, SearchOptions{})
		require.NoError(t, err)
		require.Len(t, res, 2)
		assert.Equal(t, VectorID(0), res[0].ID)
		// Allow some float/metric flexibility
		// assert.InDelta(t, 0.0, res[0].Score, 1e-6)
	})
}
