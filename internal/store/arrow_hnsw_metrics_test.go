package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_Metrics(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Vectors: [1, 0] and [0, 1]
	vectors := [][]float32{
		{1.0, 0.0},
		{0.0, 1.0},
	}
	dims := 2
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "arrow_metrics_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	t.Run("Cosine", func(t *testing.T) {
		config := DefaultArrowHNSWConfig()
		config.M = 16
		config.EfConstruction = 100
		config.Metric = MetricCosine

		idx := NewArrowHNSW(ds, config)

		// Add vectors 0 and 1
		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)
		_, err = idx.AddByLocation(context.Background(), 0, 1)
		require.NoError(t, err)

		// Search for [1.0, 0.0]
		res, err := idx.Search(context.Background(), []float32{1.0, 0.0}, 2, nil)
		require.NoError(t, err)
		require.Len(t, res, 2)

		// Find ID 0
		foundSelf := false
		for _, r := range res {
			if r.ID == 0 {
				assert.InDelta(t, 0.0, r.Dist, 1e-6)
				foundSelf = true
			}
		}
		assert.True(t, foundSelf, "Should find self")
	})

	t.Run("PolymorphicMetrics", func(t *testing.T) {
		// Reset metrics
		metrics.HNSWPolymorphicSearchCount.Reset()
		metrics.HNSWPolymorphicLatency.Reset()
		metrics.HNSWPolymorphicThroughput.Reset()

		// Setup vector data
		mem := memory.NewGoAllocator()
		vectors := [][]float32{{0.5, 0.5, 0.5, 0.5}}
		rec := makeBatchTestRecord(mem, 4, vectors)
		defer rec.Release()

		ds := &Dataset{
			Records: []arrow.RecordBatch{rec},
		}

		// Create ArrowHNSW
		config := DefaultArrowHNSWConfig()
		idx := NewArrowHNSW(ds, config)

		// Add vector
		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)

		// Perform search
		q := []float32{0.5, 0.5, 0.5, 0.5}
		_, err = idx.Search(context.Background(), q, 10, nil)
		require.NoError(t, err)

		// Verify Metrics
		count := testutil.ToFloat64(metrics.HNSWPolymorphicSearchCount.WithLabelValues("float32"))
		assert.Equal(t, 1.0, count, "Should record 1 float32 search")

		throughput := testutil.ToFloat64(metrics.HNSWPolymorphicThroughput.WithLabelValues("float32"))
		assert.Greater(t, throughput, 0.0, "Throughput should be recorded")

		// Test Float16 configuration
		configF16 := DefaultArrowHNSWConfig()
		configF16.Float16Enabled = true
		idxF16 := NewArrowHNSW(ds, configF16)

		// Must add a vector so search doesn't early return
		_, err = idxF16.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)

		_, err = idxF16.Search(context.Background(), q, 10, nil)
		require.NoError(t, err)

		countF16 := testutil.ToFloat64(metrics.HNSWPolymorphicSearchCount.WithLabelValues("float16"))
		assert.Equal(t, 1.0, countF16, "Should record 1 float16 search")
	})
}
