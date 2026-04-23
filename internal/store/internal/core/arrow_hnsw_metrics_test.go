package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"testing"

	basecore "github.com/23skdu/longbow/internal/core"
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
	rec := MakeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &MockDataset{
		Name:    "arrow_metrics_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	t.Run("Cosine", func(t *testing.T) {
		config := types.DefaultArrowHNSWConfig()
		config.M = 16
		config.EfConstruction = 100
		config.Metric = basecore.MetricCosine

		idx := NewArrowHNSW(ds, &config, nil)

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
		rec := MakeBatchTestRecord(mem, 4, vectors)
		defer rec.Release()

		ds := &MockDataset{
			Records: []arrow.RecordBatch{rec},
		}

		// Create ArrowHNSW
		config := types.DefaultArrowHNSWConfig()
		idx := NewArrowHNSW(ds, &config, nil)

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
		configF16 := types.DefaultArrowHNSWConfig()
		configF16.Float16Enabled = true
		idxF16 := NewArrowHNSW(ds, &configF16, nil)

		// Must add a vector so search doesn't early return
		_, err = idxF16.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)

		_, err = idxF16.Search(context.Background(), q, 10, nil)
		require.NoError(t, err)

		countF16 := testutil.ToFloat64(metrics.HNSWPolymorphicSearchCount.WithLabelValues("float16"))
		assert.Equal(t, 1.0, countF16, "Should record 1 float16 search")
	})

	t.Run("SearchLayerSampling", func(t *testing.T) {
		config := types.DefaultArrowHNSWConfig()
		config.SearchLayerSampleRate = 1.0 // 100% sampling
		idx := NewArrowHNSW(ds, &config, nil)
		
		// Add vector to ensure search has work to do
		_, err := idx.AddByLocation(context.Background(), 0, 0)
		require.NoError(t, err)

		// Record initial distance calculation count
		initialDistCalcs := testutil.ToFloat64(metrics.HnswDistanceCalculations)

		// Search
		q := []float32{1.0, 0.0}
		_, err = idx.Search(context.Background(), q, 1, nil)
		require.NoError(t, err)

		// Verify distance calculations (always recorded)
		finalDistCalcs := testutil.ToFloat64(metrics.HnswDistanceCalculations)
		assert.Greater(t, finalDistCalcs, initialDistCalcs, "Should increase distance calculations")

		// Verify nodes visited (sampled at 1.0)
		// We check if any observations were recorded
		assert.Greater(t, testutil.CollectAndCount(metrics.HnswNodesVisited), 0, "Should record nodes visited when sampled at 1.0")

		// Test with 0% sampling
		// Note: HnswNodesVisited is a global, so we can't easily reset it.
		// We'll track the count before and after.
		initialObsCount := testutil.CollectAndCount(metrics.HnswNodesVisited)
		
		configNoSample := types.DefaultArrowHNSWConfig()
		configNoSample.SearchLayerSampleRate = 0.0000001 // Practically 0
		idxNoSample := NewArrowHNSW(ds, &configNoSample, nil)

		_, err = idxNoSample.Search(context.Background(), q, 1, nil)
		require.NoError(t, err)

		finalObsCount := testutil.CollectAndCount(metrics.HnswNodesVisited)
		assert.Equal(t, initialObsCount, finalObsCount, "Should NOT increase observations when sampled at near-zero")
	})
}
