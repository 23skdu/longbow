package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_ObservabilityMetrics(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 0.0}, {0.0, 1.0}}
	dims := 2
	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "observability_test",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	// Enable something that triggers the 'useRefinement' flag if we want to test refinement throughput
	// useRefinement := (h.config.SQ8Enabled || h.config.PQEnabled || h.config.BQEnabled) && h.config.RefinementFactor > 1.0
	config.SQ8Enabled = true
	config.RefinementFactor = 2.0

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	// Add vectors
	_, err := idx.AddByLocation(0, 0)
	require.NoError(t, err)
	_, err = idx.AddByLocation(0, 1)
	require.NoError(t, err)

	// Reset metrics before search
	metrics.HNSWSearchLatencyByType.Reset()
	metrics.HNSWSearchLatencyByDim.Reset()
	metrics.HNSWRefineThroughput.Reset()

	// Search
	q := []float32{1.0, 0.0}
	_, err = idx.Search(q, 1, 10, nil)
	require.NoError(t, err)

	// Verify Metrics
	typeLabel := "sq8" // Based on config.SQ8Enabled
	// Latency metrics are histograms, we can check if they were collected
	countByType := testutil.CollectAndCount(metrics.HNSWSearchLatencyByType)
	assert.GreaterOrEqual(t, countByType, 1, "Should record search latency by type")

	countByDim := testutil.CollectAndCount(metrics.HNSWSearchLatencyByDim)
	assert.GreaterOrEqual(t, countByDim, 1, "Should record search latency by dim")

	// Refine throughput
	refineCount := testutil.ToFloat64(metrics.HNSWRefineThroughput.WithLabelValues(typeLabel))
	// targetK = k * RefinementFactor = 1 * 2 = 2. But we only have 2 points total.
	// So results should be at most 2.
	assert.Greater(t, refineCount, 0.0, "Should record refinement throughput")
}
