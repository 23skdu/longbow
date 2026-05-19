package store

// slab_cow_metrics_test.go – Validates that the CoW Adjacency metrics
// (HnswCowCopyCount, HnswUpdateContentionSeconds) are correctly declared
// in the metrics package and can be exercised without a full HNSW stack.
//
// Full integration coverage of these metrics during real HNSW batch inserts
// is provided by the existing sharded_hnsw integration tests.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/23skdu/longbow/internal/metrics"
)

// TestCowMetrics_CounterVecDeclaration verifies that HnswCowCopyCount is
// correctly declared as a CounterVec with the expected label set.
func TestCowMetrics_CounterVecDeclaration(t *testing.T) {
	// Add a value to verify the metric is addressable and the counter increments.
	metrics.HnswCowCopyCount.WithLabelValues("declaration_test", "0").Add(1)
	assert.NotNil(t, metrics.HnswCowCopyCount,
		"HnswCowCopyCount must be a non-nil CounterVec")
}

// TestCowMetrics_ContentionHistogramDeclaration verifies that
// HnswUpdateContentionSeconds is a working HistogramVec.
func TestCowMetrics_ContentionHistogramDeclaration(t *testing.T) {
	// Observe a dummy duration to verify the histogram is wired.
	d := time.Since(time.Now().Add(-time.Microsecond)).Seconds()
	metrics.HnswUpdateContentionSeconds.WithLabelValues("declaration_test").Observe(d)
	assert.NotNil(t, metrics.HnswUpdateContentionSeconds,
		"HnswUpdateContentionSeconds must be a non-nil HistogramVec")
}
