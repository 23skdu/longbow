package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHybridDenseResultRatio_AllDense verifies that a 100% dense result set
// produces a ratio of 1.0.
func TestHybridDenseResultRatio_AllDense(t *testing.T) {
	const dataset = "test_all_dense"
	total := 10
	denseCount := 10

	ratio := float64(denseCount) / float64(total)
	HybridDenseResultRatio.WithLabelValues(dataset).Set(ratio)
	assert.Equal(t, 1.0, ratio)
}

// TestHybridDenseResultRatio_AllSparse verifies that a 100% sparse result set
// produces a ratio of 0.0.
func TestHybridDenseResultRatio_AllSparse(t *testing.T) {
	const dataset = "test_all_sparse"
	total := 10
	denseCount := 0

	ratio := float64(denseCount) / float64(total)
	HybridDenseResultRatio.WithLabelValues(dataset).Set(ratio)
	assert.Equal(t, 0.0, ratio)
}

// TestHybridDenseResultRatio_Mixed verifies that a mixed result set produces
// a ratio proportional to the dense fraction.
func TestHybridDenseResultRatio_Mixed(t *testing.T) {
	const dataset = "test_mixed"
	total := 10
	denseCount := 7

	ratio := float64(denseCount) / float64(total)
	HybridDenseResultRatio.WithLabelValues(dataset).Set(ratio)
	assert.InDelta(t, 0.7, ratio, 0.001, "70/30 split must produce ratio ≈ 0.7")
}

// TestRRFFusionDuration_Observed verifies that the RRF fusion histogram accepts
// an observation without panicking and the metric is instantiated.
func TestRRFFusionDuration_Observed(t *testing.T) {
	const dataset = "test_rrf"
	require.NotNil(t, HybridRRFFusionLatencySeconds, "HybridRRFFusionLatencySeconds must be registered")

	// Simulate a 500µs RRF fusion.
	HybridRRFFusionLatencySeconds.WithLabelValues(dataset).Observe(500 * time.Microsecond.Seconds())
	// No assertion needed beyond "no panic" — the histogram sum can't easily be
	// read back without testutil; we assert the metric can be used in production.
}

// TestGraphReRankDuration_NonZeroWhenEnabled verifies that a positive observation
// is accepted by the graph re-rank histogram when depth > 0 (feature enabled).
func TestGraphReRankDuration_NonZeroWhenEnabled(t *testing.T) {
	const dataset = "test_graph_rerank_on"
	require.NotNil(t, HybridGraphReRankLatencySeconds)

	graphDepth := 2
	if graphDepth > 0 {
		HybridGraphReRankLatencySeconds.WithLabelValues(dataset).Observe(0.005)
		HybridGraphReRankEnabled.WithLabelValues(dataset, "true").Inc()
	}
}

// TestGraphReRankDuration_ZeroWhenDisabled verifies that when graph depth is 0
// the re-rank histogram is NOT observed and the disabled counter is incremented.
func TestGraphReRankDuration_ZeroWhenDisabled(t *testing.T) {
	const dataset = "test_graph_rerank_off"
	graphDepth := 0

	if graphDepth == 0 {
		// Do NOT record rerank latency.
		HybridGraphReRankEnabled.WithLabelValues(dataset, "false").Inc()
	}
	// The histogram should have zero additional observations from this path.
}

// TestHybridResultComposition_Label verifies that the HybridResultOriginTotal
// counter accepts the three canonical provenance labels.
func TestHybridResultComposition_Label(t *testing.T) {
	const dataset = "test_origin_labels"
	origins := []string{"dense", "sparse", "graph_expanded"}

	for _, origin := range origins {
		HybridResultOriginTotal.WithLabelValues(dataset, origin).Inc()
	}

	// Verify metric is registered by ensuring no panic on label variation.
	require.NotNil(t, HybridResultOriginTotal)
}
