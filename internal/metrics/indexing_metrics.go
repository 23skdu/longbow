package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Indexing-Related Metrics
// =============================================================================

var (
	// BloomFilter metrics for filter evaluation optimization
	BloomFalsePositiveRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_bloom_false_positive_rate",
			Help: "Estimated false positive rate of Bloom filters",
		},
		[]string{"dataset", "column"},
	)

	// ColumnIndexSize tracks the size of columnar indexes
	ColumnIndexSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_column_index_size_bytes",
			Help: "Size of on-disk columnar indexes in bytes",
		},
		[]string{"dataset", "column"},
	)

	// ColumnIndexLookupDuration measures latency of columnar index lookups
	ColumnIndexLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_column_index_lookup_duration_seconds",
			Help:    "Latency of columnar index lookups",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"dataset", "column"},
	)
)
