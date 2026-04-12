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

var (
	// PQTrainingDuration measures latency of Product Quantization training
	PQTrainingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_pq_training_duration_seconds",
			Help:    "Latency of PQ training operations",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
		},
		[]string{"dataset", "dimension", "subspaces"},
	)

	// PQEncodingDuration measures latency of Product Quantization encoding
	PQEncodingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_pq_encoding_duration_seconds",
			Help:    "Latency of PQ encoding operations",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5},
		},
		[]string{"dataset", "vector_count"},
	)

	// PQOperationsTotal tracks total PQ operations
	PQOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_pq_operations_total",
			Help: "Total number of PQ operations",
		},
		[]string{"dataset", "operation", "status"}, // operation: "train", "encode"
	)
)
