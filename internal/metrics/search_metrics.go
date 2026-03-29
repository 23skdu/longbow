package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Search-Related Metrics
// =============================================================================

var (
	// SearchResultPool metrics
	SearchResultPoolGetTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_result_pool_get_total",
			Help: "Total number of result slices retrieved from the pool",
		},
		[]string{"capacity"}, // bucket by initial capacity
	)

	SearchResultPoolPutTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_result_pool_put_total",
			Help: "Total number of result slices returned to the pool",
		},
		[]string{"capacity"},
	)

	SearchResultPoolHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_result_pool_hits_total",
			Help: "Total number of pool hits (reused slices)",
		},
		[]string{"capacity"},
	)

	SearchResultPoolMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_result_pool_misses_total",
			Help: "Total number of pool misses (new allocations)",
		},
		[]string{"capacity"},
	)

	// General search latency metrics
	SearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_search_latency_seconds",
			Help:    "Latency of search operations by type",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"dataset", "type"},
	)

	// Batch search metrics
	BatchSearchRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_batch_search_requests_total",
			Help: "Total number of batch search requests",
		},
		[]string{"batch_size"},
	)

	BatchSearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_batch_search_latency_seconds",
			Help:    "Latency of batch search operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		},
		[]string{"batch_size"},
	)

	BatchSearchQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_batch_search_queries_total",
			Help: "Total number of queries processed in batch searches",
		},
		[]string{"batch_size"},
	)

	// Work queue metrics
	WorkQueueBacklog = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_work_queue_backlog",
			Help: "Current number of items in work queue",
		},
		[]string{"queue_name"},
	)

	WorkQueueOverflowsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_work_queue_overflows_total",
			Help: "Total number of work queue overflow rejections",
		},
		[]string{"queue_name"},
	)
)

// =============================================================================
// EOF Normalisation Metrics (Item 1)
// =============================================================================

var (
	// EOFNormalisationTotal counts how often stream-terminal errors are
	// successfully normalised to nil (expected, healthy stream termination).
	EOFNormalisationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_eof_normalisation_total",
			Help: "Total number of stream EOF normalisations (healthy stream terminations detected)",
		},
		[]string{"direction", "protocol"}, // direction: client|server; protocol: arrow|grpc
	)

	// StreamTerminationErrors counts unexpected stream termination errors
	// that are NOT normal EOF (e.g. transport resets, timeouts).
	StreamTerminationErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_stream_termination_errors_total",
			Help: "Total number of unexpected stream termination errors (non-EOF)",
		},
		[]string{"direction", "error_type"}, // error_type: canceled|deadline_exceeded|transport|other
	)
)

// =============================================================================
// Search Consistency Level Metrics (Item 2)
// =============================================================================

var (
	// SearchConsistencyLevelTotal counts searches by requested consistency level.
	SearchConsistencyLevelTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_consistency_level_total",
			Help: "Total number of vector searches by consistency level",
		},
		[]string{"dataset", "level"}, // level: eventual|strong
	)

	// SearchStrongModeLatencySeconds tracks the latency overhead introduced
	// by strong-consistency mode (ExactK=true + elevated Ef).
	SearchStrongModeLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_search_strong_mode_latency_seconds",
			Help:    "Latency of searches running in strong consistency mode",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		},
		[]string{"dataset"},
	)
)

// =============================================================================
// GetNeighbors Metrics (Item 6)
// =============================================================================

var (
	// GetNeighborsTotal counts GetNeighbors operations by result outcome.
	GetNeighborsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_get_neighbors_total",
			Help: "Total number of GetNeighbors operations",
		},
		[]string{"dataset", "index_type", "result"}, // result: success|not_found|not_supported|error
	)

	// GetNeighborsLatencySeconds tracks the latency of GetNeighbors calls.
	GetNeighborsLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_get_neighbors_latency_seconds",
			Help:    "Latency of GetNeighbors operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"dataset", "index_type"},
	)

	// GetNeighborsResultSize tracks the distribution of neighbor set sizes returned.
	GetNeighborsResultSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_get_neighbors_result_size",
			Help:    "Number of neighbors returned per GetNeighbors call",
			Buckets: []float64{1, 2, 5, 10, 20, 32, 64, 128},
		},
		[]string{"dataset"},
	)

	// =============================================================================
	// Recommendation Engine Metrics (v0.1.9)
	// =============================================================================

	// RecommendationsTotal counts Recommend operations by result outcome.
	RecommendationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_recommendations_total",
			Help: "Total number of Recommend operations",
		},
		[]string{"dataset", "result"}, // result: success|error
	)

	// RecommendationsLatencySeconds tracks the latency of Recommend calls.
	RecommendationsLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_recommendations_latency_seconds",
			Help:    "Latency of Recommend operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		},
		[]string{"dataset"},
	)

	// RecommendationsSeedCount tracks the number of seeds per request.
	RecommendationsSeedCount = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_recommendations_seed_count",
			Help:    "Number of seeds provided per Recommend request",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
		},
		[]string{"dataset"},
	)
)
