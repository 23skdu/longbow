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
