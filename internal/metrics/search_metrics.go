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
)
