package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Vector pool metrics
	VectorPoolHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_hits_total",
		Help: "Total number of vector pool hits (reused vectors)",
	})

	VectorPoolMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_misses_total",
		Help: "Total number of vector pool misses (new allocations)",
	})

	VectorPoolPuts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_puts_total",
		Help: "Total number of vectors returned to pool",
	})

	// HNSW allocation metrics
	HNSWVectorAllocations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_vector_allocations_total",
		Help: "Total number of vector allocations for HNSW graph storage",
	})

	HNSWVectorAllocatedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_vector_allocated_bytes_total",
		Help: "Total bytes allocated for HNSW vector storage",
	})

	HNSWGraphNodeAllocations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_graph_node_allocations_total",
		Help: "Total number of HNSW graph node allocations",
	})

	// Lock contention metrics
	LockContentionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_lock_contention_duration_seconds",
		Help:    "Time spent waiting for locks",
		Buckets: []float64{1e-6, 1e-5, 1e-4, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1}, // 1us to 1s
	}, []string{"type"})

	// Index growth metrics
	HNSWIndexGrowthDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_index_growth_duration_seconds",
		Help:    "Time spent growing the HNSW index capacity",
		Buckets: prometheus.DefBuckets,
	})
)
