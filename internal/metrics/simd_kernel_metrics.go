package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// SIMD Kernel Performance Metrics
// =============================================================================

var (
	// SimdKernelDuration tracks SIMD kernel execution duration by dimension and operation
	SimdKernelDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "longbow_simd_kernel_duration_seconds",
			Help: "SIMD kernel execution duration in seconds",
			Buckets: []float64{
				0.000001, // 1µs
				0.000005, // 5µs
				0.00001,  // 10µs
				0.000025, // 25µs
				0.00005,  // 50µs
				0.0001,   // 100µs
				0.00025,  // 250µs
				0.0005,   // 500µs
				0.001,    // 1ms
				0.0025,   // 2.5ms
				0.005,    // 5ms
			},
		},
		[]string{"dtype", "dimension", "operation"}, // e.g., "float32", "384", "euclidean"
	)

	// SimdKernelOperationsTotal counts SIMD kernel operations by dimension
	SimdKernelOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_kernel_operations_total",
			Help: "Total number of SIMD kernel operations",
		},
		[]string{"dtype", "dimension", "operation"},
	)

	// SimdFallbackTotal counts non-optimized path usage
	SimdFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_fallback_total",
			Help: "Total number of times SIMD fell back to generic implementation",
		},
		[]string{"dtype", "dimension", "reason"}, // "dimension_not_optimized", "type_not_supported"
	)

	// SimdBlockedProcessingTotal counts blocked SIMD path usage
	SimdBlockedProcessingTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_blocked_processing_total",
			Help: "Total number of times blocked SIMD processing was used",
		},
		[]string{"dtype", "dimension", "block_size"},
	)

	// SearchDimensionDistribution tracks search queries by dimension
	SearchDimensionDistribution = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_search_dimension_distribution_total",
			Help: "Distribution of search queries by dimension",
		},
		[]string{"dtype", "dimension", "search_type"}, // "dense", "hybrid", "filtered", "byid"
	)

	// SearchLatencyByDimension tracks search latency by dimension
	SearchLatencyByDimension = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "longbow_search_latency_by_dimension_seconds",
			Help: "Search latency in seconds, bucketed by dimension",
			Buckets: []float64{
				0.0001,  // 100µs
				0.00025, // 250µs
				0.0005,  // 500µs
				0.001,   // 1ms
				0.0025,  // 2.5ms
				0.005,   // 5ms
				0.01,    // 10ms
				0.025,   // 25ms
				0.05,    // 50ms
			},
		},
		[]string{"dtype", "dimension", "search_type"},
	)
)

// =============================================================================
// Memory Pressure Metrics
// =============================================================================

var (
	// SearchAllocationBytes tracks search-related memory allocations
	SearchAllocationBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "longbow_search_allocation_bytes",
			Help: "Memory allocated during search operations in bytes",
			Buckets: []float64{
				1024,     // 1KB
				4096,     // 4KB
				16384,    // 16KB
				65536,    // 64KB
				262144,   // 256KB
				1048576,  // 1MB
				4194304,  // 4MB
				16777216, // 16MB
			},
		},
	)

	// VectorCopyTotal counts vector copies (non-zero-copy operations)
	VectorCopyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_copy_total",
			Help: "Total number of vector copies (indicates zero-copy violations)",
		},
		[]string{"dtype", "dimension", "reason"}, // "type_conversion", "dimension_expansion", "pool_exhaustion"
	)

	// BufferPoolHitsTotal tracks buffer pool hit rate
	BufferPoolHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_buffer_pool_hits_total",
			Help: "Total number of buffer pool hits",
		},
	)

	// BufferPoolMissesTotal tracks buffer pool miss rate
	BufferPoolMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_buffer_pool_misses_total",
			Help: "Total number of buffer pool misses",
		},
	)

	// BufferPoolSizeBytes tracks current buffer pool size
	BufferPoolSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_buffer_pool_size_bytes",
			Help: "Current size of buffer pool in bytes",
		},
	)

	// DimensionBufferBytes tracks buffers allocated per dimension
	DimensionBufferBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_dimension_buffer_bytes",
			Help: "Buffer memory allocated per dimension",
		},
		[]string{"dimension"},
	)

	// Use existing ArenaAllocatedBytes from metrics_arena.go
)

// =============================================================================
// Performance Stability Metrics
// =============================================================================

var (
	// SearchQPSByDimension tracks QPS by dimension for stability monitoring
	SearchQPSByDimension = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_search_qps_by_dimension",
			Help: "Current measured QPS by dimension",
		},
		[]string{"dtype", "dimension", "search_type"},
	)

	// SearchP50LatencyByDimension tracks P50 latency
	SearchP50LatencyByDimension = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_search_p50_latency_ms",
			Help: "P50 search latency in milliseconds by dimension",
		},
		[]string{"dtype", "dimension", "search_type"},
	)

	// SearchP99LatencyByDimension tracks P99 latency
	SearchP99LatencyByDimension = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_search_p99_latency_ms",
			Help: "P99 search latency in milliseconds by dimension",
		},
		[]string{"dtype", "dimension", "search_type"},
	)

	// KernelPerformanceRegression tracks potential regressions
	KernelPerformanceRegression = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_kernel_performance_regression_ratio",
			Help: "Ratio of current performance to baseline (1.0 = no change, <1.0 = regression)",
		},
		[]string{"dtype", "dimension", "operation"},
	)
)
