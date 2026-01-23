package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// System & Platform Metrics
// =============================================================================

var (
	// GcPauseDurationSeconds measures Go GC pause duration
	GcPauseDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_gc_pause_duration_seconds",
			Help:    "Duration of GC pauses",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
	)

	SimdEnabled = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_simd_enabled",
			Help: "Whether SIMD acceleration is enabled for the architecture (1=yes, 0=no)",
		},
		[]string{"instruction_set"}, // "AVX2", "AVX512", "NEON"
	)

	SimdOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_operations_total",
			Help: "Total number of SIMD-accelerated operations",
		},
		[]string{"op"}, // "dot_product", "l2_sq", "quantize"
	)

	// SimdDispatchCount counts SIMD instruction dispatches
	SimdDispatchCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_dispatch_count",
			Help: "Total number of dynamic SIMD instruction dispatches",
		},
		[]string{"instruction"},
	)

	// CosineBatchCallsTotal counts total calls to cosine batch functions
	CosineBatchCallsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_simd_cosine_batch_calls_total",
			Help: "Total number of batched cosine distance calculations",
		},
	)

	// DotProductBatchCallsTotal counts total calls to dot product batch functions
	DotProductBatchCallsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_simd_dot_product_batch_calls_total",
			Help: "Total number of batched dot product calculations",
		},
	)

	// ParallelReductionVectorsProcessed tracks the number of vectors processed using parallel reduction
	ParallelReductionVectorsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_parallel_reduction_vectors_processed_total",
		Help: "Total number of vectors processed using parallel reduction optimizations",
	})

	// SimdF16OpsTotal tracks the number of FP16 SIMD operations performed
	SimdF16OpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_simd_f16_ops_total",
		Help: "Total number of FP16 SIMD operations explicitly dispatched",
	}, []string{"operation", "impl"})

	// SimdStaticDispatchType tracks the currently active SIMD implementation type
	// 0=Generic, 1=NEON, 2=AVX2, 3=AVX512
	SimdStaticDispatchType = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_simd_static_dispatch_type",
		Help: "Type of SIMD implementation statically dispatched (0=Generic, 1=NEON, 2=AVX2, 3=AVX512)",
	})

	// Adaptive streaming metrics
	DoGetChunkSizeHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_do_get_chunk_size_bytes",
		Help:    "Distribution of chunk sizes returned by DoGet operations",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 10), // 1KB to ~1MB
	})

	DoGetAdaptiveChunksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_do_get_adaptive_chunks_total",
		Help: "Total number of adaptive chunks created during DoGet operations",
	})
)
