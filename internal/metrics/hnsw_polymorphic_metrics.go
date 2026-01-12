package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HNSWIndexTypeCount tracks the number of indices created per data type.
	HNSWIndexTypeCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_index_type_count",
		Help: "Total number of HNSW indices created by data type",
	}, []string{"type"})

	// HNSWArenaAllocationBytes tracks memory allocated in arenas per data type.
	HNSWArenaAllocationBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_arena_allocation_bytes_total",
		Help: "Total bytes allocated in HNSW arenas per data type",
	}, []string{"type"})

	// ArrowExtractionErrorsTotal tracks errors during vector extraction from Arrow.
	ArrowExtractionErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_arrow_extraction_errors_total",
		Help: "Total number of errors encountered while extracting vectors from Arrow record batches",
	}, []string{"type", "error"})

	// HNSWSimdDispatchLatency measures the time taken for dynamic SIMD kernel dispatch.
	HNSWSimdDispatchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_simd_dispatch_latency_seconds",
		Help:    "Latency of dynamic SIMD kernel dispatch by data type",
		Buckets: []float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001},
	}, []string{"type"})

	// HNSWSearchQueriesTotal tracks search queries labeled by vector dimension.
	HNSWSearchQueriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_search_queries_total",
		Help: "Total number of HNSW search queries executed, labeled by dimensions",
	}, []string{"dims"})

	// HNSWComplexOpsTotal tracks the number of complex number operations (distance calcs).
	HNSWComplexOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_complex_ops_total",
		Help: "Total number of complex number distance calculations",
	}, []string{"type"})

	HNSWPolymorphicSearchCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_polymorphic_search_count",
		Help: "Total number of searches by polymorphic vector type",
	}, []string{"type"})

	// HNSWPolymorphicLatency measures latency of search operations by type.
	HNSWPolymorphicLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_polymorphic_latency_seconds",
		Help:    "Latency of search operations by polymorphic vector type",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})

	// HNSWPolymorphicThroughput tracks total bytes processed during search by type.
	HNSWPolymorphicThroughput = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_polymorphic_throughput_bytes",
		Help: "Total bytes processed during polymorphic search",
	}, []string{"type"})

	HNSWPolymorphicFuzzCrashRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_fuzz_crash_recovered_total",
		Help: "Total number of recovered crashes during fuzz testing",
	})
)
