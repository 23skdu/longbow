package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Flight & RPC Metrics
// =============================================================================

var (
	// FlightBytesReadTotal counts total bytes read from Arrow Flight tickets
	FlightBytesReadTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_bytes_read_total",
			Help: "Total bytes read from Flight tickets",
		},
	)

	// FlightBytesWrittenTotal counts total bytes written to Arrow Flight streams
	FlightBytesWrittenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_bytes_written_total",
			Help: "Total bytes written to Flight streams",
		},
	)

	// FlightOpsTotal counts total Flight operations
	FlightOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_ops_total",
			Help: "Total number of Flight operations",
		},
		[]string{"action", "status"},
	)

	// FlightDurationSeconds measures latency of Flight operations
	FlightDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_duration_seconds",
			Help:    "Latency of Flight operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		},
		[]string{"action"},
	)

	// FlightActiveTickets tracks currently processing/active tickets
	FlightActiveTickets = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_flight_active_tickets",
			Help: "Number of currently active Flight tickets",
		},
	)

	// FlightStreamPoolSize - Number of recycled stream writers
	FlightStreamPoolSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_flight_stream_pool_size",
			Help: "Number of recycled Flight stream writers in the pool",
		},
	)

	// FlightPoolConnectionsActive tracks active connections in the flight client pool
	FlightPoolConnectionsActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_flight_pool_connections_active",
			Help: "Number of active connections in the flight client pool",
		},
		[]string{"host"},
	)

	// FlightPoolWaitDuration measures time waiting for a flight client connection
	FlightPoolWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_pool_wait_duration_seconds",
			Help:    "Time waiting for a flight client connection",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"host"},
	)
	// FlightTicketParseDurationSeconds measures time to parse flight tickets
	FlightTicketParseDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_ticket_parse_duration_seconds",
			Help:    "Time taken to parse Flight tickets",
			Buckets: []float64{0.0001, 0.001, 0.01},
		},
	)

	// ArrowMemoryUsedBytes tracks memory used by Arrow allocators
	ArrowMemoryUsedBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_arrow_memory_used_bytes",
			Help: "Current bytes allocated by Arrow memory pool",
		},
		[]string{"allocator"},
	)
)

// =============================================================================
// Rate Limiting & Server Protections
// =============================================================================

var (
	RateLimitRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_rate_limit_requests_total",
			Help: "Total number of requests processed by rate limiter",
		},
		[]string{"result"}, // "allowed", "throttled"
	)

	// ActiveSearchContexts tracks concurrent search requests
	ActiveSearchContexts = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_active_search_contexts",
			Help: "Number of currently active search contexts",
		},
	)

	ValidationFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_validation_failures_total",
			Help: "Total number of request validation failures",
		},
		[]string{"type"},
	)

	IpcDecodeErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_ipc_decode_errors_total",
			Help: "Total number of IPC message decoding errors",
		},
		[]string{"type"},
	)

	// PanicTotal counts total recovering panics in the system
	PanicTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_panics_total",
			Help: "Total number of recovered panics",
		},
		[]string{"component"},
	)
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

	// ParallelReductionVectorsProcessed counts vectors processed via parallel reduction
	ParallelReductionVectorsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_simd_parallel_reduction_vectors_processed",
			Help: "Total number of vectors processed using parallel reduction",
		},
	)

	TraceSpansTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_spans_total",
			Help: "Total number of trace spans created",
		},
		[]string{"name"},
	)

	// IPC Buffer Pool Metrics
	IpcBufferPoolUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_ipc_buffer_pool_utilization",
			Help: "Current utilization of the IPC buffer pool (0-1)",
		},
	)
	IpcBufferPoolHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_ipc_buffer_pool_hits_total",
			Help: "Total number of IPC buffer pool hits",
		},
	)
	IpcBufferPoolMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_ipc_buffer_pool_misses_total",
			Help: "Total number of IPC buffer pool misses",
		},
	)

	// Warmup Metrics
	WarmupProgressPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_warmup_progress_percent",
			Help: "Current warmup progress percentage (0-100)",
		},
	)
	WarmupDatasetsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_warmup_datasets_total",
			Help: "Total number of datasets to warmup",
		},
	)
	WarmupDatasetsCompleted = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_warmup_datasets_completed",
			Help: "Total number of datasets where warmup is completed",
		},
	)
)

// =============================================================================
// Pipeline & S3 Metrics
// =============================================================================

var (
	PipelineOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_pipeline_operations_total",
			Help: "Total number of pipeline operations",
		},
		[]string{"stage", "status"},
	)

	PipelineDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_pipeline_duration_seconds",
			Help:    "Duration of pipeline stages",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"stage"},
	)

	// PipelineBatchesPerSecond tracks the rate of batches processed by the pipeline
	PipelineBatchesPerSecond = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_pipeline_batches_per_second",
			Help: "Current rate of record batches processed per second",
		},
	)

	// PipelineWorkerUtilization tracks the utilization of pipeline workers
	PipelineWorkerUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_pipeline_worker_utilization",
			Help: "Utilization of pipeline workers (0-1)",
		},
		[]string{"worker_id"},
	)

	S3OperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_s3_operations_total",
			Help: "Total number of S3 operations",
		},
		[]string{"operation", "status"},
	)

	// S3RequestDuration measures duration of S3 operations
	S3RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_s3_request_duration_seconds",
			Help:    "Duration of S3 operations",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"operation"},
	)

	// S3RetriesTotal counts total S3 operation retries
	S3RetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_s3_retries_total",
			Help: "Total number of S3 operation retries",
		},
		[]string{"operation"},
	)
)

// =============================================================================
// gRPC Metrics
// =============================================================================

var (
	GRPCMaxHeaderListSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_header_list_size",
			Help: "Configured max header list size for gRPC",
		},
	)
	GRPCMaxRecvMsgSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_recv_msg_size",
			Help: "Configured max receive message size for gRPC",
		},
	)
	GRPCMaxSendMsgSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_send_msg_size",
			Help: "Configured max send message size for gRPC",
		},
	)
)

// =============================================================================
// Namespace Metrics
// =============================================================================

var (
	NamespaceDatasetsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_datasets_total",
			Help: "Total number of datasets in a namespace",
		},
		[]string{"namespace"},
	)
)

// =============================================================================
// HNSW Graph Metrics
// =============================================================================

var (
	HNSWInsertDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_insert_duration_seconds",
			Help:    "Duration of HNSW vector insertion",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)

	HNSWNodesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_nodes_total",
			Help: "Total number of nodes in the HNSW graph",
		},
		[]string{"dataset"},
	)

	HNSWResizesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_resizes_total",
			Help: "Total number of HNSW graph resizes",
		},
		[]string{"dataset"},
	)
	// IndexBuildDurationSeconds measures the time taken to build or update the index
	IndexBuildDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_index_build_duration_seconds",
			Help:    "Duration of index build or update operations",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60},
		},
		[]string{"dataset"},
	)

	// SearchLatencySeconds measures the duration of search operations by query type
	SearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_search_latency_seconds",
			Help:    "Latency of search operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"dataset", "query_type"}, // query_type: "vector", "hybrid", "keyword"
	)
)
