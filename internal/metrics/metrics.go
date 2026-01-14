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

	// DoGetTimeToFirstChunk measures time from request to first chunk sent
	DoGetTimeToFirstChunk = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doget_time_to_first_chunk_seconds",
			Help:    "Time from DoGet request to first chunk sent (latency indicator)",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// DoGetChunkSizeHistogram tracks chunk sizes used in DoGet operations
	DoGetChunkSizeHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doget_chunk_size_rows",
			Help:    "Distribution of chunk sizes (in rows) used in DoGet operations",
			Buckets: []float64{1000, 2000, 4096, 8192, 16384, 32768, 65536, 131072},
		},
	)

	// DoGetAdaptiveChunksTotal counts chunks sent with adaptive sizing
	DoGetAdaptiveChunksTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_doget_adaptive_chunks_total",
			Help: "Total number of chunks sent using adaptive chunk sizing",
		},
	)

	// DoGetChunkGrowthRate tracks current growth multiplier
	DoGetChunkGrowthRate = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_doget_chunk_growth_rate",
			Help: "Current chunk size growth rate (multiplier)",
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

	// AllocatorBytesAllocatedTotal tracks cumulative bytes allocated
	AllocatorBytesAllocatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_allocator_bytes_allocated_total",
			Help: "Total bytes allocated by the custom allocator",
		},
	)

	// AllocatorBytesFreedTotal tracks cumulative bytes freed
	AllocatorBytesFreedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_allocator_bytes_freed_total",
			Help: "Total bytes freed by the custom allocator",
		},
	)

	// AllocatorAllocationsActive tracks number of active allocation objects
	AllocatorAllocationsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_allocator_allocations_active",
			Help: "Number of currently active memory allocations",
		},
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

	// DoExchangeSearchTotal counts total number of searches via DoExchange
	DoExchangeSearchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_do_exchange_search_total",
		Help: "Total number of searches performed via Arrow Flight DoExchange binary protocol",
	})

	// DoExchangeSearchDuration measures latency of DoExchange search operations
	DoExchangeSearchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_do_exchange_search_duration_seconds",
		Help:    "Latency of DoExchange search operations",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})

	// Rate Limit Metrics
	CompactionRateLimitWaitSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_compaction_rate_limit_wait_seconds",
			Help:    "Time spent waiting for compaction rate limiter",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5},
		},
	)
	SnapshotRateLimitWaitSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_rate_limit_wait_seconds",
			Help:    "Time spent waiting for snapshot rate limiter",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5},
		},
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

	// Adaptive GC Metrics
	AdaptiveGCCurrentGOGC = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_current_gogc",
		Help: "Current GOGC value set by adaptive GC controller",
	})

	AdaptiveGCAdjustmentsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_adaptive_gc_adjustments_total",
		Help: "Total number of GOGC adjustments made by adaptive controller",
	})

	AdaptiveGCAllocationRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_allocation_rate_bytes_per_sec",
		Help: "Current memory allocation rate in bytes per second",
	})

	AdaptiveGCMemoryPressure = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_memory_pressure_ratio",
		Help: "Current memory pressure ratio (0-1, where 1 is maximum pressure)",
	})

	// HNSW Repair Agent Metrics
	HNSWRepairOrphansDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_repair_orphans_detected_total",
		Help: "Total number of orphaned nodes detected by repair agent",
	}, []string{"dataset"})

	HNSWRepairOrphansRepaired = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_repair_orphans_repaired_total",
		Help: "Total number of orphaned nodes repaired by repair agent",
	}, []string{"dataset"})

	HNSWRepairScanDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_repair_scan_duration_seconds",
		Help:    "Duration of repair agent scan cycles",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~4s
	}, []string{"dataset"})

	HNSWRepairLastScanTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_hnsw_repair_last_scan_timestamp_seconds",
		Help: "Unix timestamp of last repair scan",
	}, []string{"dataset"})

	// Fragmentation-Aware Compaction Metrics
	CompactionTombstoneDensity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_compaction_tombstone_density_ratio",
		Help: "Current tombstone density per batch (0-1)",
	}, []string{"dataset", "batch"})

	CompactionFragmentedBatches = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_compaction_fragmented_batches_total",
		Help: "Number of batches exceeding fragmentation threshold",
	}, []string{"dataset"})

	CompactionTriggersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_compaction_triggers_total",
		Help: "Total number of compaction triggers by reason",
	}, []string{"dataset", "reason"})

	CompactionBatchesMerged = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_compaction_batches_merged_total",
		Help: "Total number of batches merged during compaction",
	}, []string{"dataset"})

	// IngestionQueueDepth tracks the current number of batches in the ingestion queue
	IngestionQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_ingestion_queue_depth",
		Help: "Current number of batches waiting in the ingestion queue",
	})

	// IngestionQueueLatency tracks time spent in the ingestion queue
	IngestionQueueLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_ingestion_queue_latency_seconds",
		Help:    "Time spent in the ingestion queue before processing",
		Buckets: prometheus.DefBuckets,
	})

	// IngestionLagCount tracks the total items (rows) waiting to be ingested
	IngestionLagCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_ingestion_lag_count",
		Help: "Total number of records waiting to be ingested",
	})

	// IndexJobsOverflowTotal tracks number of jobs sent to overflow queue
	IndexJobsOverflowTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_index_jobs_overflow_total",
		Help: "Total number of index jobs sent to overflow buffer or retried asynchronously",
	})

	// HNSWVisitedResetDuration tracks the time spent resetting the visited bitset/list
	HNSWVisitedResetDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_visited_reset_duration_seconds",
		Help:    "Time spent resetting HNSW visited set",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01}, // tailored for fast ops
	})

	// PrefetchOperationsTotal tracks software prefetch instructions issued
	PrefetchOperationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_prefetch_operations_total",
			Help: "Total number of software prefetch instructions issued during search",
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

	FlightZeroCopyBytesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_zero_copy_bytes_total",
			Help: "Total bytes sent via zero-copy optimization",
		},
	)

	DoPutZeroCopyPathTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_flight_doput_zerocopy_path_total",
			Help: "Total number of batches processed via Zero-Copy (direct) path",
		},
	)

	VectorCastF16ToF32Total = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_cast_f16_to_f32_total",
			Help: "Total number of vector casts from Float16 to Float32",
		},
	)

	VectorCastF32ToF16Total = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_cast_f32_to_f16_total",
			Help: "Total number of vector casts from Float32 to Float16",
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
// GOGC Auto-Tuning Metrics
// =============================================================================
var (
	GCTunerTargetGOGC = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gc_tuner_target_gogc",
			Help: "Current target GOGC value set by the tuner",
		},
	)

	GCTunerHeapUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gc_tuner_heap_utilization",
			Help: "Current heap utilization ratio (heap_inuse / limit)",
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

	// GRPCStreamStallTotal counts total number of detected stream stalls
	GRPCStreamStallTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_grpc_stream_stall_total",
			Help: "Total number of gRPC stream stalling events detected",
		},
	)

	// GRPCStreamSendLatencySeconds measures the latency of gRPC SendMsg calls
	GRPCStreamSendLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_grpc_stream_send_latency_seconds",
			Help:    "Latency of gRPC SendMsg calls (used to detect flow control stalling)",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
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

	// CompressedVectorsSentTotal counts total number of quantized vectors sent in search results
	CompressedVectorsSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compressed_vectors_sent_total",
			Help: "Total number of quantized (SQ8/PQ) vectors sent in search results",
		},
	)

	// RawVectorsSentTotal counts total number of full-precision vectors sent in search results
	RawVectorsSentTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_raw_vectors_sent_total",
			Help: "Total number of raw (F32/F16) vectors sent in search results",
		},
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

	HNSWBulkInsertDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bulk_insert_duration_seconds",
			Help:    "Duration of HNSW bulk vector insertion",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30},
		},
	)

	HNSWBulkVectorsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_bulk_vectors_processed_total",
			Help: "Total number of vectors processed using bulk insert path",
		},
	)

	// HnswSearchThroughputDims counts search operations bucketed by dimension
	HnswSearchThroughputDims = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_ahnsw_search_throughput_dims",
			Help: "Total number of search operations bucketed by vector dimension",
		},
		[]string{"dims"},
	)

	// HNSWSearchLatencyByType measures search latency per vector data type
	HNSWSearchLatencyByType = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_search_latency_by_type_seconds",
			Help:    "Latency of HNSW search operations bucketed by vector type",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	// HNSWSearchLatencyByDim measures search latency per vector dimension
	HNSWSearchLatencyByDim = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_search_latency_by_dim_seconds",
			Help:    "Latency of HNSW search operations bucketed by dimension",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"dim"},
	)

	// HNSWRefineThroughput counts vectors refined per type
	HNSWRefineThroughput = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_refine_throughput_total",
			Help: "Total vectors processed through refinement layer",
		},
		[]string{"type"},
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

	HNSWSearchPoolGetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_get_total",
			Help: "Total number of search contexts retrieved from the pool",
		},
	)

	HNSWSearchPoolNewTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_new_total",
			Help: "Total number of new search contexts allocated (cache misses)",
		},
	)

	HNSWSearchPoolPutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_put_total",
			Help: "Total number of search contexts returned to the pool",
		},
	)

	HNSWInsertPoolGetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_get_total",
			Help: "Total number of insert contexts retrieved from the pool",
		},
	)

	HNSWInsertPoolNewTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_new_total",
			Help: "Total number of new insert contexts allocated (cache misses)",
		},
	)

	HNSWInsertPoolPutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_put_total",
			Help: "Total number of insert contexts returned to the pool",
		},
	)

	HNSWBitsetGrowTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_bitset_grow_total",
			Help: "Total number of times a bitset was grown/reallocated",
		},
	)

	HNSWDistanceCalculationsF16Total = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_distance_calculations_f16_total",
			Help: "Total number of native FP16 distance calculations performed",
		},
	)

	// VectorSentinelHitTotal counts number of times a sentinel zero-vector was returned
	VectorSentinelHitTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_sentinel_hit_total",
			Help: "Total number of times a sentinel vector was used due to missing data",
		},
	)

	// HNSWBitmapIndexEntriesTotal tracks number of entries in metadata bitmap index
	HNSWBitmapIndexEntriesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_bitmap_index_entries_total",
			Help: "Total number of unique field:value pairs in the bitmap index",
		},
		[]string{"dataset"},
	)

	// HNSWBitmapFilterDurationSeconds measures time to evaluate bitset filters
	HNSWBitmapFilterDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bitmap_filter_duration_seconds",
			Help:    "Time taken to evaluate metadata bitset filters",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"dataset"},
	)

	// HNSWSearchEarlyTerminationsTotal counts number of searches that terminated early
	HNSWSearchEarlyTerminationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_early_terminations_total",
			Help: "Total number of searches that terminated early due to convergence",
		},
		[]string{"dataset", "reason"},
	)

	// QueryCacheOpsTotal counts query cache operations (hit, miss, evict)
	QueryCacheOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_ops_total",
			Help: "Total number of query cache operations",
		},
		[]string{"dataset", "type"}, // "hit", "miss", "evict", "set"
	)

	QueryCacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_query_cache_size",
			Help: "Current number of entries in query cache",
		},
		[]string{"dataset"},
	)

	QueryCacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_hits_total",
			Help: "Total number of query cache hits",
		},
		[]string{"dataset"},
	)

	QueryCacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_misses_total",
			Help: "Total number of query cache misses",
		},
		[]string{"dataset"},
	)

	DiskStoreReadBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_disk_store_read_bytes_total",
			Help: "Total bytes read from disk vector store",
		},
		[]string{"dataset"},
	)

	DiskStoreWriteBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_disk_store_write_bytes_total",
			Help: "Total bytes written to disk vector store",
		},
		[]string{"dataset"},
	)

	QueryCacheEvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_evictions_total",
			Help: "Total number of query cache evictions",
		},
		[]string{"dataset"},
	)
)

// =============================================================================
// BQ & RCU Metrics
// =============================================================================
var (
	BQVectorsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_bq_vectors_total",
			Help: "Total number of vectors indexed with Binary Quantization",
		},
		[]string{"dataset"},
	)

	DatasetUpdateRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_dataset_update_retries_total",
			Help: "Total number of retries during lock-free dataset map updates (CAS failures)",
		},
	)
)

// =============================================================================
// JIT Metrics
// =============================================================================
var (
	JitCompilationDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_jit_compilation_duration_seconds",
			Help:    "Time spent compiling JIT kernels",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
	)

	JitKernelCallsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_jit_kernel_calls_total",
			Help: "Total number of JIT kernel function calls",
		},
		[]string{"kernel"},
	)

	JitKernelErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_jit_kernel_errors_total",
			Help: "Total number of JIT kernel execution errors",
		},
	)

	// WAL Metrics
	WALWriteErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_write_errors_total",
			Help: "Total number of WAL write errors",
		},
	)

	WALWriteDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_write_duration_seconds",
			Help:    "Duration of WAL writes",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05},
		},
	)
)
