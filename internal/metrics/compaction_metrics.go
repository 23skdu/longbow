package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Compaction & Snapshot Metrics
// =============================================================================

var (
	CompactionOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_compaction_operations_total",
			Help: "Total compaction operations by status",
		},
		[]string{"dataset", "status"},
	)

	// CompactionDurationSeconds measures duration of compaction jobs
	CompactionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_compaction_duration_seconds",
			Help:    "Duration of compaction jobs",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"dataset", "type"},
	)

	// CompactionRecordsProcessedTotal counts records processed during compaction
	CompactionRecordsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_records_processed_total",
			Help: "Total number of records processed during compaction",
		},
	)

	// CompactionRecordsRemovedTotal - Records removed during compaction (restored)
	CompactionRecordsRemovedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_compaction_records_removed_total",
			Help: "Total number of records removed during compaction",
		},
		[]string{"dataset"},
	)

	// CompactionAutoTriggersTotal - Auto-triggered compaction tracking (restored)
	CompactionAutoTriggersTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_auto_triggers_total",
			Help: "Total number of auto-triggered compactions when batch count exceeds threshold",
		},
	)

	// CompactionErrorsTotal counts compaction errors
	CompactionErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_errors_total",
			Help: "Total number of compaction errors",
		},
	)

	SnapshotTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_snapshot_operations_total",
			Help: "Total number of snapshot operations",
		},
		[]string{"status"},
	)
	SnapshotDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_duration_seconds",
			Help:    "Duration of snapshot creation operations",
			Buckets: prometheus.DefBuckets,
		},
	)
	SnapshotWriteDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_write_duration_seconds",
			Help:    "Duration of Parquet snapshot write operations",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
	)
	SnapshotSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_size_bytes",
			Help:    "Size of generated Parquet snapshots in bytes",
			Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8, 1e9}, // 10KB to 1GB
		},
	)

	// AdaptiveIndexMigrationsTotal
	AdaptiveIndexMigrationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_adaptive_index_migrations_total",
			Help: "Total number of adaptive index migrations",
		},
		[]string{"from", "to"},
	)

	// HnswSearchesTotal
	HnswSearchesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_searches_total",
			Help: "Total number of HNSW index searches",
		},
	)

	// BruteForceSearchesTotal
	BruteForceSearchesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_brute_force_searches_total",
			Help: "Total number of brute force searches",
		},
	)

	// BinaryQuantizeOpsTotal
	BinaryQuantizeOpsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_binary_quantize_ops_total",
			Help: "Total number of binary quantization operations",
		},
	)

	// POPCNTDistanceOpsTotal
	POPCNTDistanceOpsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_popcnt_distance_ops_total",
			Help: "Total number of POPCNT distance calculations",
		},
	)

	// BitmapPoolGetsTotal
	BitmapPoolGetsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_gets_total",
			Help: "Total number of bitmap pool retrievals",
		},
	)

	// BitmapPoolHitsTotal
	BitmapPoolHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_hits_total",
			Help: "Total number of bitmap pool hits (reused)",
		},
	)

	// BitmapPoolMissesTotal
	BitmapPoolMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_misses_total",
			Help: "Total number of bitmap pool misses (allocations)",
		},
	)

	// BitmapPoolPutsTotal
	BitmapPoolPutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_puts_total",
			Help: "Total number of bitmap pool returns",
		},
	)

	// BitmapPoolDiscardsTotal
	BitmapPoolDiscardsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_discards_total",
			Help: "Total number of bitmap pool discards (oversized)",
		},
	)

	// Checkpoint Metrics
	CheckpointEpoch = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_checkpoint_epoch",
			Help: "Current checkpoint epoch",
		},
	)
	CheckpointsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_checkpoints_total",
			Help: "Total number of checkpoints created",
		},
	)
	CheckpointBarrierReached = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_checkpoint_barrier_reached_total",
			Help: "Total number of checkpoint barriers reached",
		},
	)
	CheckpointTimeoutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_checkpoint_timeouts_total",
			Help: "Total number of checkpoint timeouts",
		},
	)

	// Store Circuit Breaker Metrics
	CircuitBreakerStateChanges = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_store_circuit_breaker_state_changes_total",
			Help: "Total number of circuit breaker state changes",
		},
		[]string{"name", "from", "to"},
	)
	CircuitBreakerRejections = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_store_circuit_breaker_rejections_total",
			Help: "Total number of requests rejected by store circuit breaker",
		},
	)
	CircuitBreakerSuccesses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_store_circuit_breaker_successes_total",
			Help: "Total number of successful requests passing through breaker",
		},
	)
	CircuitBreakerFailures = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_store_circuit_breaker_failures_total",
			Help: "Total number of failed requests passing through breaker",
		},
	)

	// PipelineUtilization (Counter to match usage, though name implies Gauge)
	PipelineUtilization = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_pipeline_utilization_total",
			Help: "Total number of pipeline activations (misnamed as utilization)",
		},
		[]string{"stage"},
	)

	// VectorSearchLatencySeconds
	VectorSearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_vector_search_latency_seconds",
			Help:    "Latency of vector search operations",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"dataset"},
	)

	// BatchDistanceDurationSeconds
	BatchDistanceDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_batch_distance_duration_seconds",
			Help:    "Duration of batch distance calculations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// BatchDistanceCallsTotal
	BatchDistanceCallsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_batch_distance_calls_total",
			Help: "Total number of batch distance calculation calls",
		},
	)

	// BatchDistanceBatchSize
	BatchDistanceBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_batch_distance_batch_size",
			Help:    "Distribution of batch sizes in distance calculations",
			Buckets: []float64{1, 10, 50, 100, 256, 512, 1024},
		},
	)

	// Hybrid Search Metrics
	HybridSearchVectorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_vector_total",
			Help: "Total number of hybrid searches triggered by vector query",
		},
	)
	HybridSearchKeywordTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_keyword_total",
			Help: "Total number of hybrid searches triggered by keyword query",
		},
	)

	// Namespace Metrics
	NamespacesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_namespaces_total",
			Help: "Total number of active namespaces",
		},
	)

	// Semaphore Metrics
	SemaphoreWaitingRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_semaphore_waiting_requests",
			Help: "Number of requests waiting for semaphore",
		},
	)
	SemaphoreQueueDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_semaphore_queue_duration_seconds",
			Help:    "Time spent waiting in semaphore queue",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
	)
	SemaphoreTimeoutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_semaphore_timeouts_total",
			Help: "Total number of semaphore acquisition timeouts",
		},
	)
	SemaphoreAcquiredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_semaphore_acquired_total",
			Help: "Total number of semaphore acquisitions",
		},
	)
	SemaphoreActiveRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_semaphore_active_requests",
			Help: "Number of requests currently holding semaphore",
		},
	)

	// Schema Evolution Metrics
	SchemaVersionCurrent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_schema_version_current",
			Help: "Current schema version",
		},
		[]string{"dataset"},
	)
	SchemaEvolutionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_schema_evolution_duration_seconds",
			Help:    "Duration of schema evolution operations",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5},
		},
	)
	SchemaColumnsAddedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_schema_columns_added_total",
			Help: "Total number of columns added via schema evolution",
		},
	)
	SchemaColumnsDroppedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_schema_columns_dropped_total",
			Help: "Total number of columns dropped via schema evolution",
		},
	)

	// Store Metrics
	DoPutPayloadSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_do_put_payload_size_bytes",
			Help:    "Size of DoPut payloads in bytes",
			Buckets: []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304},
		},
	)
	FlightRowsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_rows_processed_total",
			Help: "Total number of rows processed by Flight service",
		},
		[]string{"operation", "status"},
	)
	DoGetPipelineStepsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_do_get_pipeline_steps_total",
			Help: "Total number of DoGet pipeline steps executed",
		},
		[]string{"step", "status"},
	)
	// Inverted Index Metrics
	InvertedIndexBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_inverted_index_size_bytes",
			Help: "Size of inverted index in bytes",
		},
		[]string{"dataset", "field"},
	)

	BM25DocumentsIndexedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bm25_documents_indexed_total",
			Help: "Total number of documents indexed in BM25",
		},
	)

	// Restored DoGetZeroCopyTotal
	DoGetZeroCopyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_do_get_zero_copy_total",
			Help: "Total number of zero-copy optimizations in DoGet",
		},
		[]string{"type"},
	)

	// Network Metrics
	TCPNoDelayConnectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_tcp_nodelay_connections_total",
			Help: "Total number of TCP connections with NoDelay set",
		},
	)

	// Vector Search Action Metrics
	VectorSearchParseFallbackTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_search_parse_fallback_total",
			Help: "Total number of vector search parse fallbacks",
		},
	)
	VectorSearchActionErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_search_action_errors_total",
			Help: "Total number of vector search action errors",
		},
	)
	ZeroAllocVectorSearchParseTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_zero_alloc_vector_search_parse_total",
			Help: "Total number of zero-alloc vector search parses",
		},
	)
	VectorSearchActionTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_search_action_total",
			Help: "Total number of vector search actions executed",
		},
	)
	VectorSearchActionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_vector_search_action_duration_seconds",
			Help:    "Duration of vector search actions",
			Buckets: []float64{0.001, 0.01, 0.1, 1},
		},
	)

	// Parser Pool Metrics
	ParserPoolGets = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_parser_pool_gets_total",
			Help: "Total number of parser pool retrievals",
		},
	)
	ParserPoolHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_parser_pool_hits_total",
			Help: "Total number of parser pool hits (reused)",
		},
	)
	ParserPoolMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_parser_pool_misses_total",
			Help: "Total number of parser pool misses (allocations)",
		},
	)
	ParserPoolPuts = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_parser_pool_puts_total",
			Help: "Total number of parser pool returns",
		},
	)

	// gRPC Configuration Metrics
	GRPCMaxRecvMsgSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_recv_msg_size_bytes",
			Help: "Configured gRPC maximum receive message size",
		},
	)
	GRPCMaxSendMsgSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_send_msg_size_bytes",
			Help: "Configured gRPC maximum send message size",
		},
	)
	GRPCInitialWindowSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_initial_window_size_bytes",
			Help: "Configured gRPC initial window size",
		},
	)
	GRPCInitialConnWindowSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_initial_conn_window_size_bytes",
			Help: "Configured gRPC initial connection window size",
		},
	)
	GRPCMaxConcurrentStreams = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_grpc_max_concurrent_streams",
			Help: "Configured gRPC maximum concurrent streams",
		},
	)
)
