package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// FlightOperationsTotal counts the number of Flight operations (DoGet, DoPut)
	FlightOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_operations_total",
			Help: "The total number of processed Arrow Flight operations",
		},
		[]string{"method", "status"},
	)

	// FlightDurationSeconds measures the latency of Flight operations
	FlightDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_flight_duration_seconds",
			Help:    "Duration of Arrow Flight operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// FlightBytesProcessed tracks the estimated bytes processed
	FlightBytesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_bytes_processed_total",
			Help: "Total bytes processed in Flight operations",
		},
		[]string{"method"},
	)

	// WalWritesTotal counts WAL write operations
	WalWritesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_wal_writes_total",
			Help: "Total number of WAL write operations",
		},
		[]string{"status"},
	)

	// WalBytesWritten tracks bytes written to WAL
	WalBytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_bytes_written_total",
			Help: "Total bytes written to the Write-Ahead Log",
		},
	)

	// WalReplayDurationSeconds measures time taken to replay WAL on startup
	WalReplayDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_replay_duration_seconds",
			Help:    "Time taken to replay the Write-Ahead Log",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
	)

	// SnapshotTotal counts snapshot operations
	SnapshotTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_snapshot_operations_total",
			Help: "Total number of snapshot operations",
		},
		[]string{"status"},
	)

	// SnapshotDurationSeconds measures duration of snapshot creation
	SnapshotDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_duration_seconds",
			Help:    "Duration of snapshot creation operations",
			Buckets: prometheus.DefBuckets,
		},
	)
	// EvictionsTotal counts the number of evicted records
	EvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_evictions_total",
			Help: "Total number of evicted records due to memory limits",
		},
		[]string{"reason"},
	)

	// IpcDecodeErrorsTotal counts IPC decoding errors and recovered panics
	IpcDecodeErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "longbow",
			Name:      "ipc_decode_errors_total",
			Help:      "Total number of IPC decoding errors and recovered panics",
		},
		[]string{"source", "status"},
	)

	// ValidationFailuresTotal counts record batch validation failures
	ValidationFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_validation_failures_total",
			Help: "Total number of record batch validation failures",
		},
		[]string{"source", "reason"},
	)

	// AdaptiveIndexMigrationsTotal counts migrations from BruteForce to HNSW
	AdaptiveIndexMigrationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_adaptive_index_migrations_total",
			Help: "Total number of migrations from BruteForce to HNSW index",
		},
	)

	// HnswSearchesTotal counts searches performed on HNSW index
	HnswSearchesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_searches_total",
			Help: "Total number of searches performed using HNSW index",
		},
	)

	// BruteForceSearchesTotal counts searches performed using BruteForce scan
	BruteForceSearchesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bruteforce_searches_total",
			Help: "Total number of searches performed using BruteForce linear scan",
		},
	)

	// BinaryQuantizeOpsTotal counts binary quantization operations
	BinaryQuantizeOpsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_binary_quantize_ops_total",
			Help: "Total number of binary quantization operations",
		},
	)

	// POPCNTDistanceOpsTotal counts POPCNT distance calculations
	POPCNTDistanceOpsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_popcnt_distance_ops_total",
			Help: "Total number of POPCNT distance calculations",
		},
	)

	// BitmapPoolMetrics
	BitmapPoolGetsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_gets_total",
			Help: "Total number of bitmap pool get operations",
		},
	)
	BitmapPoolHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_hits_total",
			Help: "Total number of bitmap pool hits",
		},
	)
	BitmapPoolMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_misses_total",
			Help: "Total number of bitmap pool misses",
		},
	)
	BitmapPoolPutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_puts_total",
			Help: "Total number of bitmap pool put operations",
		},
	)
	BitmapPoolDiscardsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_pool_discards_total",
			Help: "Total number of bitmap pool discards",
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
			Help: "Total number of times checkpoint barrier was reached",
		},
	)
	CheckpointTimeoutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_checkpoint_timeouts_total",
			Help: "Total number of checkpoint timeouts",
		},
	)

	// CircuitBreaker Metrics
	CircuitBreakerStateChanges = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_circuit_breaker_state_changes_total",
			Help: "Total number of circuit breaker state changes",
		},
	)
	CircuitBreakerRejections = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_circuit_breaker_rejections_total",
			Help: "Total number of requests rejected by circuit breaker",
		},
	)
	CircuitBreakerSuccesses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_circuit_breaker_successes_total",
			Help: "Total number of successful circuit breaker operations",
		},
	)
	CircuitBreakerFailures = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_circuit_breaker_failures_total",
			Help: "Total number of failed circuit breaker operations",
		},
	)

	// Pool Metrics
	PoolLockWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_pool_lock_wait_duration_seconds",
			Help:    "Time spent waiting for pool locks",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"pool"},
	)

	// Graph Metrics
	GraphTraversalDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_traversal_duration_seconds",
			Help:    "Duration of graph traversal operations",
			Buckets: prometheus.DefBuckets,
		},
	)
	GraphClusteringDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_clustering_duration_seconds",
			Help:    "Duration of graph clustering operations",
			Buckets: prometheus.DefBuckets,
		},
	)
	GraphCommunitiesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_graph_communities_total",
			Help: "Total number of detected graph communities",
		},
	)

	// HNSW Graph Sync Metrics
	HNSWGraphSyncExportsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_graph_sync_exports_total",
			Help: "Total number of graph sync exports",
		},
	)
	HNSWGraphSyncImportsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_graph_sync_imports_total",
			Help: "Total number of graph sync imports",
		},
	)
	HNSWGraphSyncDeltasTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_graph_sync_deltas_total",
			Help: "Total number of graph sync deltas generated",
		},
	)
	HNSWGraphSyncDeltaAppliesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_graph_sync_delta_applies_total",
			Help: "Total number of graph sync deltas applied",
		},
	)

	// Load Balancer Metrics
	LoadBalancerReplicasTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_load_balancer_replicas_total",
			Help: "Total number of replicas tracked by load balancer",
		},
	)
	LoadBalancerUnhealthyTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_load_balancer_unhealthy_total",
			Help: "Total number of unhealthy replicas",
		},
	)
	LoadBalancerSelectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_load_balancer_selections_total",
			Help: "Total number of replica selections for read operations",
		},
		[]string{"strategy"},
	)

	// Memory Backpressure Metrics
	MemoryPressureLevel = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_pressure_level",
			Help: "Current memory pressure level (0-100)",
		},
	)
	MemoryHeapInUse = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_heap_in_use_bytes",
			Help: "Current heap memory in use",
		},
	)
	MemoryBackpressureRejectsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_backpressure_rejects_total",
			Help: "Total number of requests rejected due to memory backpressure",
		},
	)
	MemoryBackpressureAcquiresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_backpressure_acquires_total",
			Help: "Total number of memory permits acquired",
		},
	)
	MemoryBackpressureReleasesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_backpressure_releases_total",
			Help: "Total number of memory permits released",
		},
	)

	// Namespace Metrics
	NamespacesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_namespaces_total",
			Help: "Total number of active namespaces",
		},
	)

	// Replication Metrics
	ReplicationPeersTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_replication_peers_total",
			Help: "Total number of replication peers",
		},
	)
	ReplicationFailuresTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_failures_total",
			Help: "Total number of replication failures",
		},
	)
	ReplicationSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_success_total",
			Help: "Total number of successful replication operations",
		},
	)
	ReplicationRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_retries_total",
			Help: "Total number of replication retries",
		},
	)
	ReplicationQueuedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_queued_total",
			Help: "Total number of operations queued for replication",
		},
	)
	ReplicationQueueDropped = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_replication_queue_dropped_total",
			Help: "Total number of operations dropped from replication queue",
		},
	)

	// Pluggable Index Metrics
	IndexTypesRegistered = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_index_types_registered",
			Help: "Total number of registered index types",
		},
	)
	IndexCreationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_index_creations_total",
			Help: "Total number of index creation attempts",
		},
		[]string{"type", "status"},
	)
	IndexCreationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_index_creation_duration_seconds",
			Help:    "Duration of index creation operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	// Quorum Metrics
	QuorumOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_quorum_operation_duration_seconds",
			Help:    "Duration of quorum operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"operation", "consistency"},
	)
	QuorumSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quorum_success_total",
			Help: "Total number of successful quorum operations",
		},
		[]string{"operation", "consistency"},
	)
	QuorumFailureTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quorum_failure_total",
			Help: "Total number of failed quorum operations",
		},
		[]string{"operation", "consistency", "reason"},
	)

	// Record Access Metrics
	RecordAccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_record_access_total",
			Help: "Total number of record accesses (LRU tracking)",
		},
	)
	RecordMetadataEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_record_metadata_entries",
			Help: "Number of entries in record eviction metadata map",
		},
	)

	// Semaphore Metrics
	SemaphoreWaitingRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_semaphore_waiting_requests",
			Help: "Current number of requests waiting for semaphore",
		},
	)
	SemaphoreActiveRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_semaphore_active_requests",
			Help: "Current number of requests holding semaphore",
		},
	)
	SemaphoreAcquiredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_semaphore_acquired_total",
			Help: "Total number of semaphore acquisitions",
		},
	)
	SemaphoreTimeoutsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_semaphore_timeouts_total",
			Help: "Total number of semaphore acquisition timeouts",
		},
	)
	SemaphoreQueueDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_semaphore_queue_duration_seconds",
			Help:    "Time spent waiting in semaphore queue",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)

	// Schema Evolution Metrics
	SchemaVersionCurrent = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_schema_version_current",
			Help: "Current schema version for dataset",
		},
		[]string{"dataset"},
	)
	SchemaEvolutionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_schema_evolution_duration_seconds",
			Help:    "Duration of schema evolution operations",
			Buckets: prometheus.DefBuckets,
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
	SchemaTableDroppedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_schema_table_dropped_total",
			Help: "Total number of tables/datasets dropped",
		},
	)

	// Split Brain Metrics
	SplitBrainHeartbeatsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_split_brain_heartbeats_total",
			Help: "Total number of split brain detector heartbeats",
		},
	)
	SplitBrainHealthyPeers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_split_brain_healthy_peers",
			Help: "Current number of healthy peers seen by detector",
		},
	)
	SplitBrainPartitionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_split_brain_partitions_total",
			Help: "Total number of partition events detected",
		},
	)
	SplitBrainFenced = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_split_brain_fenced_state",
			Help: "Whether the node is currently fenced (1=fenced, 0=normal)",
		},
	)

	// Vector Clock Metrics
	VectorClockMergesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_clock_merges_total",
			Help: "Total number of vector clock merges",
		},
	)
	VectorClockConflictsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_clock_conflicts_total",
			Help: "Total number of vector clock conflicts detected",
		},
	)

	// FlightRowsProcessed counts rows processed in Flight operations
	FlightRowsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_flight_rows_processed_total",
			Help: "Total number of rows processed in Flight operations",
		},
		[]string{"method", "status"},
	)

	// DoPutPayloadSizeBytes tracks the distribution of DoPut batch sizes
	DoPutPayloadSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doput_payload_size_bytes",
			Help:    "Size of DoPut record batches in bytes",
			Buckets: []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216}, // 1KB to 16MB
		},
	)

	// VectorSearchLatencySeconds measures latency of vector search operations
	VectorSearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_vector_search_latency_seconds",
			Help:    "Latency of vector search operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"dataset"},
	)

	// TombstonesTotal counts active tombstones per dataset
	TombstonesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_tombstones_total",
			Help: "Total number of active tombstones",
		},
		[]string{"dataset"},
	)
)

// VectorIndexSize tracks the number of vectors in the index
var VectorIndexSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_vector_index_size",
		Help: "Current number of vectors in the index",
	},
)

// AverageVectorNorm tracks the average L2 norm of stored vectors
var AverageVectorNorm = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_average_vector_norm",
		Help: "Average L2 norm of vectors in the index",
	},
)

// IndexBuildLatency measures the time taken to rebuild or update the index
var IndexBuildLatency = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_index_build_latency_seconds",
		Help:    "Latency of vector index build operations",
		Buckets: prometheus.DefBuckets,
	},
)

// MemoryFragmentationRatio tracks the ratio of allocated to used memory
var MemoryFragmentationRatio = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_memory_fragmentation_ratio",
		Help: "Ratio of system memory reserved vs used (fragmentation indicator)",
	},
)

// ShardLockWaitDuration measures time spent waiting for shard locks
var ShardLockWaitDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_shard_lock_wait_seconds",
		Help:    "Time spent waiting to acquire shard locks",
		Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
	},
)

// WalBufferPoolOperations counts buffer pool Get/Put operations
var WalBufferPoolOperations = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_wal_buffer_pool_operations_total",
		Help: "Total number of WAL buffer pool operations",
	},
	[]string{"operation"},
)

// HnswActiveReaders tracks the number of active zero-copy readers per dataset
var HnswActiveReaders = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_hnsw_active_readers",
		Help: "Number of active zero-copy readers per dataset",
	},
	[]string{"dataset"},
)

// ShardLockWaitDuration measures time spent waiting for shard locks

// =============================================================================
// New Prometheus metrics for enhanced observability
// =============================================================================

// 1. WalFsyncDurationSeconds - Time taken for walFile.Sync() calls
var WalFsyncDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_wal_fsync_duration_seconds",
		Help:    "Time taken for WAL fsync operations",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	},
	[]string{"status"},
)

// 2. WalBatchSize - Number of entries flushed per batch
var WalBatchSize = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_wal_batch_size",
		Help:    "Number of entries flushed per WAL batch",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
	},
)

// 3. WalPendingEntries - Current length of WAL entries channel
var WalPendingEntries = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_wal_pending_entries",
		Help: "Current number of pending WAL entries (backpressure indicator)",
	},
)

// 4. IndexQueueDepth - Current length of indexing channel
var IndexQueueDepth = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_index_queue_depth",
		Help: "Current depth of the indexing queue (lag indicator)",
	},
)

// 5. IndexJobLatencySeconds - Time from job creation to completion
var IndexJobLatencySeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_index_job_latency_seconds",
		Help:    "Latency of index job processing by dataset",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	},
	[]string{"dataset"},
)

// 6. DatasetRecordBatchesCount - Number of batches per dataset (fragmentation)
var DatasetRecordBatchesCount = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_dataset_record_batches_count",
		Help: "Number of record batches per dataset (high = fragmentation)",
	},
	[]string{"dataset"},
)

// 7. FilterExecutionDurationSeconds - Time spent applying filters
var FilterExecutionDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "longbow",
		Name:      "filter_execution_duration_seconds",
		Help:      "Duration of filter execution by dataset",
		Buckets:   []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
	},
	[]string{"dataset"},
)

// 8. FilterSelectivityRatio - Ratio of rows output / rows input
var FilterSelectivityRatio = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_filter_selectivity_ratio",
		Help:    "Filter selectivity ratio (output rows / input rows)",
		Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0},
	},
	[]string{"dataset"},
)

// 9. HnswGraphHeight - Max layer of the HNSW graph
var HnswGraphHeight = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_hnsw_graph_height",
		Help: "Maximum layer height of the HNSW graph (search complexity)",
	},
	[]string{"dataset"},
)

// 10. HnswNodeCount - Total nodes in the HNSW graph
var HnswNodeCount = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_hnsw_node_count",
		Help: "Total number of nodes in the HNSW graph",
	},
	[]string{"dataset"},
)

// HnswShardingMigrationsTotal counts HNSW index migrations to sharded format
var HnswShardingMigrationsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_hnsw_sharding_migrations_total",
		Help: "Total number of HNSW index migrations to sharded format",
	},
)

// 11. FlightTicketParseDurationSeconds - Time spent parsing JSON ticket
var FlightTicketParseDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_flight_ticket_parse_duration_seconds",
		Help:    "Time spent parsing Flight ticket JSON",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
	},
)

// 12. VectorScratchPoolMissesTotal - Pool allocation misses
var VectorScratchPoolMissesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_vector_scratch_pool_misses_total",
		Help: "Count of scratch buffer pool misses requiring allocation",
	},
)

// 13. DatasetLockWaitDurationSeconds - Time waiting for dataset locks
var DatasetLockWaitDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_dataset_lock_wait_duration_seconds",
		Help:    "Time spent waiting for dataset mutex by operation type",
		Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
	},
	[]string{"operation"},
)

// 14. ArrowMemoryUsedBytes - Arrow allocator memory usage
var ArrowMemoryUsedBytes = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_arrow_memory_used_bytes",
		Help: "Memory bytes used by Arrow allocator",
	},
	[]string{"allocator"},
)

var (
	// 15. SimdDispatchCount - SIMD implementation dispatch counts
	SimdDispatchCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_dispatch_total",
			Help: "Count of SIMD implementation selections",
		},
		[]string{"implementation"},
	)

	// =============================================================================
	// Phase 19: Production Observability Metrics
	// =============================================================================

	// ZeroCopyRatio tracks the ratio of zero-copy reads vs copy-based reads
	ZeroCopyRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_zero_copy_ratio",
			Help: "Ratio of zero-copy reads to total reads (0.0-1.0)",
		},
		[]string{"dataset"},
	)

	// PipelineUtilization tracks usage of the DoGet pipeline vs direct path
	PipelineUtilization = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_pipeline_utilization_total",
			Help: "Count of operations processed via pipeline vs direct path",
		},
		[]string{"path"},
	)

	// TraceSpansTotal counts generated OpenTelemetry spans
	TraceSpansTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_spans_total",
			Help: "Total number of OpenTelemetry spans generated",
		},
		[]string{"operation"},
	)
)

// 16. SnapshotWriteDurationSeconds - Time to write Parquet snapshot
var SnapshotWriteDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_snapshot_write_duration_seconds",
		Help:    "Duration of Parquet snapshot write operations",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
	},
)

// 17. SnapshotSizeBytes - Size of generated snapshots
var SnapshotSizeBytes = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_snapshot_size_bytes",
		Help:    "Size of generated Parquet snapshots in bytes",
		Buckets: []float64{1e4, 1e5, 1e6, 1e7, 1e8, 1e9}, // 10KB to 1GB
	},
)

// 18. GcPauseDurationSeconds - Go GC pause times
var GcPauseDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_gc_pause_duration_seconds",
		Help:    "Go garbage collector pause durations",
		Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
	},
)

// 19. ActiveSearchContexts - Concurrent DoGet requests
var ActiveSearchContexts = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_active_search_contexts",
		Help: "Number of concurrent DoGet/search operations in progress",
	},
)

// 20. CompactionOperationsTotal - Batch compaction tracking
var CompactionOperationsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_compaction_operations_total",
		Help: "Total compaction operations by status",
	},
	[]string{"dataset", "status"},
)

// CompactionDurationSeconds - Time taken for compaction
var CompactionDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_compaction_duration_seconds",
		Help:    "Duration of compaction operations",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"dataset"},
)

// CompactionRecordsRemovedTotal - Records removed during compaction
var CompactionRecordsRemovedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_compaction_records_removed_total",
		Help: "Total number of records removed during compaction",
	},
	[]string{"dataset"},
)

// 21. CompactionAutoTriggersTotal - Auto-triggered compaction tracking
var CompactionAutoTriggersTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_compaction_auto_triggers_total",
		Help: "Total number of auto-triggered compactions when batch count exceeds threshold",
	},
)

// =============================================================================
// IO_Uring Metrics
// =============================================================================

// WalUringSubmissionQueueDepth tracks the number of entries in the submission queue
var WalUringSubmissionQueueDepth = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_wal_uring_sq_depth",
		Help: "Current depth of the io_uring submission queue",
	},
)

// WalUringCompletionQueueDepth tracks the number of entries in the completion queue
var WalUringCompletionQueueDepth = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_wal_uring_cq_depth",
		Help: "Current depth of the io_uring completion queue",
	},
)

// WalUringSubmitLatencySeconds measures the latency of io_uring submission calls
var WalUringSubmitLatencySeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_wal_uring_submit_latency_seconds",
		Help:    "Latency of io_uring Enter/Submit calls",
		Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01}, // Microsecond resolution
	},
)

// =============================================================================
// Global Search Metrics
// =============================================================================

// GlobalSearchDuration measures latency of scatter-gather search operations
var (
	GlobalSearchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_global_search_duration_seconds",
		Help:    "Latency of global search operations",
		Buckets: prometheus.DefBuckets,
	})
	GlobalSearchFanoutSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_global_search_fanout_size",
		Help:    "Number of peers queried during global search",
		Buckets: []float64{1, 2, 3, 5, 10, 20, 50, 100},
	})
	GlobalSearchPartialFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_global_search_partial_failures_total",
		Help: "Total number of failed peer queries during global search",
	})
	IDResolutionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_id_resolution_duration_seconds",
		Help:    "Latency of resolving internal IDs to user IDs",
		Buckets: prometheus.DefBuckets,
	})
)

// =============================================================================
// Comprehensive Prometheus Metrics Expansion
// Added for enhanced observability across all components
// =============================================================================

// -----------------------------------------------------------------------------
// SearchArena Metrics
// -----------------------------------------------------------------------------

// ArenaAllocBytesTotal tracks total bytes allocated from search arenas
var ArenaAllocBytesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_arena_alloc_bytes_total",
		Help: "Total bytes allocated from search arenas",
	},
)

// ArenaOverflowTotal counts arena capacity overflow events
var ArenaOverflowTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_arena_overflow_total",
		Help: "Total arena capacity overflow events requiring heap fallback",
	},
)

// ArenaResetsTotal counts arena reset operations
var ArenaResetsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_arena_resets_total",
		Help: "Total arena reset operations",
	},
)

// ArenaPoolGets counts arena acquisitions from the global pool
var ArenaPoolGets = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_arena_pool_gets_total",
		Help: "Total arena acquisitions from global pool",
	},
)

// ArenaPoolPuts counts arena returns to the global pool
var ArenaPoolPuts = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_arena_pool_puts_total",
		Help: "Total arena returns to global pool",
	},
)

// -----------------------------------------------------------------------------
// Result Pool Metrics
// -----------------------------------------------------------------------------

// ResultPoolHitsTotal counts pool hits by k-size
var ResultPoolHitsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_result_pool_hits_total",
		Help: "Total result pool hits by k-size",
	},
	[]string{"k_size"},
)

// ResultPoolMissesTotal counts pool misses by k-size
var ResultPoolMissesTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_result_pool_misses_total",
		Help: "Total result pool misses by k-size",
	},
	[]string{"k_size"},
)

// -----------------------------------------------------------------------------
// Bloom Filter Metrics
// -----------------------------------------------------------------------------

// BloomLookupsTotal counts bloom filter lookups by result
var BloomLookupsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_bloom_lookups_total",
		Help: "Total bloom filter lookups by result (hit/miss)",
	},
	[]string{"result"},
)

// BloomFalsePositiveRate tracks observed false positive rate
var BloomFalsePositiveRate = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_bloom_false_positive_rate",
		Help: "Observed bloom filter false positive rate",
	},
)

// -----------------------------------------------------------------------------
// Column Index Metrics
// -----------------------------------------------------------------------------

// ColumnIndexSize tracks index size by dataset and column
var ColumnIndexSize = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_column_index_size",
		Help: "Column inverted index size by dataset and column",
	},
	[]string{"dataset", "column"},
)

// ColumnIndexLookupDuration measures index lookup latency
var ColumnIndexLookupDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_column_index_lookup_duration_seconds",
		Help:    "Column index lookup duration by dataset",
		Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
	},
	[]string{"dataset"},
)

// -----------------------------------------------------------------------------
// HNSW Search Metrics
// -----------------------------------------------------------------------------

// HnswNodesVisited tracks nodes visited per search
var HnswNodesVisited = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_hnsw_nodes_visited",
		Help:    "Number of HNSW nodes visited per search",
		Buckets: []float64{10, 25, 50, 100, 200, 500, 1000, 2500, 5000},
	},
	[]string{"dataset"},
)

// HnswDistanceCalculations counts total distance computations
var HnswDistanceCalculations = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_hnsw_distance_calculations_total",
		Help: "Total HNSW distance calculations performed",
	},
)

// -----------------------------------------------------------------------------
// Peer Replication Metrics
// -----------------------------------------------------------------------------

// ReplicationLagSeconds tracks replication lag by peer
var ReplicationLagSeconds = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_replication_lag_seconds",
		Help: "Replication lag in seconds by peer",
	},
	[]string{"peer"},
)

// PeerHealthStatus tracks peer health (0=down, 1=up)
var PeerHealthStatus = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_peer_health_status",
		Help: "Peer health status (0=down, 1=up)",
	},
	[]string{"peer"},
)

// GossipActiveMembers tracks the number of alive members in the mesh
var GossipActiveMembers = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_gossip_active_members",
		Help: "Current number of alive members in the gossip mesh",
	},
)

// GossipPingsTotal counts gossip pings sent and received
var GossipPingsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_gossip_pings_total",
		Help: "Total number of gossip pings",
	},
	[]string{"direction"}, // "sent", "received"
)

// MeshSyncDeltasTotal counts record batches replicated via mesh sync
var MeshSyncDeltasTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_mesh_sync_deltas_total",
		Help: "Total number of record batches replicated via mesh sync",
	},
	[]string{"status"}, // "success", "error"
)

// MeshSyncBytesTotal counts bytes replicated via mesh sync
var MeshSyncBytesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_mesh_sync_bytes_total",
		Help: "Total bytes replicated via mesh sync",
	},
)

// MeshMerkleMatchTotal counts Merkle root comparison results
var MeshMerkleMatchTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_mesh_merkle_match_total",
		Help: "Total Merkle root comparison results",
	},
	[]string{"result"}, // "match", "mismatch"
)

// -----------------------------------------------------------------------------
// Flight Client Pool Metrics
// -----------------------------------------------------------------------------

// FlightPoolConnectionsActive tracks active connections by host
var FlightPoolConnectionsActive = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_flight_pool_connections_active",
		Help: "Active Flight client pool connections by host",
	},
	[]string{"host"},
)

// FlightPoolWaitDuration measures time waiting for connections
var FlightPoolWaitDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_flight_pool_wait_duration_seconds",
		Help:    "Time spent waiting for Flight pool connection by host",
		Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1, 10},
	},
	[]string{"host"},
)

// -----------------------------------------------------------------------------
// DoGet Pipeline Metrics
// -----------------------------------------------------------------------------

// PipelineBatchesPerSecond tracks pipeline throughput
var PipelineBatchesPerSecond = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_pipeline_batches_per_second",
		Help: "DoGet pipeline throughput in batches per second (gauge)",
	},
)

// PipelineBatchesTotal tracks total historical pipeline throughput
var PipelineBatchesTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_pipeline_batches_total",
		Help: "Total number of batches processed via DoGet pipeline",
	},
)

// PipelineWorkerUtilization tracks worker busy percentage
var PipelineWorkerUtilization = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_pipeline_worker_utilization",
		Help: "DoGet pipeline worker utilization (0-1)",
	},
	[]string{"worker_id"},
)

// -----------------------------------------------------------------------------
// Adaptive WAL Metrics
// -----------------------------------------------------------------------------

// WalAdaptiveIntervalMs tracks current adaptive flush interval
var WalAdaptiveIntervalMs = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_wal_adaptive_interval_ms",
		Help: "Current adaptive WAL flush interval in milliseconds",
	},
)

// WalWriteRatePerSecond tracks current write rate
var WalWriteRatePerSecond = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_wal_write_rate_per_second",
		Help: "Current WAL write rate per second",
	},
)

// -----------------------------------------------------------------------------
// Zero-Copy / HNSW Epoch Metrics
// -----------------------------------------------------------------------------

// HnswEpochTransitions counts epoch advancement events
var HnswEpochTransitions = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_hnsw_epoch_transitions_total",
		Help: "Total HNSW epoch transitions for zero-copy access",
	},
)

// -----------------------------------------------------------------------------
// Fast Path Filter Metrics
// -----------------------------------------------------------------------------

// FastPathUsageTotal counts fast path vs fallback usage
var FastPathUsageTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_fast_path_usage_total",
		Help: "Filter fast path usage count (fast/fallback)",
	},
	[]string{"path"},
)

// -----------------------------------------------------------------------------
// IPC Buffer Pool Metrics
// -----------------------------------------------------------------------------

// IpcBufferPoolUtilization tracks buffer pool utilization
var IpcBufferPoolUtilization = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_ipc_buffer_pool_utilization",
		Help: "IPC buffer pool utilization ratio (0-1)",
	},
)

// IpcBufferPoolHits counts pool hits
var IpcBufferPoolHits = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_ipc_buffer_pool_hits_total",
		Help: "Total IPC buffer pool hits",
	},
)

// IpcBufferPoolMisses counts pool misses
var IpcBufferPoolMisses = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_ipc_buffer_pool_misses_total",
		Help: "Total IPC buffer pool misses",
	},
)

// -----------------------------------------------------------------------------
// S3 Backend Metrics
// -----------------------------------------------------------------------------

// S3RequestDuration measures S3 operation latency
var S3RequestDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_s3_request_duration_seconds",
		Help:    "S3 request duration by operation type",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
	},
	[]string{"operation"},
)

// S3RetriesTotal counts S3 retries by operation
var S3RetriesTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_s3_retries_total",
		Help: "Total S3 operation retries by operation type",
	},
	[]string{"operation"},
)

// -----------------------------------------------------------------------------
// Warmup Metrics
// -----------------------------------------------------------------------------

// WarmupProgressPercent tracks warmup progress
var WarmupProgressPercent = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_warmup_progress_percent",
		Help: "Warmup progress percentage (0-100)",
	},
)

// WarmupDatasetsTotal tracks total datasets to warm up
var WarmupDatasetsTotal = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_warmup_datasets_total",
		Help: "Total datasets to warm up",
	},
)

// WarmupDatasetsCompleted tracks completed warmup datasets
var WarmupDatasetsCompleted = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_warmup_datasets_completed",
		Help: "Number of datasets warmed up",
	},
)

// -----------------------------------------------------------------------------
// Sharded HNSW Metrics
// -----------------------------------------------------------------------------

// ShardedHnswLoadFactor tracks load factor by shard
var ShardedHnswLoadFactor = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_sharded_hnsw_load_factor",
		Help: "Sharded HNSW load factor by shard (0-1)",
	},
	[]string{"dataset", "shard"},
)

// -----------------------------------------------------------------------------
// Phase 5: Final Optimization Metrics (New)
// -----------------------------------------------------------------------------

// DoGetZeroCopyTotal counts zero-copy vs key-copy operations
var DoGetZeroCopyTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_doget_zero_copy_total",
		Help: "Total DoGet operations by copy method (zero-copy vs deep-copy)",
	},
	[]string{"type"}, // "zero_copy", "copy"
)

// DoGetPipelineStepsTotal counts pipeline vs simple path usage
var DoGetPipelineStepsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "longbow_doget_pipeline_steps_total",
		Help: "Total DoGet pipeline steps processed by method",
	},
	[]string{"method"}, // "pipeline", "simple"
)

// NumaWorkerDistribution tracks workers pinned to NUMA nodes
var NumaWorkerDistribution = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_numa_worker_distribution",
		Help: "Number of workers pinned to each NUMA node",
	},
	[]string{"node"},
)

// =============================================================================
// SIMD Metrics (Restored)
// =============================================================================

// CosineBatchCallsTotal counts batch cosine distance calls
var CosineBatchCallsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_simd_cosine_batch_calls_total",
		Help: "Total number of batch cosine distance calculations",
	},
)

// DotProductBatchCallsTotal counts batch dot product calls
var DotProductBatchCallsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_simd_dot_product_batch_calls_total",
		Help: "Total number of batch dot product calculations",
	},
)

// ParallelReductionVectorsProcessed tracks vectors processed by parallel reduction
var ParallelReductionVectorsProcessed = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_simd_parallel_reduction_vectors_total",
		Help: "Total number of vectors processed using parallel reduction",
	},
)

// =============================================================================
// WAL & Exchange Metrics (Restored)
// =============================================================================

// WALLockWaitDuration measures time waiting for WAL locks
var WALLockWaitDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_wal_lock_wait_duration_seconds",
		Help:    "Time spent waiting for WAL locks",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
	},
	[]string{"type"}, // "data", "cond"
)

// DoExchangeCallsTotal counts calls to DoExchange (gossip)
var DoExchangeCallsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_do_exchange_calls_total",
		Help: "Total number of DoExchange (gossip) calls",
	},
)

// DoExchangeErrorsTotal counts failed DoExchange calls
var DoExchangeErrorsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_do_exchange_errors_total",
		Help: "Total number of failed DoExchange (gossip) calls",
	},
)

// DoExchangeBatchesReceivedTotal counts batches received during gossip
var DoExchangeBatchesReceivedTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_do_exchange_batches_received_total",
		Help: "Total number of record batches received via DoExchange",
	},
)

// DoExchangeBatchesSentTotal counts batches sent during gossip
var DoExchangeBatchesSentTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_do_exchange_batches_sent_total",
		Help: "Total number of record batches sent via DoExchange",
	},
)

// DoExchangeDurationSeconds measures latency of gossip exchange
var DoExchangeDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_do_exchange_duration_seconds",
		Help:    "Latency of DoExchange (gossip) operations",
		Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10},
	},
)

// IndexLockWaitDuration measures time waiting for index RWMutex
var IndexLockWaitDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "longbow_index_lock_wait_duration_seconds",
		Help:    "Time spent waiting for index locks",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1},
	},
	[]string{"dataset", "type"}, // "dataset", "read/write"
)

// =============================================================================
// Batch, Sharding & TCP Metrics (Restored)
// =============================================================================

// BatchDistanceDurationSeconds measures latency of batch distance calcs
var BatchDistanceDurationSeconds = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_batch_distance_duration_seconds",
		Help:    "Latency of batch distance calculations",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
	},
)

// BatchDistanceCallsTotal counts batch distance function calls
var BatchDistanceCallsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_batch_distance_calls_total",
		Help: "Total number of batch distance function calls",
	},
)

// BatchDistanceBatchSize tracks distribution of batch sizes
var BatchDistanceBatchSize = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_batch_distance_batch_size",
		Help:    "Size of batches in distance calculations",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000},
	},
)

// ShardedHnswShardSize tracks number of vectors per shard
var ShardedHnswShardSize = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "longbow_sharded_hnsw_shard_size",
		Help: "Number of vectors in each HNSW shard",
	},
	[]string{"dataset", "shard"},
)

// TCPNoDelayConnectionsTotal counts connections with TCP_NODELAY set
var TCPNoDelayConnectionsTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_tcp_nodelay_connections_total",
		Help: "Total connections configured with TCP_NODELAY",
	},
)

// VectorSearchParseFallbackTotal counts fallbacks to standard JSON parsing
var VectorSearchParseFallbackTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_vector_search_parse_fallback_total",
		Help: "Total fallback to standard JSON parsing for search queries",
	},
)

// ZeroAllocVectorSearchParseTotal counts successful zero-alloc parsing
var ZeroAllocVectorSearchParseTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_zero_alloc_vector_search_parse_total",
		Help: "Total successful zero-allocation parsing of search queries",
	},
)

// VectorSearchActionErrors counts errors in search action processing
var VectorSearchActionErrors = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_vector_search_action_errors_total",
		Help: "Total errors during vector search action processing",
	},
)

// VectorSearchActionTotal counts total search action requests
var VectorSearchActionTotal = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_vector_search_action_requests_total",
		Help: "Total vector search action requests processed",
	},
)

// VectorSearchActionDuration measures latency of search actions
var VectorSearchActionDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_vector_search_action_duration_seconds",
		Help:    "Latency of vector search action requests",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	},
)

// =============================================================================
// Parser Pool Metrics (Restored)
// =============================================================================

// ParserPoolGets counts wrapper acquisitions from the pool
var ParserPoolGets = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_parser_pool_gets_total",
		Help: "Total number of parser wrapper acquisitions from the pool",
	},
)

// ParserPoolHits counts wrapper pool hits
var ParserPoolHits = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_parser_pool_hits_total",
		Help: "Total number of parser wrapper pool hits",
	},
)

// ParserPoolMisses counts wrapper pool misses
var ParserPoolMisses = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_parser_pool_misses_total",
		Help: "Total number of parser wrapper pool misses",
	},
)

// ParserPoolPuts counts wrapper returns to the pool
var ParserPoolPuts = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "longbow_parser_pool_puts_total",
		Help: "Total number of parser wrapper returns to the pool",
	},
)

// =============================================================================
// gRPC Configuration Metrics (Restored)
// =============================================================================

// GRPCMaxRecvMsgSizeBytes tracks the configured max receive message size
var GRPCMaxRecvMsgSizeBytes = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_grpc_max_recv_msg_size_bytes",
		Help: "Configured maximum gRPC receive message size in bytes",
	},
)

// GRPCMaxSendMsgSizeBytes tracks the configured max send message size
var GRPCMaxSendMsgSizeBytes = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_grpc_max_send_msg_size_bytes",
		Help: "Configured maximum gRPC send message size in bytes",
	},
)

// GRPCInitialWindowSizeBytes tracks the configured initial window size
var GRPCInitialWindowSizeBytes = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_grpc_initial_window_size_bytes",
		Help: "Configured gRPC initial window size in bytes",
	},
)

// GRPCInitialConnWindowSizeBytes tracks the configured initial connection window size
var GRPCInitialConnWindowSizeBytes = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_grpc_initial_conn_window_size_bytes",
		Help: "Configured gRPC initial connection window size in bytes",
	},
)

// GRPCMaxConcurrentStreams tracks the configured max concurrent streams
var GRPCMaxConcurrentStreams = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_grpc_max_concurrent_streams",
		Help: "Configured maximum concurrent gRPC streams",
	},
)

// InvertedIndexPostingsTotal tracks total postings in inverted indexes
var InvertedIndexPostingsTotal = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "longbow_inverted_index_postings_total",
		Help: "Total number of postings in inverted indexes",
	},
)

// =============================================================================
// Hybrid Search Metrics (Phase 20)
// =============================================================================

var (
	// HybridSearchVectorTotal counts dense vector searches in hybrid path
	HybridSearchVectorTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_vector_total",
			Help: "Total dense vector searches in hybrid path",
		},
	)

	// HybridSearchKeywordTotal counts sparse keyword searches in hybrid path
	HybridSearchKeywordTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_keyword_total",
			Help: "Total sparse keyword searches in hybrid path",
		},
	)

	// BM25DocumentsIndexedTotal counts documents added to BM25 index
	BM25DocumentsIndexedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bm25_documents_indexed_total",
			Help: "Total documents added to the BM25 inverted index",
		},
	)

	// HNSWPQEnabled tracks if PQ is enabled per dataset
	HNSWPQEnabled = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_pq_enabled",
			Help: "Whether Product Quantization is enabled (1) or disabled (0) for the dataset",
		},
		[]string{"dataset"},
	)

	// HNSWPQTrainingDuration tracks training latency
	HNSWPQTrainingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_pq_training_duration_seconds",
			Help:    "Time taken to train PQ encoder for a dataset",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300},
		},
		[]string{"dataset"},
	)

	// NUMACrossNodeAccessTotal counts memory accesses across NUMA nodes
	NUMACrossNodeAccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_numa_cross_node_access_total",
			Help: "Total number of memory accesses where worker node != data node",
		},
		[]string{"worker_node", "data_node"},
	)
)
