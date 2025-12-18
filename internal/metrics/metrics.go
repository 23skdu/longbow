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
Name: "longbow_flight_duration_seconds",
Help: "Duration of Arrow Flight operations",
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
Name: "longbow_wal_replay_duration_seconds",
Help: "Time taken to replay the Write-Ahead Log",
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
Name: "longbow_snapshot_duration_seconds",
Help: "Duration of snapshot creation operations",
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
Name: "longbow_index_build_latency_seconds",
Help: "Latency of vector index build operations",
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
Name: "longbow_shard_lock_wait_seconds",
Help: "Time spent waiting to acquire shard locks",
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
Name:    "longbow_filter_execution_duration_seconds",
Help:    "Duration of filter execution by operator type",
Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
},
[]string{"operator"},
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

// 15. SimdDispatchCount - SIMD implementation dispatch counts
var SimdDispatchCount = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "longbow_simd_dispatch_count_total",
Help: "Count of SIMD dispatch calls by implementation",
},
[]string{"impl"},
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
[]string{"status"},
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
Help: "DoGet pipeline throughput in batches per second",
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
[]string{"shard_id"},
)

// ShardedHnswShardSize tracks shard sizes
var ShardedHnswShardSize = promauto.NewGaugeVec(
prometheus.GaugeOpts{
Name: "longbow_sharded_hnsw_shard_size",
Help: "Sharded HNSW shard size (vector count)",
},
[]string{"shard_id"},
)

// -----------------------------------------------------------------------------
// Record Size Cache Metrics
// -----------------------------------------------------------------------------

// RecordSizeCacheHitRate tracks cache hit rate
var RecordSizeCacheHitRate = promauto.NewGauge(
prometheus.GaugeOpts{
Name: "longbow_record_size_cache_hit_rate",
Help: "Record size cache hit rate (0-1)",
},
)

// BitmapPool metrics
var BitmapPoolGetsTotal = promauto.NewCounter(
prometheus.CounterOpts{
Name: "longbow_bitmap_pool_gets_total",
Help: "Total bitmap buffer get operations",
},
)

var BitmapPoolHitsTotal = promauto.NewCounter(
prometheus.CounterOpts{
Name: "longbow_bitmap_pool_hits_total",
Help: "Total bitmap buffer pool hits (reused buffers)",
},
)

var BitmapPoolMissesTotal = promauto.NewCounter(
prometheus.CounterOpts{
Name: "longbow_bitmap_pool_misses_total",
Help: "Total bitmap buffer pool misses (new allocations)",
},
)

var BitmapPoolPutsTotal = promauto.NewCounter(
prometheus.CounterOpts{
Name: "longbow_bitmap_pool_puts_total",
Help: "Total bitmap buffer put (return) operations",
},
)

var BitmapPoolDiscardsTotal = promauto.NewCounter(
prometheus.CounterOpts{
Name: "longbow_bitmap_pool_discards_total",
Help: "Total bitmap buffers discarded (oversized)",
},
)


// -----------------------------------------------------------------------------
// PerP Result Pool Metrics
// -----------------------------------------------------------------------------

var (
// PerPPoolGetsTotal tracks total Get operations per shard
PerPPoolGetsTotal = promauto.NewCounterVec(prometheus.CounterOpts{

Name:      "longbow_perp_pool_gets_total",
Help:      "Total number of Get operations on PerP result pool",
}, []string{"shard"})

// PerPPoolPutsTotal tracks total Put operations per shard
PerPPoolPutsTotal = promauto.NewCounterVec(prometheus.CounterOpts{

Name:      "longbow_perp_pool_puts_total",
Help:      "Total number of Put operations on PerP result pool",
}, []string{"shard"})

// PerPPoolHitsTotal tracks pool hits (buffer reuse)
PerPPoolHitsTotal = promauto.NewCounter(prometheus.CounterOpts{

Name:      "longbow_perp_pool_hits_total",
Help:      "Total number of pool hits (buffer reuse) on PerP result pool",
})

// PerPPoolMissesTotal tracks pool misses (new allocations)
PerPPoolMissesTotal = promauto.NewCounter(prometheus.CounterOpts{

Name:      "longbow_perp_pool_misses_total",
Help:      "Total number of pool misses (new allocations) on PerP result pool",
})

// PerPPoolShardDistribution tracks distribution of operations across shards
PerPPoolShardDistribution = promauto.NewHistogramVec(prometheus.HistogramOpts{

Name:      "longbow_perp_pool_shard_distribution",
Help:      "Distribution of operations across PerP pool shards",
Buckets:   prometheus.LinearBuckets(0, 1, 16),
}, []string{"operation"})
)

// RecordSizeCacheHitsTotal tracks total cache hits
var RecordSizeCacheHitsTotal = promauto.NewCounter(
prometheus.CounterOpts{
Namespace: "longbow",
Name:      "record_size_cache_hits_total",
Help:      "Total number of record size cache hits",
},
)

// RecordSizeCacheMissesTotal tracks total cache misses
var RecordSizeCacheMissesTotal = promauto.NewCounter(
prometheus.CounterOpts{
Namespace: "longbow",
Name:      "record_size_cache_misses_total",
Help:      "Total number of record size cache misses",
},
)
