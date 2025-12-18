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
