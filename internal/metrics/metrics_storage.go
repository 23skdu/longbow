package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Storage & WAL Metrics
// =============================================================================

var (
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

	// WalBufferPoolOperations counts buffer pool Get/Put operations
	WalBufferPoolOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_wal_buffer_pool_operations_total",
			Help: "Total number of WAL buffer pool operations",
		},
		[]string{"operation"},
	)

	// 1. WalFsyncDurationSeconds - Time taken for walFile.Sync() calls
	WalFsyncDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_fsync_duration_seconds",
			Help:    "Time taken for WAL fsync operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"status"},
	)

	// 2. WalBatchSize - Number of entries flushed per batch
	WalBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_batch_size",
			Help:    "Number of entries flushed per WAL batch",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
		},
	)

	// 3. WalPendingEntries - Current length of WAL entries channel
	WalPendingEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_pending_entries",
			Help: "Current number of pending WAL entries (backpressure indicator)",
		},
	)

	// WalAdaptiveIntervalMs tracks current adaptive flush interval
	WalAdaptiveIntervalMs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_adaptive_interval_ms",
			Help: "Current adaptive WAL flush interval in milliseconds",
		},
	)

	// WalWriteRatePerSecond tracks current write rate
	WalWriteRatePerSecond = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_write_rate_per_second",
			Help: "Current WAL write rate per second",
		},
	)

	// WalRingBufferUtilization tracks ring buffer usage (0-1)
	WalRingBufferUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_ring_buffer_utilization",
			Help: "Current utilization of WAL ring buffer (0-1)",
		},
	)

	// WalRingBufferPushesTotal counts successful ring buffer pushes
	WalRingBufferPushesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_pushes_total",
			Help: "Total number of successful ring buffer push operations",
		},
	)

	// WalRingBufferDrainsTotal counts ring buffer drain operations
	WalRingBufferDrainsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_drains_total",
			Help: "Total number of ring buffer drain operations",
		},
	)

	// WalRingBufferFullTotal counts times buffer was full
	WalRingBufferFullTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_full_total",
			Help: "Total number of times ring buffer was full (backpressure)",
		},
	)
)

// =============================================================================
// IO_Uring Metrics
// =============================================================================

var (
	// WalUringSubmissionQueueDepth tracks the number of entries in the submission queue
	WalUringSubmissionQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_uring_sq_depth",
			Help: "Current depth of the io_uring submission queue",
		},
	)

	// WalUringCompletionQueueDepth tracks the number of entries in the completion queue
	WalUringCompletionQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_uring_cq_depth",
			Help: "Current depth of the io_uring completion queue",
		},
	)

	// WalUringSubmitLatencySeconds measures the latency of io_uring submission calls
	WalUringSubmitLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_uring_submit_latency_seconds",
			Help:    "Latency of io_uring Enter/Submit calls",
			Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01}, // Microsecond resolution
		},
	)
)

// =============================================================================
// Index & Vector Metrics
// =============================================================================

var (
	// VectorIndexSize tracks the number of vectors in the index
	VectorIndexSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_vector_index_size",
			Help: "Current number of vectors in the index",
		},
	)

	// AverageVectorNorm tracks the average L2 norm of stored vectors
	AverageVectorNorm = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_average_vector_norm",
			Help: "Average L2 norm of vectors in the index",
		},
	)

	// IndexBuildLatency measures the time taken to rebuild or update the index
	IndexBuildLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_index_build_latency_seconds",
			Help:    "Latency of vector index build operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// IndexQueueDepth - Current length of indexing channel
	IndexQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_index_queue_depth",
			Help: "Current depth of the indexing queue (lag indicator)",
		},
	)

	// IndexJobLatencySeconds - Time from job creation to completion
	IndexJobLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_index_job_latency_seconds",
			Help:    "Latency of index job processing by dataset",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"dataset"},
	)

	// IndexJobsDroppedTotal
	IndexJobsDroppedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_index_jobs_dropped_total",
			Help: "Total number of index jobs dropped due to queue overflow",
		},
	)

	// IndexMigrationDuration measures the time taken to migrate index from HNSW to Sharded
	IndexMigrationDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_index_migration_duration_seconds",
			Help:    "Duration of index migration operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// IndexTypesRegistered = promauto.NewGauge
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

	// VectorScratchPoolMissesTotal - Pool allocation misses
	VectorScratchPoolMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_scratch_pool_misses_total",
			Help: "Count of scratch buffer pool misses requiring allocation",
		},
	)

	// InvertedIndexPostingsTotal tracks total postings in inverted indexes
	InvertedIndexPostingsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_inverted_index_postings_total",
			Help: "Total number of postings in inverted indexes",
		},
	)

	// VectorSearchGPULatencySeconds measures the latency of GPU search operations
	VectorSearchGPULatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_vector_search_gpu_latency_seconds",
			Help:    "Latency of GPU vector search operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"}, // "search", "add"
	)

	// VectorSearchGPUOperationsTotal counts GPU operations
	VectorSearchGPUOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_search_gpu_operations_total",
			Help: "Total number of GPU vector search operations",
		},
		[]string{"operation", "status"},
	)

	// ResultPoolHitsTotal
	ResultPoolHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_result_pool_hits_total",
			Help: "Total number of result object pool hits",
		},
		[]string{"k_size"},
	)

	// ResultPoolMissesTotal
	ResultPoolMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_result_pool_misses_total",
			Help: "Total number of result object pool misses",
		},
		[]string{"k_size"},
	)
)

// =============================================================================
// HNSW & Graph Metrics
// =============================================================================

var (
	// HnswActiveReaders tracks the number of active zero-copy readers per dataset
	HnswActiveReaders = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_active_readers",
			Help: "Number of active zero-copy readers per dataset",
		},
		[]string{"dataset"},
	)

	// HnswGraphHeight - Max layer of the HNSW graph
	HnswGraphHeight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_graph_height",
			Help: "Maximum layer height of the HNSW graph (search complexity)",
		},
		[]string{"dataset"},
	)

	// HnswNodeCount - Total nodes in the HNSW graph
	HnswNodeCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_node_count",
			Help: "Total number of nodes in the HNSW graph",
		},
		[]string{"dataset"},
	)

	HnswShardingMigrationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_sharding_migrations_total",
			Help: "Total number of HNSW index migrations to sharded format",
		},
	)

	HnswNodesVisited = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_nodes_visited",
			Help:    "Number of HNSW nodes visited per search",
			Buckets: []float64{10, 25, 50, 100, 200, 500, 1000, 2500, 5000},
		},
		[]string{"dataset"},
	)

	HnswDistanceCalculations = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_distance_calculations_total",
			Help: "Total HNSW distance calculations performed",
		},
	)

	HNSWPQEnabled = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_pq_enabled",
			Help: "Whether Product Quantization is enabled (1) or disabled (0) for the dataset",
		},
		[]string{"dataset"},
	)

	HNSWPQTrainingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_pq_training_duration_seconds",
			Help:    "Time taken to train PQ encoder for a dataset",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300},
		},
		[]string{"dataset"},
	)

	HNSWPQTrainingTriggered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_pq_training_triggered_total",
			Help: "Total number of auto-triggered PQ training events",
		},
		[]string{"dataset"},
	)

	HNSWPQCompressedBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_pq_compressed_bytes_total",
			Help: "Total number of bytes stored in PQ compressed format",
		},
		[]string{"dataset"},
	)

	ShardedHnswShardSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_sharded_hnsw_shard_size",
			Help: "Number of vectors in each HNSW shard",
		},
		[]string{"dataset", "shard"},
	)

	// ShardedHnswShardSplitCount counts shard split events
	ShardedHnswShardSplitCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_sharded_hnsw_shard_split_total",
			Help: "Total number of HNSW shard split events",
		},
		[]string{"dataset"},
	)

	ShardedHnswLoadFactor = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_sharded_hnsw_load_factor",
			Help: "Sharded HNSW load factor by shard (0-1)",
		},
		[]string{"dataset", "shard"},
	)

	// Graph Traversal Metrics
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

	// HNSW Graph Sync
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

	HnswEpochTransitions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_epoch_transitions_total",
			Help: "Total HNSW epoch transitions for zero-copy access",
		},
	)

	HnswParallelSearchSplits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_parallel_search_splits_total",
			Help: "Total number of parallel search splits",
		},
		[]string{"dataset"},
	)

	// Adaptive Chunk Sizing Metrics
	HnswAdaptiveChunkSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_adaptive_chunk_size",
			Help:    "Chunk sizes used in adaptive parallel search",
			Buckets: []float64{10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		},
		[]string{"dataset", "method"}, // method: "parallel", "serial"
	)

	HnswParallelSearchWorkerCount = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_parallel_search_worker_count",
			Help:    "Number of workers used in parallel search",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		},
		[]string{"dataset"},
	)

	HnswParallelSearchEfficiency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_parallel_search_efficiency",
			Help:    "Efficiency ratio (work per worker) in parallel search",
			Buckets: []float64{0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 10.0},
		},
		[]string{"dataset"},
	)

	HnswSerialFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_serial_fallback_total",
			Help: "Total number of serial fallback decisions",
		},
		[]string{"dataset", "reason"}, // reason: "small_set", "disabled", "efficiency"
	)
)

// =============================================================================
// Arena & Memory Metrics
// =============================================================================

var (
	ArenaAllocBytesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_alloc_bytes_total",
			Help: "Total bytes allocated from search arenas",
		},
	)
	ArenaOverflowTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_overflow_total",
			Help: "Total arena capacity overflow events requiring heap fallback",
		},
	)
	ArenaResetsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_resets_total",
			Help: "Total arena reset operations",
		},
	)
	ArenaPoolGets = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_pool_gets_total",
			Help: "Total arena acquisitions from global pool",
		},
	)
	ArenaPoolPuts = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_arena_pool_puts_total",
			Help: "Total arena returns to global pool",
		},
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

	// MemoryFragmentationRatio tracks the ratio of allocated to used memory
	MemoryFragmentationRatio = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_fragmentation_ratio",
			Help: "Ratio of system memory reserved vs used (fragmentation indicator)",
		},
	)
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

// =============================================================================
// Dataset & Filter Metrics
// =============================================================================

var (
	// DatasetRecordBatchesCount - Number of batches per dataset (fragmentation)
	DatasetRecordBatchesCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_dataset_record_batches_count",
			Help: "Number of record batches per dataset (high = fragmentation)",
		},
		[]string{"dataset"},
	)

	EvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_evictions_total",
			Help: "Total number of evicted records due to memory limits",
		},
		[]string{"reason"},
	)

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
	TombstonesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_tombstones_total",
			Help: "Total number of active tombstones",
		},
		[]string{"dataset"},
	)

	// FilterExecutionDurationSeconds - Time spent applying filters
	FilterExecutionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "longbow",
			Name:      "filter_execution_duration_seconds",
			Help:      "Duration of filter execution by dataset",
			Buckets:   []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
		},
		[]string{"dataset"},
	)

	// FilterSelectivityRatio - Ratio of rows output / rows input
	FilterSelectivityRatio = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_filter_selectivity_ratio",
			Help:    "Filter selectivity ratio (output rows / input rows)",
			Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0},
		},
		[]string{"dataset"},
	)

	FastPathUsageTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_fast_path_usage_total",
			Help: "Filter fast path usage count (fast/fallback)",
		},
		[]string{"path"},
	)

	BloomLookupsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_bloom_lookups_total",
			Help: "Total bloom filter lookups by result (hit/miss)",
		},
		[]string{"result"},
	)
	BloomFalsePositiveRate = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_bloom_false_positive_rate",
			Help: "Observed bloom filter false positive rate",
		},
	)

	ColumnIndexSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_column_index_size",
			Help: "Column inverted index size by dataset and column",
		},
		[]string{"dataset", "column"},
	)
	ColumnIndexLookupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_column_index_lookup_duration_seconds",
			Help:    "Column index lookup duration by dataset",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"dataset"},
	)
)
