package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Storage & WAL Metrics
// =============================================================================

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
