package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	// GPUSearchDurationSeconds measures GPU search operation duration
	GPUSearchDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_search_duration_seconds",
			Help:    "Duration of GPU search operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"backend"},
	)

	// GPUMemoryBytes tracks GPU memory usage
	GPUMemoryBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_memory_bytes",
			Help: "GPU memory usage in bytes",
		},
		[]string{"device", "type"}, // type: "total", "free", "used"
	)

	// GPUSyncDurationSeconds measures GPU synchronization duration
	GPUSyncDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_sync_duration_seconds",
			Help:    "Duration of GPU synchronization operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// GPUFallbackTotal counts GPU to CPU fallback events
	GPUFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gpu_fallback_total",
			Help: "Total number of GPU to CPU fallback events",
		},
		[]string{"reason"},
	)

	// GPUIndexSize tracks the number of vectors in GPU index
	GPUIndexSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_index_size",
			Help: "Number of vectors stored in GPU index",
		},
		[]string{"device"},
	)

	// GPUOperationsTotal counts all GPU operations (sync, search, etc.)
	GPUOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gpu_operations_total",
			Help: "Total number of GPU operations",
		},
		[]string{"operation", "type"}, // operation: "sync", "search"; type: "batch", "single", "error"
	)

	// GPUBatchSize tracks the current size of pending GPU batches
	GPUBatchSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_batch_size",
			Help: "Current number of vectors pending in GPU batch",
		},
	)

	// GPUDeviceUtilization tracks GPU device utilization percentage
	GPUDeviceUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_device_utilization_percent",
			Help: "GPU device utilization percentage (0-100)",
		},
		[]string{"device"},
	)

	// GPUDeviceTemperature tracks GPU device temperature in Celsius
	GPUDeviceTemperature = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_device_temperature_celsius",
			Help: "GPU device temperature in Celsius",
		},
		[]string{"device"},
	)

	// GPUDevicePowerUsage tracks GPU power consumption in Watts
	GPUDevicePowerUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_device_power_watts",
			Help: "GPU device power consumption in Watts",
		},
		[]string{"device"},
	)

	// GPUIndexPoolIdle tracks idle indexes in the GPU pool
	GPUIndexPoolIdle = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_index_pool_idle",
			Help: "Number of idle GPU indexes in the pool",
		},
	)

	// GPUIndexPoolActive tracks active (checked out) indexes
	GPUIndexPoolActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_index_pool_active",
			Help: "Number of active (checked out) GPU indexes",
		},
	)

	// GPUIndexPoolTotalCreated tracks total indexes created
	GPUIndexPoolTotalCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_gpu_index_pool_created_total",
			Help: "Total number of GPU indexes created",
		},
	)

	// GPUIndexPoolTotalReused tracks total times indexes were reused
	GPUIndexPoolTotalReused = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_gpu_index_pool_reused_total",
			Help: "Total number of times GPU indexes were reused from pool",
		},
	)

	// GPUUsed indicates whether GPU (Metal/CUDA) was actually used per search operation
	// Labels: backend (metal/cuda), type (vector type: f32/f16/c64/c128/f64/int)
	GPUUsed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gpu_used_total",
			Help: "Total number of search operations that used GPU acceleration",
		},
		[]string{"backend", "type"},
	)

	// Metal-specific metrics for Apple Silicon GPU operations

	// MetalInitDurationSeconds measures Metal GPU initialization duration
	MetalInitDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_metal_init_duration_seconds",
			Help:    "Duration of Metal GPU initialization",
			Buckets: prometheus.DefBuckets,
		},
	)

	// MetalInitOperationsTotal counts Metal initialization operations
	MetalInitOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_metal_init_operations_total",
			Help: "Total number of Metal initialization operations",
		},
		[]string{"status"}, // "success", "error"
	)

	// MetalSearchDurationSeconds measures Metal search operation duration
	MetalSearchDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_metal_search_duration_seconds",
			Help:    "Duration of Metal search operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// MetalSearchOperationsTotal counts Metal search operations
	MetalSearchOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_metal_search_operations_total",
			Help: "Total number of Metal search operations",
		},
		[]string{"status"}, // "success", "error"
	)

	// MetalSearchVectorsProcessed tracks total vectors processed by Metal search
	MetalSearchVectorsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_metal_search_vectors_processed_total",
			Help: "Total number of vectors processed by Metal search operations",
		},
	)

	// MetalAddDurationSeconds measures Metal add operation duration
	MetalAddDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_metal_add_duration_seconds",
			Help:    "Duration of Metal add operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// MetalAddOperationsTotal counts Metal add operations
	MetalAddOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_metal_add_operations_total",
			Help: "Total number of Metal add operations",
		},
		[]string{"status"}, // "success", "error"
	)

	// MetalAddVectorsProcessed tracks total vectors added via Metal
	MetalAddVectorsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_metal_add_vectors_processed_total",
			Help: "Total number of vectors added via Metal operations",
		},
	)

	// MetalIndexVectors tracks number of vectors in Metal index
	MetalIndexVectors = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_metal_index_vectors",
			Help: "Number of vectors stored in Metal index",
		},
		[]string{"device"},
	)

	// MetalIndexDimensions tracks dimensions of Metal index
	MetalIndexDimensions = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_metal_index_dimensions",
			Help: "Number of dimensions in Metal index",
		},
		[]string{"device"},
	)

	// MetalMemoryBytes tracks Metal GPU memory usage
	MetalMemoryBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_metal_memory_bytes",
			Help: "Metal GPU memory usage in bytes",
		},
		[]string{"type"}, // "allocated", "used", "vectors"
	)

	// MetalShaderCompileDurationSeconds measures Metal shader compilation duration
	MetalShaderCompileDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_metal_shader_compile_duration_seconds",
			Help:    "Duration of Metal shader compilation",
			Buckets: prometheus.DefBuckets,
		},
	)

	// MetalShaderCompileTotal counts Metal shader compilation attempts
	MetalShaderCompileTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_metal_shader_compile_total",
			Help: "Total number of Metal shader compilation attempts",
		},
		[]string{"status"}, // "success", "error"
	)

	// MetalShaderKernelCount tracks number of kernels compiled
	MetalShaderKernelCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_metal_shader_kernel_count",
			Help: "Number of Metal shader kernels compiled",
		},
	)

	// Multi-GPU metrics

	MultiGPUQueryDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_multi_gpu_query_duration_seconds",
			Help:    "Duration of multi-GPU query operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"strategy"},
	)

	MultiGPUTotalDevices = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_multi_gpu_total_devices",
			Help: "Total number of GPU devices in multi-GPU setup",
		},
	)

	MultiGPUQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_multi_gpu_queries_total",
			Help: "Total number of multi-GPU queries",
		},
		[]string{"strategy", "status"},
	)

	MultiGPUFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_multi_gpu_fallback_total",
			Help: "Total number of multi-GPU fallback events",
		},
		[]string{"reason"},
	)

	MultiGPUReplicateDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_multi_gpu_replicate_duration_seconds",
			Help:    "Duration of multi-GPU replication operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	MultiGPUReplicateOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_multi_gpu_replicate_operations_total",
			Help: "Total number of multi-GPU replication operations",
		},
		[]string{"status"},
	)

	MultiGPUReplicateVectorsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_multi_gpu_replicate_vectors_processed_total",
			Help: "Total number of vectors replicated across GPUs",
		},
	)

	MultiGPUDeviceQueries = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_multi_gpu_device_queries",
			Help: "Number of queries processed by each GPU device",
		},
		[]string{"device"},
	)

	MultiGPUDeviceErrors = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_multi_gpu_device_errors",
			Help: "Number of errors on each GPU device",
		},
		[]string{"device"},
	)

	// GPU HNSW Build metrics

	GPUHNSWBuildDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_hnsw_build_duration_seconds",
			Help:    "Duration of GPU-accelerated HNSW index building",
			Buckets: prometheus.DefBuckets,
		},
	)

	GPUHNSWBuildOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gpu_hnsw_build_operations_total",
			Help: "Total number of GPU HNSW build operations",
		},
		[]string{"status"},
	)

	GPUHNSWBuildVectorsProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_gpu_hnsw_build_vectors_processed_total",
			Help: "Total number of vectors processed during GPU HNSW build",
		},
	)

	GPUHNSWBuildBatchDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_hnsw_build_batch_duration_seconds",
			Help:    "Duration of GPU HNSW build batch operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	GPUHNSWBuildBatchSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gpu_hnsw_build_batch_size",
			Help: "Current batch size for GPU HNSW build",
		},
	)

	GPUHNSWBuildFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_gpu_hnsw_build_fallback_total",
			Help: "Total number of GPU HNSW build fallback events",
		},
		[]string{"reason"},
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

	HNSWNodesSkippedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_nodes_skipped_total",
			Help: "Total number of HNSW nodes skipped due to early-exit filtering",
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
			Help: "Total number of Bloom filter lookups",
		},
		[]string{"result"},
	)

	BloomHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bloom_hits_total",
			Help: "Total number of Bloom filter hits (likely present)",
		},
	)

	BloomMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bloom_misses_total",
			Help: "Total number of Bloom filter misses (definitely absent)",
		},
	)

	BloomFalsePositivesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bloom_false_positives_total",
			Help: "Total number of Bloom filter false positives",
		},
	)

	// StringFilterOperationsTotal - Total string filter operations
	StringFilterOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_string_filter_operations_total",
			Help: "Total number of string filter operations by type",
		},
		[]string{"type"},
	)

	// NumericFilterOperationsTotal - Total numeric filter operations
	NumericFilterOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_numeric_filter_operations_total",
			Help: "Total number of numeric filter operations by type",
		},
		[]string{"type"},
	)

	// BooleanFilterOperationsTotal - Total boolean filter operations
	BooleanFilterOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_boolean_filter_operations_total",
			Help: "Total number of boolean filter operations by type",
		},
		[]string{"type"},
	)

	// RangeFilterOperationsTotal - Total range filter operations
	RangeFilterOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_range_filter_operations_total",
			Help: "Total number of range filter operations by type",
		},
		[]string{"type"},
	)

	// NullFilterOperationsTotal - Total null check operations
	NullFilterOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_null_filter_operations_total",
			Help: "Total number of null filter operations by type",
		},
		[]string{"type"},
	)

	// FilterOptimizationTotal - Total filter optimizations applied
	FilterOptimizationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_filter_optimization_total",
			Help: "Total number of filter optimizations applied",
		},
		[]string{"optimization"},
	)

	// FilterComplexityScore - Measure of filter complexity
	FilterComplexityScore = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_filter_complexity_score",
			Help:    "Complexity score of applied filters",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200},
		},
	)

	// FilterMemoryUsageBytes - Memory used by active filters
	FilterMemoryUsageBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_filter_memory_usage_bytes",
			Help: "Memory used by active filter state",
		},
	)
)

// =============================================================================
// Dimension Auto-Detection Metrics (Item 3)
// =============================================================================

var (
	// DatasetDimensionAutoDetectTotal counts auto-dimension-detection events.
	DatasetDimensionAutoDetectTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_dataset_dimension_auto_detect_total",
			Help: "Total number of dataset dimension auto-detection events",
		},
		[]string{"dataset", "result"}, // result: success|conflict
	)

	// DatasetDimensionMismatchTotal counts dimension mismatch validation failures.
	DatasetDimensionMismatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_dataset_dimension_mismatch_total",
			Help: "Total number of vector dimension mismatch errors",
		},
		[]string{"dataset"},
	)
)

// =============================================================================
// TurboQuant Native Storage Metrics (Item 5)
// =============================================================================

var (
	// DatasetVectorTypeTotal tracks the vector type distribution at dataset creation.
	DatasetVectorTypeTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_dataset_vector_type_total",
			Help: "Number of datasets by declared vector type",
		},
		[]string{"dataset", "vector_type"}, // vector_type: float32|turboquant|int8|binary
	)

	// TurboQuantEncodingTotal counts TurboQuant encoding events by direction.
	TurboQuantEncodingTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_turboquant_encoding_total",
			Help: "Total number of TurboQuant encoding operations",
		},
		[]string{"dataset", "direction"}, // direction: client_provided|server_encoded
	)

	// TurboQuantEncodingLatencySeconds tracks how long server-side TQ encoding takes.
	TurboQuantEncodingLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_turboquant_encoding_latency_seconds",
			Help:    "Latency of server-side TurboQuant encoding operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05},
		},
		[]string{"dataset"},
	)

	// TurboQuantStorageBytesTotal tracks cumulative storage bytes for TQ datasets.
	TurboQuantStorageBytesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_turboquant_storage_bytes_total",
			Help: "Total storage bytes used by TurboQuant-encoded vectors (vs float32 baseline)",
		},
		[]string{"dataset"},
	)
 
	// TurboQuantSearchTotal counts TurboQuant-accelerated search operations.
	TurboQuantSearchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_turboquant_search_total",
			Help: "Total number of searches performed using TurboQuant acceleration",
		},
		[]string{"dataset", "bit_width"}, // bit_width: "4", "2"
	)
 
	// TurboQuantSearchLatencySeconds tracks latency of TurboQuant-accelerated searches.
	TurboQuantSearchLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_turboquant_search_latency_seconds",
			Help:    "Latency of TurboQuant-accelerated search operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"dataset", "bit_width"},
	)
)
