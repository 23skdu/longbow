package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	GPUComputeDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_compute_duration_seconds",
			Help:    "Time spent in GPU compute operations",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
		[]string{"backend", "operation"},
	)

	// WAL Metrics
	WALQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_queue_depth",
			Help: "Current number of batches waiting in the WAL persistence queue",
		},
	)

	WALQueueLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_queue_latency_seconds",
			Help:    "Time spent in the persistence queue before processing",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
	)

	WALReplicationLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_replication_latency_seconds",
			Help:    "Latency of synchronous WAL replication across the cluster",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
		},
	)

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

	// PackedAdjacencyCoWTotal counts total number of PackedAdjacency Copy-on-Write updates
	PackedAdjacencyCoWTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_packed_adjacency_cow_total",
			Help: "Total number of PackedAdjacency Copy-on-Write updates",
		},
	)

	// GPU Ingestion Metrics
	GPUIngestKernelDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_gpu_ingest_kernel_duration_seconds",
			Help:    "Duration of GPU-accelerated ingestion kernels",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
	)

	GPUNeighborPruneOpsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_gpu_neighbor_prune_ops_total",
			Help: "Total number of GPU neighbor prune operations",
		},
	)

	// TurboQuant Metrics
	TurboQuantDequantizeLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_search_dequantize_latency_seconds",
			Help:    "Latency of TurboQuant dequantization during search",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1},
		},
	)

	// Temporal Tree Metrics
	//
	// longbow_temporal_tree_cache_hit_ratio used to be declared here. It was
	// registered, graphed on the memory-performance dashboard, and never set by
	// anything, so that panel reported a flat 0 and read as "the cache never
	// hits". It is gone rather than wired: the real result cache is covered by the
	// counters below, and a precomputed ratio gauge duplicates what PromQL derives
	// from two counters while going stale the moment either is missed.

	// TemporalResultCache counters (roadmap R11a). These were maintained on the
	// cache and read by nothing, so "the temporal cache is thrashing" had to be
	// inferred from latency. Expiries and capacity evictions are separate series
	// because they need opposite remedies: a shorter TTL versus a larger cache.
	TemporalCacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_temporal_cache_hits_total",
			Help: "Temporal as-of search results served from cache",
		},
		[]string{"dataset"},
	)
	TemporalCacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_temporal_cache_misses_total",
			Help: "Temporal as-of search results not served from cache, by cause",
		},
		[]string{"dataset"},
	)
	TemporalCacheExpiriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_temporal_cache_expiries_total",
			Help: "Temporal cache entries dropped because their TTL had passed. A high rate means the TTL is shorter than the reuse interval.",
		},
		[]string{"dataset"},
	)
	TemporalCacheEvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_temporal_cache_evictions_total",
			Help: "Temporal cache entries dropped because the LRU was full. A high rate means the cache is too small.",
		},
		[]string{"dataset"},
	)
	TemporalCacheEntries = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_temporal_cache_entries",
			Help: "Temporal cache entries currently resident",
		},
		[]string{"dataset"},
	)

	TemporalQueryScannedNodesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_temporal_query_scanned_nodes_total",
			Help: "Total number of tree nodes scanned during temporal queries",
		},
	)
	TemporalTreeNodesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_temporal_tree_nodes_total",
			Help: "Total number of unique timestamps in the temporal tree",
		},
	)
	TemporalTreeAllocatedBytesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_temporal_tree_allocated_bytes_total",
			Help: "Total bytes allocated in the temporal tree arenas",
		},
	)

	SlabHugePageCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_slab_hugepage_count",
			Help: "Total slab allocations successfully backed by transparent hugepages via madvise(MADV_HUGEPAGE)",
		},
	)

	WalReplayParallelism = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_replay_parallelism_total",
			Help: "Current number of parallel WAL replay applier workers",
		},
	)

	// IndexingPausedDurationSeconds tracks total time background indexing is paused
	IndexingPausedDurationSeconds = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_indexing_paused_duration_seconds",
			Help: "Total time background graph maintenance is delayed due to ingestion bursts",
		},
	)

	// =============================================================================

	// Simd Tiled Metrics
	SimdTiledDistanceBatchTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_simd_tiled_distance_batch_total",
			Help: "Total number of tiled distance batch operations performed for high-dim vectors (>1024 dims)",
		},
	)

	// DoPut Adaptive Batching Alignment
	DoPutBatchSizeBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doput_batch_size_bytes",
			Help:    "Payload size of each flushed DoPut batch",
			Buckets: []float64{1024, 65536, 1048576, 4194304, 8388608, 10485760, 16777216},
		},
	)

	// DoPutBatchSizeVectors tracks number of vectors per batch
	DoPutBatchSizeVectors = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doput_batch_size_vectors",
			Help:    "Number of vectors per DoPut batch",
			Buckets: []float64{100, 1000, 5000, 10000, 25000, 50000, 100000},
		},
	)

	// DoPutBatchLatencySeconds tracks batch processing latency
	DoPutBatchLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_doput_batch_latency_seconds",
			Help:    "Latency of DoPut batch processing",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
	)

	// Vector Access Metrics - Zero-Copy Optimization
	VectorAccessZeroCopyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_access_zerocopy_total",
			Help: "Total number of zero-copy vector accesses",
		},
		[]string{"dataset", "index_type"},
	)

	VectorAccessCopyTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_access_copy_total",
			Help: "Total number of vector accesses requiring copy",
		},
		[]string{"dataset", "index_type"},
	)

	VectorAccessBytesAllocated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_access_bytes_allocated_total",
			Help: "Total bytes allocated for vector copies",
		},
		[]string{"dataset", "index_type"},
	)

	// BloomFilter metrics for filter evaluation optimization
	BloomFilterEarlyExitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bloom_filter_early_exits_total",
			Help: "Total number of times Bloom filter optimization caused early exit (all rows rejected)",
		},
	)

	BloomFilterSelectivityHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_bloom_filter_selectivity",
			Help:    "Distribution of estimated filter selectivity (0-1, where 1 means all rows match)",
			Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.2, 0.5, 0.8, 1.0},
		},
		[]string{"filter_type"}, // "int64", "float32", "float64", "string"
	)

	BloomFilterBitmapZeroChecksTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bloom_filter_bitmap_zero_checks_total",
			Help: "Total number of bitmap zero checks performed during filter evaluation",
		},
	)

	// StringFilter metrics for SIMD-accelerated string filter operations
	StringFilterOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_string_filter_ops_total",
			Help: "Total number of string filter operations",
		},
		[]string{"operator", "path"}, // operator: "eq", "neq", "gt", etc.; path: "fast", "slow"
	)

	StringFilterDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_string_filter_duration_seconds",
			Help:    "Duration of string filter operations",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
		},
		[]string{"operator", "path"},
	)

	StringFilterEqualLengthTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_string_filter_equal_length_total",
			Help: "Total number of string filters using equal-length fast path",
		},
	)

	StringFilterComparisonsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_string_filter_comparisons_total",
			Help: "Total number of string comparisons performed",
		},
	)

	StringFilterBytesComparedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_string_filter_bytes_compared_total",
			Help: "Total number of bytes compared during string filtering",
		},
	)

	// Connection Pool metrics for distributed query optimization
	ConnectionPoolGetTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_connection_pool_get_total",
			Help: "Total number of connection pool get operations",
		},
		[]string{"result"}, // "hit", "miss", "stale", "error"
	)

	ConnectionPoolCreateTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_connection_pool_create_total",
			Help: "Total number of new connections created",
		},
	)

	ConnectionPoolCloseTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_connection_pool_close_total",
			Help: "Total number of connections closed",
		},
	)

	ConnectionPoolActiveConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_connection_pool_active_connections",
			Help: "Current number of active connections in the pool",
		},
		[]string{"target"}, // target address
	)

	ConnectionPoolGetDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_connection_pool_get_duration_seconds",
			Help:    "Duration of connection pool get operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"result"}, // "hit", "miss", "stale", "error"
	)

	ConnectionPoolHealthCheckTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_connection_pool_health_check_total",
			Help: "Total number of connection health checks",
		},
		[]string{"result"}, // "healthy", "unhealthy", "error"
	)

	ConnectionPoolRefreshTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_connection_pool_refresh_total",
			Help: "Total number of connection refreshes due to health check failure",
		},
	)

	// Branch Prediction metrics for HNSW graph traversal optimization
	HnswBranchPredictionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_branch_prediction_total",
			Help: "Total number of branch predictions by type",
		},
		[]string{"branch_type"}, // "filter_match", "filter_miss", "location_found", "location_miss", "result_append"
	)

	HnswBranchPredictionLikelyTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_branch_prediction_likely_total",
			Help: "Total number of branches marked as likely (true)",
		},
	)

	HnswBranchPredictionUnlikelyTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_branch_prediction_unlikely_total",
			Help: "Total number of branches marked as unlikely (false)",
		},
	)

	HnswTraversalIterationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_traversal_iterations_total",
			Help: "Total number of iterations during HNSW graph traversal",
		},
	)

	HnswContextCheckTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_context_check_total",
			Help: "Total number of context checks performed during traversal",
		},
	)

	HnswContextCheckCancelledTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_context_check_cancelled_total",
			Help: "Total number of times context check detected cancellation",
		},
	)

	// Batch Distance Compute metrics for SIMD batch optimization
	BatchDistanceComputeTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_batch_distance_compute_total",
			Help: "Total number of batch distance compute operations",
		},
	)

	BatchDistanceComputeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_batch_distance_compute_duration_seconds",
			Help:    "Duration of batch distance compute operations",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
		},
	)

	BatchDistanceComputePairsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_batch_distance_compute_pairs_total",
			Help: "Total number of query-candidate pairs processed in batch",
		},
	)

	BatchDistanceComputeSIMDUsed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_batch_distance_compute_simd_used_total",
			Help: "Total number of batch operations using SIMD optimization",
		},
	)

	BatchDistanceComputeFallbackTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_batch_distance_compute_fallback_total",
			Help: "Total number of batch operations falling back to scalar",
		},
	)

	// ArrowMemoryUsedBytes tracks memory used by Arrow allocators
	ArrowMemoryUsedBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_arrow_memory_used_bytes",
			Help: "Current bytes used by Arrow memory allocators",
		},
		[]string{"allocator"},
	)

	// SimdDispatchTotal counts SIMD dispatch calls by implementation type
	SimdDispatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_dispatch_total",
			Help: "Total number of SIMD dispatch calls by implementation",
		},
		[]string{"implementation", "operation"},
	)
)
