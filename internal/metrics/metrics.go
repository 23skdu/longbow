package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Rate Limit Metrics
// =============================================================================

var (
	// CompactionRateLimitWaitSeconds measures the time spent waiting for the compaction rate limiter.
	CompactionRateLimitWaitSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_compaction_rate_limit_wait_seconds",
			Help:    "Time spent waiting for compaction rate limiter",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5},
		},
	)
	// SnapshotRateLimitWaitSeconds is a snapshot rate limit wait seconds value.
	SnapshotRateLimitWaitSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_snapshot_rate_limit_wait_seconds",
			Help:    "Time spent waiting for snapshot rate limiter",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5},
		},
	)

	// RateLimitRequestsTotal is a rate limit requests total value.
	RateLimitRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_rate_limit_requests_total",
			Help: "Total number of rate limited requests",
		},
		[]string{"result"}, // "allowed", "throttled"
	)

	// ValidationFailuresTotal is a validation failures total value.
	ValidationFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_validation_failures_total",
			Help: "Total number of validation failures",
		},
		[]string{"component", "reason"},
	)

	// PanicTotal is a panic total value.
	PanicTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_panic_total",
			Help: "Total number of panics recovered",
		},
		[]string{"component"},
	)

	// DoGetTimeToFirstChunk is a do get time to first chunk value.
	DoGetTimeToFirstChunk = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_do_get_time_to_first_chunk_seconds",
			Help:    "Time from DoGet start to receiving first chunk",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
	)
)

// =============================================================================
// Adaptive GC Metrics
// =============================================================================

var (
	// AdaptiveGCCurrentGOGC is the current GOGC value set by adaptive GC controller.
	AdaptiveGCCurrentGOGC = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_current_gogc",
		Help: "Current GOGC value set by adaptive GC controller",
	})

	// AdaptiveGCAdjustmentsTotal is a adaptive gc adjustments total value.
	AdaptiveGCAdjustmentsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_adaptive_gc_adjustments_total",
		Help: "Total number of GOGC adjustments made by adaptive controller",
	})

	// AdaptiveGCAllocationRate is a adaptive gc allocation rate value.
	AdaptiveGCAllocationRate = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_allocation_rate_bytes_per_sec",
		Help: "Current memory allocation rate in bytes per second",
	})

	// AdaptiveGCMemoryPressure is a adaptive gc memory pressure value.
	AdaptiveGCMemoryPressure = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_adaptive_gc_memory_pressure_ratio",
		Help: "Current memory pressure ratio (0-1, where 1 is maximum pressure)",
	})
)

// =============================================================================
// Fragmentation-Aware Compaction Metrics
// =============================================================================

var (
	// CompactionTombstoneDensity tracks the current tombstone density ratio per batch.
	CompactionTombstoneDensity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_compaction_tombstone_density_ratio",
		Help: "Current tombstone density per batch (0-1)",
	}, []string{"dataset", "batch"})

	// CompactionFragmentedBatches tracks the number of batches exceeding the fragmentation threshold.
	CompactionFragmentedBatches = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_compaction_fragmented_batches_total",
		Help: "Number of batches exceeding fragmentation threshold",
	}, []string{"dataset"})

	// CompactionTriggersTotal is a compaction triggers total value.
	CompactionTriggersTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_compaction_triggers_total",
		Help: "Total number of compaction triggers by reason",
	}, []string{"dataset", "reason"})

	// CompactionBatchesMerged is a compaction batches merged value.
	CompactionBatchesMerged = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_compaction_batches_merged_total",
		Help: "Total number of batches merged during compaction",
	}, []string{"dataset"})

	// CompactionRunsTotal is a compaction runs total value.
	CompactionRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_compaction_runs_total",
		Help: "Total number of compaction runs performed",
	}, []string{"dataset"})

	// WarmupProgressPercent tracks the current warmup progress percentage.
	WarmupProgressPercent = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_warmup_progress_percent",
			Help: "Current warmup progress percentage (0-100)",
		},
	)
	// WarmupDatasetsTotal is a warmup datasets total value.
	WarmupDatasetsTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_warmup_datasets_total",
			Help: "Total number of datasets to warmup",
		},
	)
	// WarmupDatasetsCompleted is a warmup datasets completed value.
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
	// GCTunerTargetGOGC is a gc tuner target gogc value.
	GCTunerTargetGOGC = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gc_tuner_target_gogc",
			Help: "Current target GOGC value set by the tuner",
		},
	)

	// GCTunerHeapUtilization is a gc tuner heap utilization value.
	GCTunerHeapUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gc_tuner_heap_utilization",
			Help: "Current heap utilization ratio (heap_inuse / limit)",
		},
	)

	// GCTunerGPUUtilization is a gc tuner gpu utilization value.
	GCTunerGPUUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_gc_tuner_gpu_utilization",
			Help: "Current GPU utilization percentage (0-100)",
		},
	)

	// HnswRepairSuccessTotal Stabilization Metrics
	HnswRepairSuccessTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_repair_success_total",
			Help: "Total number of successful HNSW graph repairs",
		},
	)
	// HnswRepairFailureTotal is a hnsw repair failure total value.
	HnswRepairFailureTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_repair_failure_total",
			Help: "Total number of failed HNSW graph repairs",
		},
	)
	// HnswRepairNodesVisitedTotal is a hnsw repair nodes visited total value.
	HnswRepairNodesVisitedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_repair_nodes_visited_total",
			Help: "Total number of nodes visited during repair operations",
		},
	)

	// HnswLockWaitDurationSeconds is a hnsw lock wait duration seconds value.
	HnswLockWaitDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_lock_wait_duration_seconds",
			Help:    "Time spent waiting for HNSW node locks",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
		},
	)

	// HnswSearchEarlyExitsTotal is a hnsw search early exits total value.
	HnswSearchEarlyExitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_early_exits_total",
			Help: "Total number of HNSW search early exits by reason",
		},
		[]string{"reason"}, // "timeout", "threshold", "no_candidates"
	)

	// HNSWEarlyTerminationTotal is a hnsw early termination total value.
	HNSWEarlyTerminationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_early_termination_total",
			Help: "Total number of HNSW searches that hit the visited nodes budget",
		},
		[]string{"reason"},
	)

	// NUMANodeCount Metrics
	NUMANodeCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_numa_node_count",
			Help: "Number of NUMA nodes detected",
		},
	)

	// NUMAEnabled is a numa enabled value.
	NUMAEnabled = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_numa_enabled",
			Help: "Whether NUMA awareness is enabled (1 = enabled, 0 = disabled)",
		},
	)

	// WALFlushErrors counts total number of WAL flush failures
	WALFlushErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_flush_errors_total",
			Help: "Total number of WAL flush failures",
		},
	)
) // =============================================================================
// Pipeline & S3 Metrics
// =============================================================================

var (
	// PipelineOperationsTotal tracks the total number of pipeline operations.
	PipelineOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_pipeline_operations_total",
			Help: "Total number of pipeline operations",
		},
		[]string{"stage", "status"},
	)

	// PipelineDurationSeconds is a pipeline duration seconds value.
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

	// S3OperationsTotal is a s3 operations total value.
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

// =============================================================================
// Namespace Metrics
// =============================================================================

var (
	// NamespaceDatasetsTotal tracks the total number of datasets in a namespace.
	NamespaceDatasetsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_datasets_total",
			Help: "Total number of datasets in a namespace",
		},
		[]string{"namespace"},
	)

	// NamespaceCreationTotal is a namespace creation total value.
	NamespaceCreationTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_namespace_creation_total",
			Help: "Total number of successful namespace creations",
		},
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

var (

	// SchemaEvolutionTotal tracks the total number of schema evolution operations.
	SchemaEvolutionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_schema_evolution_total",
			Help: "Total number of schema evolution operations",
		},
		[]string{"operation", "status"},
	)

	// VectorSentinelHitTotal counts number of times a sentinel zero-vector was returned
	VectorSentinelHitTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_vector_sentinel_hit_total",
			Help: "Total number of times a sentinel vector was used due to missing data",
		},
	)

	// FilterEvaluatorOpsTotal counts filter evaluator operations
	FilterEvaluatorOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_filter_evaluator_ops_total",
			Help: "Total number of filter evaluator operations",
		},
		[]string{"method"}, // "Matches", "MatchesBatch", "MatchesBatchFused", "MatchesAll"
	)

	// FilterEvaluatorDurationSeconds measures time for filter evaluator operations
	FilterEvaluatorDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_filter_evaluator_duration_seconds",
			Help:    "Duration of filter evaluator operations",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1},
		},
		[]string{"method"}, // "Matches", "MatchesBatch", "MatchesBatchFused", "MatchesAll"
	)

	// FilterEvaluatorAllocations tracks allocations during filter evaluation
	FilterEvaluatorAllocations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_filter_evaluator_allocations_total",
			Help: "Total number of allocations during filter evaluation",
		},
		[]string{"method", "type"}, // method: "MatchesBatch", "MatchesAll"; type: "bitmap", "indices", "intermediate"
	)

	// QueryCacheOpsTotal counts query cache operations (hit, miss, evict)
	QueryCacheOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_ops_total",
			Help: "Total number of query cache operations",
		},
		[]string{"dataset", "type"}, // "hit", "miss", "evict", "set"
	)

	// QueryCacheSize tracks the current number of entries in the query cache.
	QueryCacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_query_cache_size",
			Help: "Current number of entries in query cache",
		},
		[]string{"dataset"},
	)

	// QueryCacheHitsTotal is a query cache hits total value.
	QueryCacheHitsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_hits_total",
			Help: "Total number of query cache hits",
		},
		[]string{"dataset"},
	)

	// QueryCacheMissesTotal is a query cache misses total value.
	QueryCacheMissesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_misses_total",
			Help: "Total number of query cache misses",
		},
		[]string{"dataset"},
	)

	// DiskStoreReadBytesTotal is a disk store read bytes total value.
	DiskStoreReadBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_disk_store_read_bytes_total",
			Help: "Total bytes read from disk vector store",
		},
		[]string{"dataset"},
	)

	// DiskStoreWriteBytesTotal is a disk store write bytes total value.
	DiskStoreWriteBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_disk_store_write_bytes_total",
			Help: "Total bytes written to disk vector store",
		},
		[]string{"dataset"},
	)

	// AutoSpillToDiskEngaged is a auto spill to disk engaged value.
	AutoSpillToDiskEngaged = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_auto_spill_to_disk_engaged_total",
			Help: "Total times automatic spill to disk was triggered for high-scale or memory-constrained datasets",
		},
		[]string{"dataset"},
	)

	// AutoQuantizeEngaged is a auto quantize engaged value.
	AutoQuantizeEngaged = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_auto_quantize_engaged_total",
			Help: "Total times automatic TurboQuant standardization was triggered for high-scale datasets",
		},
		[]string{"dataset"},
	)

	// QueryCacheEvictionsTotal is a query cache evictions total value.
	QueryCacheEvictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_query_cache_evictions_total",
			Help: "Total number of query cache evictions",
		},
		[]string{"dataset"},
	)

	// BitmapCacheHitsTotal is a bitmap cache hits total value.
	BitmapCacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_cache_hits_total",
			Help: "Total number of filter bitset cache hits",
		},
	)

	// BitmapCacheMissesTotal is a bitmap cache misses total value.
	BitmapCacheMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bitmap_cache_misses_total",
			Help: "Total number of filter bitset cache misses",
		},
	)
)

// =============================================================================
// BQ & RCU Metrics
// =============================================================================
var (
	// BQVectorsTotal is a bq vectors total value.
	BQVectorsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_bq_vectors_total",
			Help: "Total number of vectors indexed with Binary Quantization",
		},
		[]string{"dataset"},
	)

	// DatasetUpdateRetriesTotal is a dataset update retries total value.
	DatasetUpdateRetriesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_dataset_update_retries_total",
			Help: "Total number of retries during lock-free dataset map updates (CAS failures)",
		},
	)
)
