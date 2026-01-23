package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Memory Subsystem Metrics
// =============================================================================

var (
	// ArenaMemoryBytes tracks current bytes allocated in arena pools by size
	ArenaMemoryBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_arena_memory_bytes",
			Help: "Current bytes allocated in arena pools by size",
		},
		[]string{"size"}, // "4MB", "8MB", "16MB", "32MB"
	)

	// SlabFragmentationRatio tracks fragmentation ratio for slab pools (pooled/active)
	SlabFragmentationRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_slab_fragmentation_ratio",
			Help: "Fragmentation ratio for slab pools (pooled/active)",
		},
		[]string{"size"},
	)

	// CompactionEventsTotal counts total number of compaction events by type
	CompactionEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_compaction_events_total",
			Help: "Total number of compaction events by type",
		},
		[]string{"dataset", "type"}, // type: "auto", "manual", "vacuum"
	)

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

	// Vector casting metrics
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

	// Memory allocator metrics
	AllocatorBytesAllocatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_allocator_bytes_allocated_total",
			Help: "Total bytes allocated by the memory allocator",
		},
	)

	AllocatorAllocationsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_allocator_allocations_active",
			Help: "Current number of active memory allocations",
		},
	)

	AllocatorBytesFreedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_allocator_bytes_freed_total",
			Help: "Total bytes freed by the memory allocator",
		},
	)
)

// =============================================================================
// IPC Buffer Pool Metrics
// =============================================================================

var (
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

	IpcBufferPoolEvictions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_ipc_buffer_pool_evictions_total",
			Help: "Total number of IPC buffer pool evictions",
		},
	)

	IpcBufferPoolSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_ipc_buffer_pool_size",
			Help: "Current size of the IPC buffer pool",
		},
	)
)

// =============================================================================
// Slab Pool Metrics
// =============================================================================

var (
	SlabPoolAllocationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_slab_pool_allocations_total",
			Help: "Total number of slab allocations (both pooled and new)",
		},
		[]string{"size", "result"}, // result: "hit", "miss"
	)
)
