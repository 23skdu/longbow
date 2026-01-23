package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Vector pool metrics
	VectorPoolHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_hits_total",
		Help: "Total number of vector pool hits (reused vectors)",
	})

	VectorPoolMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_misses_total",
		Help: "Total number of vector pool misses (new allocations)",
	})

	VectorPoolPuts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_vector_pool_puts_total",
		Help: "Total number of vectors returned to pool",
	})

	// HNSW allocation metrics
	HNSWVectorAllocations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_vector_allocations_total",
		Help: "Total number of vector allocations for HNSW graph storage",
	})

	HNSWVectorAllocatedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_vector_allocated_bytes_total",
		Help: "Total bytes allocated for HNSW vector storage",
	})

	HNSWGraphNodeAllocations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_graph_node_allocations_total",
		Help: "Total number of HNSW graph node allocations",
	})

	// Index growth metrics
	HNSWIndexGrowthDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_index_growth_duration_seconds",
		Help:    "Time spent growing the HNSW index capacity",
		Buckets: prometheus.DefBuckets,
	})

	// HNSW Repair Agent Metrics
	HNSWRepairOrphansDetected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_repair_orphans_detected_total",
		Help: "Total number of orphaned nodes detected by repair agent",
	}, []string{"dataset"})

	HNSWRepairOrphansRepaired = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_repair_orphans_repaired_total",
		Help: "Total number of orphaned nodes repaired by repair agent",
	}, []string{"dataset"})

	HNSWRepairScanDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_repair_scan_duration_seconds",
		Help:    "Time spent scanning for orphaned nodes",
		Buckets: []float64{0.001, 0.01, 0.1, 1, 10, 60, 300},
	}, []string{"dataset"})

	HNSWRepairLastScanTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_hnsw_repair_last_scan_timestamp_seconds",
		Help: "Unix timestamp of last repair scan",
	}, []string{"dataset"})

	HNSWInsertDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_insert_duration_seconds",
			Help:    "Duration of HNSW vector insertion",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
	)

	HNSWBulkInsertDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bulk_insert_duration_seconds",
			Help:    "Duration of HNSW bulk vector insertion",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30},
		},
	)

	// HNSWInsertLatencyByType measures insert latency per vector data type
	HNSWInsertLatencyByType = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_insert_latency_by_type_seconds",
			Help:    "Latency of HNSW insert operations bucketed by vector type",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"type"},
	)

	// HNSWInsertLatencyByDim measures insert latency per vector dimension
	HNSWInsertLatencyByDim = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_insert_latency_by_dim_seconds",
			Help:    "Latency of HNSW insert operations bucketed by dimension",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"dim"},
	)

	// HNSWBulkInsertLatencyByType measures bulk insert latency per vector data type
	HNSWBulkInsertLatencyByType = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bulk_insert_latency_by_type_seconds",
			Help:    "Latency of HNSW bulk insert operations bucketed by vector type",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"type"},
	)

	// HNSWBulkInsertLatencyByDim measures bulk insert latency per vector dimension
	HNSWBulkInsertLatencyByDim = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bulk_insert_latency_by_dim_seconds",
			Help:    "Latency of HNSW bulk insert operations bucketed by dimension",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"dim"},
	)

	HNSWBulkVectorsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_bulk_vectors_processed_total",
			Help: "Total number of vectors processed in bulk operations",
		},
	)

	// HNSWSearchLatencyByType measures search latency per vector data type
	HNSWSearchLatencyByType = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_search_latency_by_type_seconds",
			Help:    "Latency of HNSW search operations bucketed by vector type",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"type"},
	)

	// HNSWSearchLatencyByDim measures search latency per vector dimension
	HNSWSearchLatencyByDim = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_search_latency_by_dim_seconds",
			Help:    "Latency of HNSW search operations bucketed by dimension",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"dim"},
	)

	// HNSWRefineThroughput counts vectors refined per type
	HNSWRefineThroughput = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_refine_throughput_total",
			Help: "Total number of vectors refined during search",
		},
		[]string{"type"},
	)

	HNSWNodesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_nodes_total",
			Help: "Total number of nodes in the HNSW graph",
		},
		[]string{"dataset"},
	)

	HNSWResizesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_resizes_total",
			Help: "Total number of HNSW graph resizes",
		},
		[]string{"dataset"},
	)

	HNSWSearchPoolGetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_get_total",
			Help: "Total number of search contexts retrieved from pool",
		},
	)

	HNSWSearchPoolNewTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_new_total",
			Help: "Total number of new search contexts created",
		},
	)

	HNSWSearchPoolPutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_pool_put_total",
			Help: "Total number of search contexts returned to pool",
		},
	)

	HNSWInsertPoolGetTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_get_total",
			Help: "Total number of insert contexts retrieved from pool",
		},
	)

	HNSWInsertPoolNewTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_new_total",
			Help: "Total number of new insert contexts created",
		},
	)

	HNSWInsertPoolPutTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_insert_pool_put_total",
			Help: "Total number of insert contexts returned to pool",
		},
	)

	HNSWBitsetGrowTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_bitset_grow_total",
			Help: "Total number of bitset grows during HNSW operations",
		},
	)

	HNSWDistanceCalculationsF16Total = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_distance_calculations_f16_total",
			Help: "Total number of F16 distance calculations",
		},
	)

	// HNSWBitmapIndexEntriesTotal tracks number of entries in metadata bitmap index
	HNSWBitmapIndexEntriesTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_bitmap_index_entries_total",
			Help: "Number of entries in the HNSW bitmap metadata index",
		},
		[]string{"dataset"},
	)

	// HNSWBitmapFilterDurationSeconds measures time to evaluate bitset filters
	HNSWBitmapFilterDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_bitmap_filter_duration_seconds",
			Help:    "Time spent evaluating bitmap filters during HNSW search",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"dataset"},
	)

	// HNSWSearchEarlyTerminationsTotal counts number of searches that terminated early
	HNSWSearchEarlyTerminationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_early_terminations_total",
			Help: "Total number of HNSW searches that terminated early due to optimization",
		},
		[]string{"reason"},
	)

	// HNSWSearchPhaseDurationSeconds measures time spent in different phases of HNSW search
	HNSWSearchPhaseDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hnsw_search_phase_duration_seconds",
			Help:    "Duration of HNSW search phases",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"phase"},
	)

	// HNSWDisconnectedComponents tracks the number of disconnected components in the HNSW graph
	HNSWDisconnectedComponents = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_disconnected_components",
			Help: "Number of disconnected components in the HNSW graph",
		},
		[]string{"dataset"},
	)

	// HNSWOrphanNodes tracks the number of nodes with zero degree in the HNSW graph
	HNSWOrphanNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_orphan_nodes",
			Help: "Number of orphan nodes (degree 0) in the HNSW graph",
		},
		[]string{"dataset"},
	)

	// HNSWAverageDegree tracks the average degree of nodes in the HNSW graph
	HNSWAverageDegree = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_average_degree",
			Help: "Average degree of nodes in the HNSW graph",
		},
		[]string{"dataset", "level"},
	)

	// HNSWMaxComponentSize tracks the size of the largest connected component
	HNSWMaxComponentSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_max_component_size",
			Help: "Size of the largest connected component in the HNSW graph",
		},
		[]string{"dataset"},
	)

	// HNSWEstimatedDiameter tracks the estimated diameter of the HNSW graph
	HNSWEstimatedDiameter = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_estimated_diameter",
			Help: "Estimated diameter (max BFS depth) of the HNSW graph",
		},
		[]string{"dataset"},
	)

	// HNSWAvgLevelDistribution tracks the distribution of nodes across HNSW levels
	HNSWAvgLevelDistribution = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_avg_level_distribution",
			Help: "Average number of nodes at each HNSW level",
		},
		[]string{"dataset", "level"},
	)

	// HNSWIngestionThroughputVectorsPerSecond measures ingestion rate
	HNSWIngestionThroughputVectorsPerSecond = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_ingestion_throughput_vectors_per_second",
			Help: "Rate of vector ingestion in vectors per second",
		},
		[]string{"dataset", "type"},
	)

	// HNSWMemoryUsageBytes tracks memory consumption of HNSW structures
	HNSWMemoryUsageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hnsw_memory_usage_bytes",
			Help: "Memory usage of HNSW index components",
		},
		[]string{"dataset", "component"},
	)

	HNSWSearchScratchSpaceResizesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_scratch_space_resizes_total",
			Help: "Total number of scratch space resizes during HNSW search",
		},
		[]string{"component"},
	)

	// HNSWVisitedResetDuration tracks the time spent resetting the visited bitset/list
	HNSWVisitedResetDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_visited_reset_duration_seconds",
		Help:    "Time spent resetting HNSW visited set",
		Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
	})

	// Additional HNSW metrics
	HnswSearchThroughputDims = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hnsw_search_throughput_dims_total",
			Help: "Total number of HNSW searches bucketed by dimension",
		},
		[]string{"dims"},
	)

	IndexBuildDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_index_build_duration_seconds",
			Help:    "Duration of index building operations",
			Buckets: []float64{0.1, 1, 10, 60, 300, 900},
		},
		[]string{"dataset"},
	)
)
