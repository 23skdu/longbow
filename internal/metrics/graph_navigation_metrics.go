package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Graph Navigation Metrics
// =============================================================================

var (
	// GraphNavigationOperationsTotal tracks the total number of FindPath calls.
	GraphNavigationOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_navigation_operations_total",
			Help: "Total number of graph navigation operations (FindPath).",
		},
		[]string{"dataset", "strategy", "result"}, // result: success, fail, cancelled, timeout
	)

	// GraphNavigationLatencySeconds tracks the execution time of FindPath.
	GraphNavigationLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_navigation_latency_seconds",
			Help:    "Execution time of graph navigation operations.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
		[]string{"dataset", "strategy"},
	)

	// GraphNavigationHopsTotal tracks the path length of successful traversals.
	GraphNavigationHopsTotal = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_navigation_hops_total",
			Help:    "Number of hops in a successful graph navigation path.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		},
		[]string{"dataset", "strategy"},
	)

	// GraphNavigationNodesVisitedTotal tracks the exploration breadth/depth.
	GraphNavigationNodesVisitedTotal = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_navigation_nodes_visited_total",
			Help:    "Total number of unique nodes visited during traversal.",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000, 50000},
		},
		[]string{"dataset", "strategy"},
	)

	// GraphNavigationFrontierMaxSize tracks the maximum frontier size during BFS/ParallelBFS.
	GraphNavigationFrontierMaxSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_navigation_frontier_max_size",
			Help:    "Maximum size of the search frontier during traversal.",
			Buckets: []float64{10, 50, 100, 200, 500, 1000, 2000, 5000},
		},
		[]string{"dataset", "strategy"},
	)

	// GraphNavigationStrategySelectionTotal tracks how often each strategy is picked by the planner.
	GraphNavigationStrategySelectionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_navigation_strategy_selection_total",
			Help: "Total number of times a navigation strategy was selected by the planner.",
		},
		[]string{"dataset", "strategy"},
	)
)

// =============================================================================
// GraphRAG Spreading Activation Metrics (Item 4)
// =============================================================================

var (
	// GraphRAGOperationsTotal tracks spreading-activation GraphRAG operations.
	GraphRAGOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_rag_operations_total",
			Help: "Total number of GraphRAG spreading-activation operations",
		},
		[]string{"dataset", "result"}, // result: success|empty|error
	)

	// GraphRAGAlphaValue tracks the distribution of alpha (damping) values used.
	// alpha=0.0 means no damping (all activation decays immediately);
	// alpha=1.0 means full spreading with no decay.
	GraphRAGAlphaValue = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_rag_alpha_value",
			Help:    "Distribution of GraphRAG spreading activation alpha (damping) values",
			Buckets: []float64{0.0, 0.1, 0.25, 0.5, 0.75, 0.85, 0.9, 0.95, 1.0},
		},
		[]string{"dataset"},
	)

	// GraphRAGDepthValue tracks the distribution of traversal depth values.
	GraphRAGDepthValue = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_rag_depth_value",
			Help:    "Distribution of GraphRAG traversal depth values",
			Buckets: []float64{0, 1, 2, 3, 4, 5, 8, 10},
		},
		[]string{"dataset"},
	)

	// GraphRAGReRankLatencySeconds measures the time for the graph re-ranking phase.
	GraphRAGReRankLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_rag_rerank_latency_seconds",
			Help:    "Latency of the GraphRAG graph re-ranking phase",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		},
		[]string{"dataset"},
	)

	// GraphRAGSeedNodesTotal tracks the number of ANN seed nodes before graph expansion.
	GraphRAGSeedNodesTotal = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_rag_seed_nodes_total",
			Help:    "Number of ANN seed nodes before GraphRAG graph expansion",
			Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500},
		},
		[]string{"dataset"},
	)

	// GraphRAGExpandedNodesTotal tracks the total nodes returned after graph expansion.
	GraphRAGExpandedNodesTotal = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_rag_expanded_nodes_total",
			Help:    "Number of nodes returned after GraphRAG graph expansion",
			Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000},
		},
		[]string{"dataset"},
	)

	// GraphStore Serialization Metrics

	// GraphStoreExportDurationSeconds measures the duration of Arrow export operations
	GraphStoreExportDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_store_export_duration_seconds",
			Help:    "Duration of GraphStore Arrow export operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// GraphStoreImportDurationSeconds measures the duration of Arrow import operations
	GraphStoreImportDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_store_import_duration_seconds",
			Help:    "Duration of GraphStore Arrow import operations",
			Buckets: prometheus.DefBuckets,
		},
	)

	// GraphStoreExportTotal counts GraphStore export operations
	GraphStoreExportTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_store_export_total",
			Help: "Total number of GraphStore export operations",
		},
		[]string{"status"}, // "success", "error"
	)

	// GraphStoreImportTotal counts GraphStore import operations
	GraphStoreImportTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_store_import_total",
			Help: "Total number of GraphStore import operations",
		},
		[]string{"status"}, // "success", "error"
	)

	// GraphStoreEdgeCount tracks the number of edges in GraphStore
	GraphStoreEdgeCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_graph_store_edge_count",
			Help: "Number of edges in GraphStore",
		},
	)

	// GraphStorePredicateCount tracks the number of unique predicates
	GraphStorePredicateCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_graph_store_predicate_count",
			Help: "Number of unique predicates in GraphStore",
		},
	)

	// GraphStoreExportBytes tracks the size of Arrow export data
	GraphStoreExportBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_graph_store_export_bytes",
			Help:    "Size of GraphStore Arrow export in bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 2, 10), // 1KB to 512KB
		},
	)
)
