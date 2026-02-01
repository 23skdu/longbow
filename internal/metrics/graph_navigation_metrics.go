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
