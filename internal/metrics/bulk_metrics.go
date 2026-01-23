package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Bulk Operation Metrics
// =============================================================================

var (
	// BulkInsertDimensionErrorsTotal counts dimension mismatch errors during bulk inserts
	BulkInsertDimensionErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_bulk_insert_dimension_errors_total",
			Help: "Total number of dimension mismatch errors during bulk vector inserts",
		},
	)

	// NeighborSelectionErrorsTotal counts errors during neighbor selection operations
	NeighborSelectionErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_neighbor_selection_errors_total",
			Help: "Total number of errors during neighbor selection operations",
		},
		[]string{"operation", "error_type"}, // "select_k_neighbors", "take_ids", "take_dists" | "length_mismatch", "kernel_failure"
	)
)
