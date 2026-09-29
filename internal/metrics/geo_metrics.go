package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// GeoSearchOpsTotal tracks total number of geospatial search operations
	GeoSearchOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_geo_search_ops_total",
			Help: "Total number of geospatial search operations",
		},
		[]string{"dataset", "search_type"}, // "radius", "box", "hybrid"
	)

	// GeoSearchDurationSeconds tracks latency of geospatial searches
	GeoSearchDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_geo_search_duration_seconds",
			Help:    "Duration of geospatial search operations in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"dataset", "search_type"},
	)

	// GeoIndexPointsTotal tracks total number of points in geo indexes
	GeoIndexPointsTotal = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_geo_index_points_total",
			Help: "Total number of points stored in geospatial indexes",
		},
		[]string{"dataset"},
	)

	// QuadtreeSubdivisionsTotal tracks number of times quadtrees have subdivided
	QuadtreeSubdivisionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quadtree_subdivisions_total",
			Help: "Total number of quadtree subdivisions (node splits)",
		},
		[]string{"dataset"},
	)

	// MortonGridCellsCreatedTotal tracks number of cells created in Morton grids
	MortonGridCellsCreatedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_morton_grid_cells_created_total",
			Help: "Total number of cells allocated in linear Morton spatial grids",
		},
		[]string{"dataset"},
	)
)
