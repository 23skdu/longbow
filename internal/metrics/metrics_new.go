package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// IndexMigrationDuration measures the time taken to migrate index from HNSW to Sharded
var IndexMigrationDuration = promauto.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "longbow_index_migration_duration_seconds",
		Help:    "Duration of index migration operations",
		Buckets: prometheus.DefBuckets,
	},
)
