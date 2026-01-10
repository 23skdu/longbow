package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HNSWRepairTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_repair_total",
		Help: "Total number of tombstone repairs performed",
	})
	HNSWRepairDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "longbow_hnsw_repair_duration_seconds",
		Help:    "Duration of tombstone repair cycles",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	})
	HNSWRepairedConnections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_hnsw_repaired_connections_total",
		Help: "Number of connections re-wired from tombstones to valid nodes",
	})
)
