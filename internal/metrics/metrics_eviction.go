package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Eviction Metrics
var (
	// EvictionRejectedQueries counts queries rejected due to eviction
	EvictionRejectedQueries = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_eviction_rejected_queries_total",
			Help: "Total queries rejected because dataset was evicting",
		},
	)
)
