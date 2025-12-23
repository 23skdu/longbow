package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Memory Management Metrics
var (
	// MemoryLimitRejects counts writes rejected due to memory limit
	MemoryLimitRejects = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_limit_rejects_total",
			Help: "Total number of writes rejected due to memory limit",
		},
	)

	// MemoryEvictionsTriggered counts evictions triggered by writes
	MemoryEvictionsTriggered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_memory_evictions_triggered_total",
			Help: "Total number of evictions triggered by write operations",
		},
	)

	// MemoryCurrentBytes tracks current memory usage
	MemoryCurrentBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_current_bytes",
			Help: "Current memory usage in bytes",
		},
	)

	// MemoryLimitBytes tracks configured memory limit
	MemoryLimitBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_limit_bytes",
			Help: "Configured memory limit in bytes",
		},
	)

	// MemoryUtilization tracks memory utilization percentage
	MemoryUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_memory_utilization",
			Help: "Memory utilization (currentMemory / maxMemory)",
		},
	)
)
