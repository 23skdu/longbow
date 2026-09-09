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

	// EvictionLayersEvictedTotal counts the number of HNSW layers evicted to disk
	EvictionLayersEvictedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_eviction_layers_evicted_total",
			Help: "Total number of HNSW graph layers evicted to disk",
		},
		[]string{"dataset"},
	)

	// EvictionLayersRestoredTotal counts the number of HNSW layers restored from disk
	EvictionLayersRestoredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_eviction_layers_restored_total",
			Help: "Total number of HNSW graph layers restored from disk",
		},
		[]string{"dataset"},
	)

	// EvictionBytesFreedTotal tracks total bytes freed by layer eviction
	EvictionBytesFreedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_eviction_bytes_freed_total",
			Help: "Total bytes freed by HNSW layer eviction to disk",
		},
		[]string{"dataset"},
	)

	// EvictionErrorsTotal counts eviction/restore errors
	EvictionErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_eviction_errors_total",
			Help: "Total number of eviction or restore errors",
		},
		[]string{"dataset", "operation"},
	)

	// EvictionActiveLayers tracks the number of currently evicted layers
	EvictionActiveLayers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_eviction_active_layers",
			Help: "Number of HNSW layers currently evicted to disk",
		},
		[]string{"dataset"},
	)

	// EvictionHeapUtilization tracks heap utilization at eviction time
	EvictionHeapUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_eviction_heap_utilization",
			Help: "Heap utilization ratio when eviction was triggered",
		},
	)
)
