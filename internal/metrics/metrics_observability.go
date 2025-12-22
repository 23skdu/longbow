package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// VectorSearchGPULatencySeconds measures the latency of GPU search operations
	VectorSearchGPULatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_vector_search_gpu_latency_seconds",
			Help:    "Latency of GPU vector search operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"}, // "search", "add"
	)

	// VectorSearchGPUOperationsTotal counts GPU operations
	VectorSearchGPUOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_vector_search_gpu_operations_total",
			Help: "Total number of GPU vector search operations",
		},
		[]string{"operation", "status"},
	)

	// CompactionDurationSeconds measures duration of compaction jobs
	CompactionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_compaction_duration_seconds",
			Help:    "Duration of compaction jobs",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"dataset", "type"}, // Added labels to match usage
	)

	// CompactionRecordsProcessedTotal counts records processed during compaction
	CompactionRecordsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_records_processed_total",
			Help: "Total number of records processed during compaction",
		},
	)

	// CompactionRecordsRemovedTotal - Records removed during compaction (restored)
	CompactionRecordsRemovedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_compaction_records_removed_total",
			Help: "Total number of records removed during compaction",
		},
		[]string{"dataset"},
	)

	// CompactionAutoTriggersTotal - Auto-triggered compaction tracking (restored)
	CompactionAutoTriggersTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_auto_triggers_total",
			Help: "Total number of auto-triggered compactions when batch count exceeds threshold",
		},
	)

	// CompactionErrorsTotal counts compaction errors
	CompactionErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_compaction_errors_total",
			Help: "Total number of compaction errors",
		},
	)

	// RateLimitRequestsTotal counts rate limited requests
	RateLimitRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_rate_limit_requests_total",
			Help: "Total number of requests handled by rate limiter",
		},
		[]string{"status"}, // "allowed", "throttled"
	)

	// ProxyRequestsForwardedTotal counts requests forwarded to other nodes
	ProxyRequestsForwardedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_proxy_requests_forwarded_total",
			Help: "Total number of requests forwarded to other nodes",
		},
		[]string{"method", "status"},
	)

	// ProxyRequestLatencySeconds measures latency of forwarded requests
	ProxyRequestLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_proxy_request_latency_seconds",
			Help:    "Latency of forwarded requests",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)
)
