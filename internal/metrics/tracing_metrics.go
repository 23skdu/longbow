package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type TraceContext struct {
	TraceID    string
	SpanID     string
	Operation  string
	Component  string
	StartTime  time.Time
	Duration   time.Duration
	Attributes map[string]string
	Status     string
	Error      string
}

var (
	TraceSpansCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_spans_created_total",
			Help: "Total number of trace spans created",
		},
		[]string{"operation", "component", "status"},
	)

	TraceDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_trace_duration_seconds",
			Help:    "Duration of trace spans",
			Buckets: []float64{0.0001, 0.001, 0.01, 0.1, 1, 5, 10},
		},
		[]string{"operation", "component"},
	)

	TraceSamplingRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_trace_sampling_rate",
			Help: "Current trace sampling rate (0-1)",
		},
		[]string{"service", "operation"},
	)

	TraceErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_errors_total",
			Help: "Total number of trace span errors",
		},
		[]string{"operation", "component", "error_type"},
	)

	TraceContextPropagationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_context_propagation_total",
			Help: "Total number of trace context propagations",
		},
		[]string{"source", "target", "status"},
	)

	TraceBufferUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_trace_buffer_utilization",
			Help: "Current trace buffer utilization (0-1)",
		},
		[]string{"buffer_type"},
	)

	TraceExportsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_trace_exports_total",
			Help: "Total number of trace exports to backends",
		},
		[]string{"backend", "status"},
	)

	SearchLatencyTraced = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_search_latency_traced_seconds",
			Help:    "Search operation latency with distributed tracing",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 10},
		},
		[]string{"dataset", "search_type"},
	)

	IndexingLatencyTraced = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_indexing_latency_traced_seconds",
			Help:    "Indexing operation latency with distributed tracing",
			Buckets: []float64{0.01, 0.1, 1, 5, 10, 30},
		},
		[]string{"dataset", "index_type"},
	)

	IOLatencyTraced = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_io_latency_traced_seconds",
			Help:    "I/O operation latency with distributed tracing",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5},
		},
		[]string{"operation", "storage_type"},
	)

	CorrelationIDActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_correlation_id_active",
			Help: "Currently active correlation IDs by operation",
		},
		[]string{"operation"},
	)

	CorrelationIDTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_correlation_id_total",
			Help: "Total correlation IDs generated",
		},
		[]string{"operation", "source"},
	)
)

func RecordTraceSpan(ctx *TraceContext) {
	status := "success"
	if ctx.Error != "" {
		status = "error"
	}

	TraceSpansCreated.WithLabelValues(ctx.Operation, ctx.Component, status).Inc()
	TraceDurationSeconds.WithLabelValues(ctx.Operation, ctx.Component).Observe(ctx.Duration.Seconds())

	if ctx.Error != "" {
		TraceErrorsTotal.WithLabelValues(ctx.Operation, ctx.Component, ctx.Error).Inc()
	}
}

func RecordContextPropagation(source, target, status string) {
	TraceContextPropagationTotal.WithLabelValues(source, target, status).Inc()
}

func RecordTraceSamplingRate(service, operation string, rate float64) {
	TraceSamplingRate.WithLabelValues(service, operation).Set(rate)
}

func RecordTraceBufferUtilization(bufferType string, utilization float64) {
	TraceBufferUtilization.WithLabelValues(bufferType).Set(utilization)
}

func RecordTraceExport(backend, status string) {
	TraceExportsTotal.WithLabelValues(backend, status).Inc()
}

func SetActiveCorrelationIDs(operation string, count float64) {
	CorrelationIDActive.WithLabelValues(operation).Set(count)
}

func IncrementCorrelationIDs(operation, source string) {
	CorrelationIDTotal.WithLabelValues(operation, source).Inc()
}

func RecordSearchLatencyTraced(dataset, searchType string, duration time.Duration) {
	SearchLatencyTraced.WithLabelValues(dataset, searchType).Observe(duration.Seconds())
}

func RecordIndexingLatencyTraced(dataset, indexType string, duration time.Duration) {
	IndexingLatencyTraced.WithLabelValues(dataset, indexType).Observe(duration.Seconds())
}

func RecordIOLatencyTraced(operation, storageType string, duration time.Duration) {
	IOLatencyTraced.WithLabelValues(operation, storageType).Observe(duration.Seconds())
}
