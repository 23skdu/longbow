package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RecordGraphStoreExport records metrics for a GraphStore Arrow export operation
func RecordGraphStoreExport(duration time.Duration, edgeCount int, byteSize int64, success bool) {
	GraphStoreExportDurationSeconds.Observe(duration.Seconds())
	if success {
		GraphStoreExportTotal.WithLabelValues("success").Inc()
	} else {
		GraphStoreExportTotal.WithLabelValues("error").Inc()
	}
	GraphStoreEdgeCount.Set(float64(edgeCount))
	if byteSize > 0 {
		GraphStoreExportBytes.Observe(float64(byteSize))
	}
}

// RecordGraphStoreImport records metrics for a GraphStore Arrow import operation
func RecordGraphStoreImport(duration time.Duration, edgeCount int, success bool) {
	GraphStoreImportDurationSeconds.Observe(duration.Seconds())
	if success {
		GraphStoreImportTotal.WithLabelValues("success").Inc()
	} else {
		GraphStoreImportTotal.WithLabelValues("error").Inc()
	}
	GraphStoreEdgeCount.Set(float64(edgeCount))
}

// RecordGraphStorePredicateCount updates the predicate count metric
func RecordGraphStorePredicateCount(count int) {
	GraphStorePredicateCount.Set(float64(count))
}

// RecordGraphStoreEdgeCount updates the edge count metric
func RecordGraphStoreEdgeCount(count int) {
	GraphStoreEdgeCount.Set(float64(count))
}

// GraphStoreMetricsCollector collects GraphStore metrics
type GraphStoreMetricsCollector struct {
	edgeCount    prometheus.Gauge
	predicateCnt prometheus.Gauge
	exportDur    prometheus.Observer
	importDur    prometheus.Observer
}

// NewGraphStoreMetricsCollector creates a new metrics collector for GraphStore
func NewGraphStoreMetricsCollector() *GraphStoreMetricsCollector {
	return &GraphStoreMetricsCollector{
		edgeCount:    GraphStoreEdgeCount,
		predicateCnt: GraphStorePredicateCount,
		exportDur:    GraphStoreExportDurationSeconds,
		importDur:    GraphStoreImportDurationSeconds,
	}
}

// UpdateEdgeCount updates the current edge count
func (c *GraphStoreMetricsCollector) UpdateEdgeCount(count int) {
	c.edgeCount.Set(float64(count))
}

// UpdatePredicateCount updates the current predicate count
func (c *GraphStoreMetricsCollector) UpdatePredicateCount(count int) {
	c.predicateCnt.Set(float64(count))
}
