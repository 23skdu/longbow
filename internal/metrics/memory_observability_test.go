package metrics

import (
	"testing"

	io_prometheus_client "github.com/prometheus/client_model/go"
)

func TestMemoryObservabilityMetrics(t *testing.T) {
	// Test SlabActiveArenas
	SlabActiveArenas.WithLabelValues("1048576").Set(42)

	metric := &io_prometheus_client.Metric{}
	if err := SlabActiveArenas.WithLabelValues("1048576").Write(metric); err != nil {
		t.Fatalf("Failed to write metric: %v", err)
	}

	if val := metric.GetGauge().GetValue(); val != 42 {
		t.Errorf("Expected SlabActiveArenas value 42, got %f", val)
	}

	// Test SlabRefCountDistribution (now a HistogramVec, requires a 'size' label).
	// Observe must not panic – the full histogram shape is verified by the slab_metrics_test.
	require := func(err error, msg string) {
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
	}
	_ = require // suppress unused warning
	SlabRefCountDistribution.WithLabelValues("test").Observe(1)
	SlabRefCountDistribution.WithLabelValues("test").Observe(2)
	SlabRefCountDistribution.WithLabelValues("test").Observe(10)
	// Observations completed without panic – metric is correctly wired.
}
