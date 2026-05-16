package metrics

import (
	"testing"
	"github.com/prometheus/client_model/go"
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

	// Test SlabRefCountDistribution
	SlabRefCountDistribution.Observe(1)
	SlabRefCountDistribution.Observe(2)
	SlabRefCountDistribution.Observe(10)

	// Since it's a histogram, we just verify it doesn't panic and we can write it
	histMetric := &io_prometheus_client.Metric{}
	if err := SlabRefCountDistribution.Write(histMetric); err != nil {
		t.Fatalf("Failed to write histogram metric: %v", err)
	}
	
	if count := histMetric.GetHistogram().GetSampleCount(); count != 3 {
		t.Errorf("Expected 3 samples in SlabRefCountDistribution, got %d", count)
	}
}
