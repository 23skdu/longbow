package metrics_test

import (
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getGaugeValue retrieves the current value of a gauge metric
func getGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	err := gauge.Write(&m)
	require.NoError(t, err)
	return m.GetGauge().GetValue()
}

// getCounterValue retrieves the current value of a counter metric
func getCounterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	err := counter.Write(&m)
	require.NoError(t, err)
	return m.GetCounter().GetValue()
}

func TestArenaMemoryBytesRegistration(t *testing.T) {
	// Test that the metric is registered and can be set
	sizeLabel := "4194304" // 4MB

	// Set a value
	testValue := float64(100 * 1024 * 1024) // 100MB
	metrics.ArenaMemoryBytes.WithLabelValues(sizeLabel).Set(testValue)

	// Retrieve and verify
	gauge := metrics.ArenaMemoryBytes.WithLabelValues(sizeLabel)
	value := getGaugeValue(t, gauge)

	assert.Equal(t, testValue, value, "ArenaMemoryBytes should match the set value")
}

func TestArenaMemoryBytesMultipleSizes(t *testing.T) {
	// Test multiple size labels
	sizes := []string{"4194304", "8388608", "16777216", "33554432"}

	for i, size := range sizes {
		testValue := float64((i + 1) * 10 * 1024 * 1024)
		metrics.ArenaMemoryBytes.WithLabelValues(size).Set(testValue)

		gauge := metrics.ArenaMemoryBytes.WithLabelValues(size)
		value := getGaugeValue(t, gauge)

		assert.Equal(t, testValue, value, "ArenaMemoryBytes for size %s should match", size)
	}
}

func TestSlabFragmentationRatioRegistration(t *testing.T) {
	// Test that the metric is registered and can be set
	sizeLabel := "4194304" // 4MB

	// Set a fragmentation ratio (e.g., 0.5 means 50% fragmentation)
	testRatio := 0.5
	metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel).Set(testRatio)

	// Retrieve and verify
	gauge := metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel)
	value := getGaugeValue(t, gauge)

	assert.Equal(t, testRatio, value, "SlabFragmentationRatio should match the set value")
}

func TestSlabFragmentationRatioBounds(t *testing.T) {
	// Test edge cases for fragmentation ratio
	sizeLabel := "8388608" // 8MB

	testCases := []struct {
		name  string
		ratio float64
	}{
		{"zero fragmentation", 0.0},
		{"low fragmentation", 0.1},
		{"medium fragmentation", 0.5},
		{"high fragmentation", 0.9},
		{"very high fragmentation", 1.5}, // Can exceed 1.0 if more pooled than active
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel).Set(tc.ratio)

			gauge := metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel)
			value := getGaugeValue(t, gauge)

			assert.Equal(t, tc.ratio, value, "Fragmentation ratio should match for %s", tc.name)
		})
	}
}

func TestCompactionEventsTotalRegistration(t *testing.T) {
	// Test that the metric is registered and can be incremented
	dataset := "test_dataset"
	eventType := "manual"

	// Get initial value
	counter := metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType)
	initialValue := getCounterValue(t, counter)

	// Increment
	metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType).Inc()

	// Verify increment
	newValue := getCounterValue(t, counter)
	assert.Equal(t, initialValue+1, newValue, "CompactionEventsTotal should increment by 1")
}

func TestCompactionEventsTotalMultipleTypes(t *testing.T) {
	// Test different event types
	dataset := "test_dataset_multi"
	eventTypes := []string{"manual", "auto", "vacuum"}

	for _, eventType := range eventTypes {
		counter := metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType)
		initialValue := getCounterValue(t, counter)

		// Increment multiple times
		for i := 0; i < 3; i++ {
			metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType).Inc()
		}

		newValue := getCounterValue(t, counter)
		assert.Equal(t, initialValue+3, newValue, "CompactionEventsTotal for %s should increment by 3", eventType)
	}
}

func TestCompactionEventsMultipleDatasets(t *testing.T) {
	// Test that different datasets have independent counters
	datasets := []string{"dataset_a", "dataset_b", "dataset_c"}
	eventType := "auto"

	for i, dataset := range datasets {
		counter := metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType)
		initialValue := getCounterValue(t, counter)

		// Increment different amounts for each dataset
		increments := i + 1
		for j := 0; j < increments; j++ {
			metrics.CompactionEventsTotal.WithLabelValues(dataset, eventType).Inc()
		}

		newValue := getCounterValue(t, counter)
		assert.Equal(t, initialValue+float64(increments), newValue,
			"CompactionEventsTotal for %s should increment by %d", dataset, increments)
	}
}

func TestMetricsIntegration(t *testing.T) {
	// Integration test: simulate a complete workflow
	sizeLabel := "4194304"
	dataset := "integration_test"

	// 1. Allocate some arena memory
	arenaBytes := float64(50 * 1024 * 1024)
	metrics.ArenaMemoryBytes.WithLabelValues(sizeLabel).Set(arenaBytes)

	// 2. Set fragmentation ratio
	fragmentation := 0.3
	metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel).Set(fragmentation)

	// 3. Trigger compaction events
	metrics.CompactionEventsTotal.WithLabelValues(dataset, "manual").Inc()
	metrics.CompactionEventsTotal.WithLabelValues(dataset, "vacuum").Inc()

	// Verify all metrics
	arenaGauge := metrics.ArenaMemoryBytes.WithLabelValues(sizeLabel)
	assert.Equal(t, arenaBytes, getGaugeValue(t, arenaGauge))

	fragGauge := metrics.SlabFragmentationRatio.WithLabelValues(sizeLabel)
	assert.Equal(t, fragmentation, getGaugeValue(t, fragGauge))

	manualCounter := metrics.CompactionEventsTotal.WithLabelValues(dataset, "manual")
	assert.GreaterOrEqual(t, getCounterValue(t, manualCounter), float64(1))

	vacuumCounter := metrics.CompactionEventsTotal.WithLabelValues(dataset, "vacuum")
	assert.GreaterOrEqual(t, getCounterValue(t, vacuumCounter), float64(1))
}
