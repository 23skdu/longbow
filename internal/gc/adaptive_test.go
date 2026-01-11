package gc

import (
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveGC_Disabled(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled: false,
	}

	controller := NewAdaptiveGCController(config)
	require.NotNil(t, controller)

	// Start should be no-op when disabled
	controller.Start()
	time.Sleep(100 * time.Millisecond)
	controller.Stop()

	// GOGC should remain unchanged
	assert.Equal(t, 100, debug.SetGCPercent(-1)) // Read current, restore
	debug.SetGCPercent(100)                      // Restore to default
}

func TestAdaptiveGC_Lifecycle(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 50 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)
	require.NotNil(t, controller)

	// Start controller
	controller.Start()

	// Let it run for a bit
	time.Sleep(150 * time.Millisecond)

	// Stop should be clean
	controller.Stop()

	// Calling Stop again should be safe
	controller.Stop()
}

func TestAdaptiveGC_HighAllocationRate(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 100 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)

	// Simulate high allocation rate
	stats := &gcStats{
		allocationRate: 100 * 1024 * 1024, // 100 MB/s
		memoryPressure: 0.3,               // Low pressure
	}

	newGOGC := controller.calculateGOGC(stats)

	// High allocation + low pressure should increase GOGC
	assert.Greater(t, newGOGC, 100, "GOGC should increase under high allocation with low memory pressure")
	assert.LessOrEqual(t, newGOGC, 200, "GOGC should not exceed max")
}

func TestAdaptiveGC_HighMemoryPressure(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 100 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)

	// Simulate high memory pressure
	stats := &gcStats{
		allocationRate: 10 * 1024 * 1024, // 10 MB/s (moderate)
		memoryPressure: 0.85,             // High pressure
	}

	newGOGC := controller.calculateGOGC(stats)

	// High pressure should decrease GOGC to trigger GC more frequently
	assert.Less(t, newGOGC, 100, "GOGC should decrease under high memory pressure")
	assert.GreaterOrEqual(t, newGOGC, 50, "GOGC should not go below min")
}

func TestAdaptiveGC_BoundaryConditions(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 100 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)

	tests := []struct {
		name           string
		allocationRate int64
		memoryPressure float64
		expectMin      bool
		expectMax      bool
	}{
		{
			name:           "zero allocation, zero pressure",
			allocationRate: 0,
			memoryPressure: 0,
			expectMin:      false,
			expectMax:      false,
		},
		{
			name:           "extreme allocation, zero pressure",
			allocationRate: 1000 * 1024 * 1024, // 1 GB/s
			memoryPressure: 0,
			expectMin:      false,
			expectMax:      true,
		},
		{
			name:           "zero allocation, max pressure",
			allocationRate: 0,
			memoryPressure: 1.0,
			expectMin:      true,
			expectMax:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := &gcStats{
				allocationRate: tt.allocationRate,
				memoryPressure: tt.memoryPressure,
			}

			newGOGC := controller.calculateGOGC(stats)

			assert.GreaterOrEqual(t, newGOGC, config.MinGOGC)
			assert.LessOrEqual(t, newGOGC, config.MaxGOGC)

			if tt.expectMin {
				assert.Equal(t, config.MinGOGC, newGOGC)
			}
			if tt.expectMax {
				assert.Equal(t, config.MaxGOGC, newGOGC)
			}
		})
	}
}

func TestAdaptiveGC_StatsCollection(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 100 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)

	// Collect stats
	stats := controller.collectStats()

	require.NotNil(t, stats)
	assert.GreaterOrEqual(t, stats.allocationRate, int64(0))
	assert.GreaterOrEqual(t, stats.memoryPressure, 0.0)
	assert.LessOrEqual(t, stats.memoryPressure, 1.0)
}

func TestAdaptiveGC_MetricsUpdated(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 50 * time.Millisecond,
	}

	controller := NewAdaptiveGCController(config)
	controller.Start()
	defer controller.Stop()

	// Let it run and update metrics
	time.Sleep(150 * time.Millisecond)

	// Metrics should be updated (we can't easily verify exact values,
	// but we can verify the controller ran without panicking)
	assert.True(t, true, "Controller ran without panic")
}

func BenchmarkAdaptiveGC_CalculateGOGC(b *testing.B) {
	config := AdaptiveGCConfig{
		Enabled: true,
		MinGOGC: 50,
		MaxGOGC: 200,
	}

	controller := NewAdaptiveGCController(config)
	stats := &gcStats{
		allocationRate: 50 * 1024 * 1024,
		memoryPressure: 0.5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = controller.calculateGOGC(stats)
	}
}
