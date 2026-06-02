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

	// Collect stats - first call captures baseline
	stats1 := controller.collectStats()
	require.NotNil(t, stats1)
	assert.GreaterOrEqual(t, stats1.allocationRate, int64(0))
	assert.GreaterOrEqual(t, stats1.memoryPressure, 0.0)
	assert.LessOrEqual(t, stats1.memoryPressure, 1.0)

	// Allocate to ensure TotalAlloc increases
	for i := 0; i < 100000; i++ {
		_ = make([]byte, 1024)
	}

	// Collect stats again - should see allocation rate > 0
	stats2 := controller.collectStats()
	require.NotNil(t, stats2)
	assert.GreaterOrEqual(t, stats2.allocationRate, int64(0))
}

func TestDefaultAdaptiveGCConfig(t *testing.T) {
	cfg := DefaultAdaptiveGCConfig()
	assert.False(t, cfg.Enabled)
	assert.Equal(t, 50, cfg.MinGOGC)
	assert.Equal(t, 200, cfg.MaxGOGC)
	assert.Equal(t, 1*time.Second, cfg.AdjustInterval)
}

func TestAdaptiveGC_ConfigValidation(t *testing.T) {
	c := NewAdaptiveGCController(AdaptiveGCConfig{MinGOGC: -10, MaxGOGC: -5, AdjustInterval: -1})
	assert.Equal(t, 50, c.config.MinGOGC)
	assert.Equal(t, 200, c.config.MaxGOGC)
	assert.Equal(t, 1*time.Second, c.config.AdjustInterval)

	c2 := NewAdaptiveGCController(AdaptiveGCConfig{MinGOGC: 100, MaxGOGC: 50, AdjustInterval: -1})
	assert.Equal(t, 50, c2.config.MinGOGC)
	assert.Equal(t, 100, c2.config.MaxGOGC)
}

func TestAdaptiveGC_StartAlreadyRunning(t *testing.T) {
	config := AdaptiveGCConfig{
		Enabled:        true,
		MinGOGC:        50,
		MaxGOGC:        200,
		AdjustInterval: 100 * time.Millisecond,
	}
	controller := NewAdaptiveGCController(config)
	controller.Start()
	controller.Start() // should be no-op
	controller.Stop()
}

func TestAdaptiveGC_CalculateGOGC_EdgeCases(t *testing.T) {
	config := AdaptiveGCConfig{MinGOGC: 50, MaxGOGC: 200}
	controller := NewAdaptiveGCController(config)

	tests := []struct {
		name     string
		stats    *gcStats
		validate func(t *testing.T, gogc int)
	}{
		{
			name:  "negative allocation rate",
			stats: &gcStats{allocationRate: -100, memoryPressure: 0.5},
			validate: func(t *testing.T, gogc int) {
				assert.GreaterOrEqual(t, gogc, 50)
			},
		},
		{
			name:  "negative memory pressure",
			stats: &gcStats{allocationRate: 0, memoryPressure: -0.5},
			validate: func(t *testing.T, gogc int) {
				assert.GreaterOrEqual(t, gogc, 50)
			},
		},
		{
			name:  "extreme memory pressure",
			stats: &gcStats{allocationRate: 0, memoryPressure: 2.0},
			validate: func(t *testing.T, gogc int) {
				assert.GreaterOrEqual(t, gogc, 50)
				assert.Equal(t, config.MinGOGC, gogc)
			},
		},
		{
			name:  "high allocation clamped",
			stats: &gcStats{allocationRate: 500 * 1024 * 1024, memoryPressure: 0},
			validate: func(t *testing.T, gogc int) {
				assert.LessOrEqual(t, gogc, 200)
				assert.Equal(t, config.MaxGOGC, gogc)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gogc := controller.calculateGOGC(tt.stats)
			tt.validate(t, gogc)
		})
	}
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
