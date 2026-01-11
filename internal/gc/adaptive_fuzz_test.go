package gc

import (
	"testing"
)

// FuzzAdaptiveGC_AllocationRate fuzzes the allocation rate input
// to ensure the controller handles extreme values without panicking
func FuzzAdaptiveGC_AllocationRate(f *testing.F) {
	// Seed corpus with interesting values
	f.Add(int64(0))                   // Zero allocation
	f.Add(int64(1024))                // 1 KB/s
	f.Add(int64(1024 * 1024))         // 1 MB/s
	f.Add(int64(100 * 1024 * 1024))   // 100 MB/s
	f.Add(int64(1000 * 1024 * 1024))  // 1 GB/s
	f.Add(int64(-1))                  // Negative (invalid)
	f.Add(int64(9223372036854775807)) // Max int64

	config := AdaptiveGCConfig{
		Enabled: true,
		MinGOGC: 50,
		MaxGOGC: 200,
	}

	controller := NewAdaptiveGCController(config)

	f.Fuzz(func(t *testing.T, allocationRate int64) {
		stats := &gcStats{
			allocationRate: allocationRate,
			memoryPressure: 0.5, // Fixed moderate pressure
		}

		// Should not panic
		gogc := controller.calculateGOGC(stats)

		// Should respect bounds
		if gogc < config.MinGOGC || gogc > config.MaxGOGC {
			t.Errorf("GOGC %d out of bounds [%d, %d]", gogc, config.MinGOGC, config.MaxGOGC)
		}
	})
}

// FuzzAdaptiveGC_MemoryPressure fuzzes the memory pressure input
func FuzzAdaptiveGC_MemoryPressure(f *testing.F) {
	// Seed corpus
	f.Add(float64(0.0))   // No pressure
	f.Add(float64(0.25))  // Low pressure
	f.Add(float64(0.5))   // Moderate pressure
	f.Add(float64(0.75))  // High pressure
	f.Add(float64(1.0))   // Max pressure
	f.Add(float64(-0.1))  // Negative (invalid)
	f.Add(float64(1.5))   // Over 100% (invalid)
	f.Add(float64(999.9)) // Extreme value

	config := AdaptiveGCConfig{
		Enabled: true,
		MinGOGC: 50,
		MaxGOGC: 200,
	}

	controller := NewAdaptiveGCController(config)

	f.Fuzz(func(t *testing.T, memoryPressure float64) {
		stats := &gcStats{
			allocationRate: 50 * 1024 * 1024, // Fixed 50 MB/s
			memoryPressure: memoryPressure,
		}

		// Should not panic
		gogc := controller.calculateGOGC(stats)

		// Should respect bounds
		if gogc < config.MinGOGC || gogc > config.MaxGOGC {
			t.Errorf("GOGC %d out of bounds [%d, %d]", gogc, config.MinGOGC, config.MaxGOGC)
		}
	})
}

// FuzzAdaptiveGC_Combined fuzzes both allocation rate and memory pressure
func FuzzAdaptiveGC_Combined(f *testing.F) {
	// Seed corpus with combinations
	f.Add(int64(0), float64(0.0))
	f.Add(int64(100*1024*1024), float64(0.5))
	f.Add(int64(1000*1024*1024), float64(0.9))
	f.Add(int64(-1), float64(-1.0))
	f.Add(int64(9223372036854775807), float64(999.9))

	config := AdaptiveGCConfig{
		Enabled: true,
		MinGOGC: 50,
		MaxGOGC: 200,
	}

	controller := NewAdaptiveGCController(config)

	f.Fuzz(func(t *testing.T, allocationRate int64, memoryPressure float64) {
		stats := &gcStats{
			allocationRate: allocationRate,
			memoryPressure: memoryPressure,
		}

		// Should not panic
		gogc := controller.calculateGOGC(stats)

		// Should respect bounds
		if gogc < config.MinGOGC || gogc > config.MaxGOGC {
			t.Errorf("GOGC %d out of bounds [%d, %d] for rate=%d pressure=%f",
				gogc, config.MinGOGC, config.MaxGOGC, allocationRate, memoryPressure)
		}
	})
}

// FuzzAdaptiveGC_ConfigBounds fuzzes the configuration bounds themselves
func FuzzAdaptiveGC_ConfigBounds(f *testing.F) {
	// Seed corpus
	f.Add(10, 500)
	f.Add(50, 200)
	f.Add(100, 100) // Min == Max
	f.Add(200, 50)  // Min > Max (invalid)
	f.Add(-10, 200) // Negative min
	f.Add(50, -10)  // Negative max

	f.Fuzz(func(t *testing.T, minGOGC int, maxGOGC int) {
		config := AdaptiveGCConfig{
			Enabled: true,
			MinGOGC: minGOGC,
			MaxGOGC: maxGOGC,
		}

		// Should not panic during construction
		controller := NewAdaptiveGCController(config)

		stats := &gcStats{
			allocationRate: 50 * 1024 * 1024,
			memoryPressure: 0.5,
		}

		// Should not panic during calculation
		gogc := controller.calculateGOGC(stats)

		// If config is valid, GOGC should be in bounds
		if minGOGC > 0 && maxGOGC > 0 && minGOGC <= maxGOGC {
			if gogc < minGOGC || gogc > maxGOGC {
				t.Errorf("GOGC %d out of bounds [%d, %d]", gogc, minGOGC, maxGOGC)
			}
		}
	})
}
