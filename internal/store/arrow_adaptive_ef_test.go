package store

import "testing"

func TestGetAdaptiveEf(t *testing.T) {
	tests := []struct {
		name        string
		config      ArrowHNSWConfig
		nodeCount   int
		expectedMin int
		expectedMax int
	}{
		{
			name: "Disabled - returns base ef",
			config: ArrowHNSWConfig{
				EfConstruction: 400,
				AdaptiveEf:     false,
			},
			nodeCount:   5000,
			expectedMin: 400,
			expectedMax: 400,
		},
		{
			name: "At start - returns min ef",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       100,
				AdaptiveEfThreshold: 10000,
				InitialCapacity:     20000,
			},
			nodeCount:   0,
			expectedMin: 100,
			expectedMax: 100,
		},
		{
			name: "At threshold - returns full ef",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       100,
				AdaptiveEfThreshold: 10000,
				InitialCapacity:     20000,
			},
			nodeCount:   10000,
			expectedMin: 400,
			expectedMax: 400,
		},
		{
			name: "Past threshold - returns full ef",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       100,
				AdaptiveEfThreshold: 10000,
				InitialCapacity:     20000,
			},
			nodeCount:   15000,
			expectedMin: 400,
			expectedMax: 400,
		},
		{
			name: "Midpoint - returns interpolated ef",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       100,
				AdaptiveEfThreshold: 10000,
				InitialCapacity:     20000,
			},
			nodeCount:   5000, // 50% of threshold
			expectedMin: 240,  // Should be around 250 (100 + 0.5 * 300)
			expectedMax: 260,
		},
		{
			name: "Auto-calculate min (ef/4)",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       0, // Auto-calculate
				AdaptiveEfThreshold: 10000,
				InitialCapacity:     20000,
			},
			nodeCount:   0,
			expectedMin: 100, // 400 / 4
			expectedMax: 100,
		},
		{
			name: "Auto-calculate threshold (capacity/2)",
			config: ArrowHNSWConfig{
				EfConstruction:      400,
				AdaptiveEf:          true,
				AdaptiveEfMin:       100,
				AdaptiveEfThreshold: 0, // Auto-calculate
				InitialCapacity:     20000,
			},
			nodeCount:   10000, // Should be at threshold (20000/2)
			expectedMin: 400,
			expectedMax: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &ArrowHNSW{
				config: tt.config,
			}
			h.efConstruction.Store(int32(tt.config.EfConstruction))

			result := h.getAdaptiveEf(tt.nodeCount)

			if result < tt.expectedMin || result > tt.expectedMax {
				t.Errorf("getAdaptiveEf(%d) = %d, want between %d and %d",
					tt.nodeCount, result, tt.expectedMin, tt.expectedMax)
			}
		})
	}
}

func TestAdaptiveEfLinearRamp(t *testing.T) {
	config := ArrowHNSWConfig{
		EfConstruction:      400,
		AdaptiveEf:          true,
		AdaptiveEfMin:       100,
		AdaptiveEfThreshold: 10000,
		InitialCapacity:     20000,
	}

	h := &ArrowHNSW{
		config: config,
	}
	h.efConstruction.Store(int32(config.EfConstruction))

	// Test that ef increases monotonically
	prevEf := 0
	for nodeCount := 0; nodeCount <= 10000; nodeCount += 1000 {
		ef := h.getAdaptiveEf(nodeCount)

		if ef < prevEf {
			t.Errorf("ef decreased: nodeCount=%d, ef=%d, prevEf=%d", nodeCount, ef, prevEf)
		}

		if ef < 100 || ef > 400 {
			t.Errorf("ef out of range: nodeCount=%d, ef=%d", nodeCount, ef)
		}

		prevEf = ef
	}
}

func TestAdaptiveEfMinimumBounds(t *testing.T) {
	// Test that minimum ef is at least 50 even if calculated value is lower
	config := ArrowHNSWConfig{
		EfConstruction:      100, // Low base ef
		AdaptiveEf:          true,
		AdaptiveEfMin:       0, // Auto-calculate: 100/4 = 25
		AdaptiveEfThreshold: 10000,
		InitialCapacity:     20000,
	}

	h := &ArrowHNSW{
		config: config,
	}
	h.efConstruction.Store(int32(config.EfConstruction))

	ef := h.getAdaptiveEf(0)

	if ef < 50 {
		t.Errorf("ef below absolute minimum: ef=%d, want >= 50", ef)
	}
}

func TestSetEfConstruction(t *testing.T) {
	config := ArrowHNSWConfig{
		EfConstruction: 400,
		AdaptiveEf:     true, // Enabled to check base scaling
	}
	h := &ArrowHNSW{
		config: config,
	}
	h.efConstruction.Store(int32(config.EfConstruction))

	// Initial check
	if ef := h.getAdaptiveEf(100000); ef != 400 {
		t.Errorf("Initial ef = %d, want 400", ef)
	}

	// Dynamic update
	h.SetEfConstruction(100)

	// Check update reflected
	if val := h.efConstruction.Load(); val != 100 {
		t.Errorf("efConstruction atomic = %d, want 100", val)
	}

	// Check adaptive scaling uses new base
	// At high node count, should be close to base
	if ef := h.getAdaptiveEf(100000); ef != 100 {
		t.Errorf("Updated ef = %d, want 100", ef)
	}
}
