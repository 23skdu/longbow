package memory

import (
	"runtime"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

type mockMemStatsReader struct {
	heapInUse uint64
}

func (m *mockMemStatsReader) ReadMemStats(stats *runtime.MemStats) {
	stats.HeapInuse = m.heapInUse
}

func TestGCTuner_Logic(t *testing.T) {
	// Setup: Limit 100MB, High 100, Low 10
	limit := int64(100 * 1024 * 1024)
	high := 100
	low := 10
	logger := zerolog.Nop()

	tuner := NewGCTuner(limit, high, low, &logger)
	mockReader := &mockMemStatsReader{}
	tuner.reader = mockReader

	tests := []struct {
		name         string
		heapUsage    uint64
		aggressive   bool
		expectedGOGC int
	}{
		{"Low Usage (10%)", uint64(10 * 1024 * 1024), false, 100},     // < 50% -> High
		{"Mid Usage (50%)", uint64(50 * 1024 * 1024), false, 100},     // = 50% -> High
		{"High Usage (90%)", uint64(90 * 1024 * 1024), false, 10},     // = 90% -> Low
		{"Critical Usage (95%)", uint64(95 * 1024 * 1024), false, 10}, // > 90% -> Low
		{"Interpolated (70%)", uint64(70 * 1024 * 1024), false, 55},   // Halfway between 0.5 and 0.9 -> Halfway between 100 and 10 = ~55
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuner.IsAggressive = tt.aggressive
			tuner.tune(tt.heapUsage)
			// We can't easily check debug.SetGCPercent without a wrapper or race,
			// but we can check internal state

			// Allow slight margin for integer math
			assert.InDelta(t, tt.expectedGOGC, tuner.currentGOGC, 5, "Heap: %d", tt.heapUsage)
		})
	}
}
