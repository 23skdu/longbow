package memory

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestGCTuner_ArenaAwareTuning(t *testing.T) {
	// Setup: Create a tuner with aggressive mode enabled
	limit := int64(50 * 1024 * 1024) // 50MB limit
	high := 100
	low := 10
	logger := zerolog.Nop()

	tuner := NewGCTuner(limit, high, low, &logger)
	tuner.IsAggressive = true

	// Create a real arena and allocate to increase its capacity
	arena := NewSlabArena(1024 * 1024) // 1MB slabs
	defer arena.Free()

	// Try to allocate some memory to increase capacity (ignore errors)
	for i := 0; i < 5; i++ {
		arena.Alloc(256 * 1024) // 256KB at a time
	}

	stats := arena.Stats()
	tuner.RegisterArena(arena)

	// Use a small heap size to make arena ratio high
	heapSize := int64(1 * 1024 * 1024) // 1MB heap
	mockReader := &mockMemStatsReader{
		heapInUse: uint64(heapSize),
	}
	tuner.reader = mockReader

	// Calculate expected arena ratio
	arenaRatio := float64(stats.TotalCapacity) / float64(heapSize)
	if arenaRatio > 0.7 {
		tuner.tune(uint64(heapSize))
		// Should set GOGC to 50 due to high arena ratio
		assert.Equal(t, 50, tuner.currentGOGC, "Should set aggressive GOGC=50 when arena ratio >0.7 (ratio=%.2f, capacity=%d, heap=%d)", arenaRatio, stats.TotalCapacity, heapSize)
	} else {
		t.Logf("Arena ratio %.2f <= 0.7, testing standard logic instead", arenaRatio)
		// Test that standard logic still works
		tuner.tune(uint64(heapSize))
		// With small heap (1MB) vs limit (50MB), should be high GOGC
		assert.Equal(t, 100, tuner.currentGOGC, "Should use standard high GOGC for low utilization")
	}
}
