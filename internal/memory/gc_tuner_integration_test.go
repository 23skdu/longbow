package memory

import (
	"runtime"
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

	// Try to allocate some memory to increase capacity
	for i := 0; i < 5; i++ {
		_, _ = arena.Alloc(256 * 1024) // 256KB at a time, ignore errors in test
	}

	tuner.AddArena(arena.StatsRecord())

	// Use a large heap size to make total ratio high (> 85% of 50MB)
	heapSize := int64(45 * 1024 * 1024) // 45MB heap
	mockReader := &mockMemStatsReader{
		heapInUse: uint64(heapSize),
	}
	tuner.reader = mockReader

	// Total Physical = 45MB (heap) + 1.25MB (arena) = 46.25MB
	// Ratio = 46.25 / 50 = 0.925 (> 0.85)
	tuner.tune(&runtime.MemStats{HeapAlloc: uint64(heapSize)}, true)
	
	// Should set GOGC to lowGOGC (10) due to high total physical pressure
	assert.Equal(t, 10, tuner.currentGOGC, "Should set aggressive GOGC=10 when total physical ratio >0.85")
}
