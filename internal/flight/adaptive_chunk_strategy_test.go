package flight

import (
	"testing"
)

// TestAdaptiveChunkStrategy_ExponentialGrowth tests exponential chunk size growth
func TestAdaptiveChunkStrategy_ExponentialGrowth(t *testing.T) {
	strategy := NewAdaptiveChunkStrategy(4096, 65536, 2.0)

	// First chunk should be minimum size
	if size := strategy.NextChunkSize(); size != 4096 {
		t.Errorf("First chunk size = %d, want 4096", size)
	}

	// Should double each time
	expectedSizes := []int{8192, 16384, 32768, 65536}
	for i, expected := range expectedSizes {
		size := strategy.NextChunkSize()
		if size != expected {
			t.Errorf("Chunk %d size = %d, want %d", i+2, size, expected)
		}
	}

	// Should cap at max size
	for i := 0; i < 5; i++ {
		size := strategy.NextChunkSize()
		if size != 65536 {
			t.Errorf("Capped chunk size = %d, want 65536", size)
		}
	}
}

// TestAdaptiveChunkStrategy_Reset tests reset behavior
func TestAdaptiveChunkStrategy_Reset(t *testing.T) {
	strategy := NewAdaptiveChunkStrategy(4096, 65536, 2.0)

	// Grow to max
	for i := 0; i < 10; i++ {
		strategy.NextChunkSize()
	}

	// Reset should go back to min
	strategy.Reset()
	if size := strategy.NextChunkSize(); size != 4096 {
		t.Errorf("After reset, first chunk size = %d, want 4096", size)
	}
}

// TestAdaptiveChunkStrategy_MinEqualsMax tests when min equals max
func TestAdaptiveChunkStrategy_MinEqualsMax(t *testing.T) {
	strategy := NewAdaptiveChunkStrategy(8192, 8192, 2.0)

	// Should always return the same size
	for i := 0; i < 10; i++ {
		size := strategy.NextChunkSize()
		if size != 8192 {
			t.Errorf("Chunk %d size = %d, want 8192", i, size)
		}
	}
}

// TestAdaptiveChunkStrategy_LinearGrowth tests linear growth strategy
func TestAdaptiveChunkStrategy_LinearGrowth(t *testing.T) {
	strategy := NewLinearChunkStrategy(4096, 65536, 4096)

	// Should grow linearly by increment
	expectedSizes := []int{4096, 8192, 12288, 16384, 20480}
	for i, expected := range expectedSizes {
		size := strategy.NextChunkSize()
		if size != expected {
			t.Errorf("Chunk %d size = %d, want %d", i+1, size, expected)
		}
	}
}

// TestAdaptiveChunkStrategy_SmallDataset tests behavior with small datasets
func TestAdaptiveChunkStrategy_SmallDataset(t *testing.T) {
	strategy := NewAdaptiveChunkStrategy(4096, 65536, 2.0)

	// For a dataset with only 1000 rows, first chunk should handle it all
	firstChunk := strategy.NextChunkSize()
	if firstChunk < 1000 {
		t.Errorf("First chunk size %d too small for 1000 row dataset", firstChunk)
	}
}

// TestAdaptiveChunkStrategy_CurrentSize tests getting current size without advancing
func TestAdaptiveChunkStrategy_CurrentSize(t *testing.T) {
	strategy := NewAdaptiveChunkStrategy(4096, 65536, 2.0)

	// Current size should be min before any NextChunkSize calls
	if size := strategy.CurrentSize(); size != 4096 {
		t.Errorf("Initial current size = %d, want 4096", size)
	}

	// Advance once - returns 4096, updates internal state to 8192
	if size := strategy.NextChunkSize(); size != 4096 {
		t.Errorf("First NextChunkSize = %d, want 4096", size)
	}

	// Current size should now be 8192 (the next size to be returned)
	if size := strategy.CurrentSize(); size != 8192 {
		t.Errorf("Current size after first next = %d, want 8192", size)
	}

	// Next should return 8192 and update to 16384
	if size := strategy.NextChunkSize(); size != 8192 {
		t.Errorf("Second chunk size = %d, want 8192", size)
	}

	if size := strategy.CurrentSize(); size != 16384 {
		t.Errorf("Current size after second next = %d, want 16384", size)
	}
}

// BenchmarkAdaptiveChunkStrategy_NextChunkSize benchmarks the overhead
func BenchmarkAdaptiveChunkStrategy_NextChunkSize(b *testing.B) {
	strategy := NewAdaptiveChunkStrategy(4096, 65536, 2.0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.NextChunkSize()
		if i%10 == 0 {
			strategy.Reset() // Reset periodically to avoid capping
		}
	}
}

// BenchmarkFixedChunkSize_Baseline benchmarks fixed chunk size (baseline)
func BenchmarkFixedChunkSize_Baseline(b *testing.B) {
	const chunkSize = 32768

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chunkSize // Simulate getting chunk size
	}
}
