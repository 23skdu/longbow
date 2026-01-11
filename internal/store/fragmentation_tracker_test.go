package store

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFragmentationTracker_RecordDeletion(t *testing.T) {
	tracker := NewFragmentationTracker()

	// Set batch size
	tracker.SetBatchSize(0, 100)

	// Record deletions
	tracker.RecordDeletion(0)
	tracker.RecordDeletion(0)
	tracker.RecordDeletion(0)

	// Density should be 3/100 = 0.03
	density := tracker.GetDensity(0)
	assert.InDelta(t, 0.03, density, 0.001, "Density should be 3%")
}

func TestFragmentationTracker_GetFragmentedBatches(t *testing.T) {
	tracker := NewFragmentationTracker()

	// Setup batches
	tracker.SetBatchSize(0, 100)
	tracker.SetBatchSize(1, 100)
	tracker.SetBatchSize(2, 100)

	// Batch 0: 10% deleted (below threshold)
	for i := 0; i < 10; i++ {
		tracker.RecordDeletion(0)
	}

	// Batch 1: 25% deleted (above threshold)
	for i := 0; i < 25; i++ {
		tracker.RecordDeletion(1)
	}

	// Batch 2: 50% deleted (above threshold)
	for i := 0; i < 50; i++ {
		tracker.RecordDeletion(2)
	}

	// Get fragmented batches with 20% threshold
	fragmented := tracker.GetFragmentedBatches(0.20)

	assert.Len(t, fragmented, 2, "Should have 2 fragmented batches")
	assert.Contains(t, fragmented, 1, "Batch 1 should be fragmented")
	assert.Contains(t, fragmented, 2, "Batch 2 should be fragmented")
	assert.NotContains(t, fragmented, 0, "Batch 0 should not be fragmented")
}

func TestFragmentationTracker_Reset(t *testing.T) {
	tracker := NewFragmentationTracker()

	tracker.SetBatchSize(0, 100)

	// Record deletions
	for i := 0; i < 30; i++ {
		tracker.RecordDeletion(0)
	}

	assert.InDelta(t, 0.30, tracker.GetDensity(0), 0.001, "Density should be 30%")

	// Reset after compaction
	tracker.Reset(0)

	assert.Equal(t, 0.0, tracker.GetDensity(0), "Density should be 0 after reset")
}

func TestFragmentationTracker_Concurrent(t *testing.T) {
	tracker := NewFragmentationTracker()

	tracker.SetBatchSize(0, 1000)

	// Concurrent deletions
	var wg sync.WaitGroup
	numGoroutines := 10
	deletionsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < deletionsPerGoroutine; j++ {
				tracker.RecordDeletion(0)
			}
		}()
	}

	wg.Wait()

	// Total deletions should be numGoroutines * deletionsPerGoroutine
	expectedDeletions := numGoroutines * deletionsPerGoroutine
	density := tracker.GetDensity(0)
	expectedDensity := float64(expectedDeletions) / 1000.0

	assert.InDelta(t, expectedDensity, density, 0.001, "Concurrent deletions should be tracked correctly")
}

func TestFragmentationTracker_MultipleBatches(t *testing.T) {
	tracker := NewFragmentationTracker()

	// Setup multiple batches with different sizes
	tracker.SetBatchSize(0, 100)
	tracker.SetBatchSize(1, 200)
	tracker.SetBatchSize(2, 50)

	// Record deletions
	for i := 0; i < 20; i++ {
		tracker.RecordDeletion(0)
	}
	for i := 0; i < 40; i++ {
		tracker.RecordDeletion(1)
	}
	for i := 0; i < 10; i++ {
		tracker.RecordDeletion(2)
	}

	// Verify densities
	assert.InDelta(t, 0.20, tracker.GetDensity(0), 0.001, "Batch 0 density")
	assert.InDelta(t, 0.20, tracker.GetDensity(1), 0.001, "Batch 1 density")
	assert.InDelta(t, 0.20, tracker.GetDensity(2), 0.001, "Batch 2 density")
}

func TestFragmentationTracker_ZeroSizeBatch(t *testing.T) {
	tracker := NewFragmentationTracker()

	// Don't set batch size (defaults to 0)
	tracker.RecordDeletion(0)

	// Density should be 0 for zero-size batch
	density := tracker.GetDensity(0)
	assert.Equal(t, 0.0, density, "Density should be 0 for zero-size batch")
}

func TestFragmentationTracker_ThresholdEdgeCases(t *testing.T) {
	tracker := NewFragmentationTracker()

	tracker.SetBatchSize(0, 100)
	tracker.SetBatchSize(1, 100)

	// Batch 0: exactly 20% deleted
	for i := 0; i < 20; i++ {
		tracker.RecordDeletion(0)
	}

	// Batch 1: 19% deleted (just below)
	for i := 0; i < 19; i++ {
		tracker.RecordDeletion(1)
	}

	// With threshold 0.20, batch 0 should be included (>=), batch 1 should not
	fragmented := tracker.GetFragmentedBatches(0.20)

	assert.Contains(t, fragmented, 0, "Batch at exactly threshold should be included")
	assert.NotContains(t, fragmented, 1, "Batch below threshold should not be included")
}

func TestFragmentationTracker_UpdateBatchSize(t *testing.T) {
	tracker := NewFragmentationTracker()

	tracker.SetBatchSize(0, 100)

	// Record 10 deletions
	for i := 0; i < 10; i++ {
		tracker.RecordDeletion(0)
	}

	assert.InDelta(t, 0.10, tracker.GetDensity(0), 0.001, "Initial density")

	// Update batch size (e.g., after compaction)
	tracker.SetBatchSize(0, 200)

	// Density should recalculate: 10/200 = 0.05
	assert.InDelta(t, 0.05, tracker.GetDensity(0), 0.001, "Density after size update")
}

func BenchmarkFragmentationTracker_RecordDeletion(b *testing.B) {
	tracker := NewFragmentationTracker()
	tracker.SetBatchSize(0, 1000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.RecordDeletion(0)
	}
}

func BenchmarkFragmentationTracker_GetFragmentedBatches(b *testing.B) {
	tracker := NewFragmentationTracker()

	// Setup 100 batches
	for i := 0; i < 100; i++ {
		tracker.SetBatchSize(i, 1000)
		// Random deletions
		for j := 0; j < i*10; j++ {
			tracker.RecordDeletion(i)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tracker.GetFragmentedBatches(0.20)
	}
}
