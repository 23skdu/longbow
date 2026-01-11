package store

import (
	"testing"
)

// FuzzFragmentationTracker_DeletionPattern fuzzes deletion patterns
func FuzzFragmentationTracker_DeletionPattern(f *testing.F) {
	// Seed corpus
	f.Add(100, 10)   // 100 size, 10 deletions
	f.Add(1000, 500) // 50% deleted
	f.Add(50, 50)    // 100% deleted
	f.Add(100, 0)    // No deletions
	f.Add(0, 10)     // Zero size

	f.Fuzz(func(t *testing.T, batchSize, deletions int) {
		if batchSize < 0 || deletions < 0 {
			t.Skip("Invalid parameters")
		}
		if batchSize > 100000 || deletions > 100000 {
			t.Skip("Too large")
		}

		tracker := NewFragmentationTracker()
		tracker.SetBatchSize(0, batchSize)

		// Record deletions
		for i := 0; i < deletions; i++ {
			tracker.RecordDeletion(0)
		}

		// Should not panic
		density := tracker.GetDensity(0)

		// Validate density is in valid range
		if density < 0 || density > 1.0 {
			t.Errorf("Invalid density: %f", density)
		}

		// If batch size > 0, density should match calculation
		if batchSize > 0 {
			expected := float64(deletions) / float64(batchSize)
			if expected > 1.0 {
				expected = 1.0 // Cap at 100%
			}
			if density != expected && deletions <= batchSize {
				t.Errorf("Density mismatch: got %f, expected %f", density, expected)
			}
		}
	})
}

// FuzzFragmentationTracker_Threshold fuzzes threshold values
func FuzzFragmentationTracker_Threshold(f *testing.F) {
	// Seed corpus
	f.Add(0.0)
	f.Add(0.20)
	f.Add(0.50)
	f.Add(1.0)
	f.Add(-0.1) // Invalid
	f.Add(1.5)  // Invalid

	f.Fuzz(func(t *testing.T, threshold float64) {
		tracker := NewFragmentationTracker()

		// Setup batches with various densities
		for i := 0; i < 10; i++ {
			tracker.SetBatchSize(i, 100)
			for j := 0; j < i*10; j++ {
				tracker.RecordDeletion(i)
			}
		}

		// Should not panic
		fragmented := tracker.GetFragmentedBatches(threshold)

		// Validate results
		for _, batchIdx := range fragmented {
			density := tracker.GetDensity(batchIdx)
			if threshold >= 0 && threshold <= 1.0 {
				if density < threshold {
					t.Errorf("Batch %d with density %f should not be in fragmented list (threshold %f)",
						batchIdx, density, threshold)
				}
			}
		}
	})
}

// FuzzFragmentationTracker_MultipleBatches fuzzes multiple batch operations
func FuzzFragmentationTracker_MultipleBatches(f *testing.F) {
	// Seed corpus
	f.Add(5, 100, 20)
	f.Add(10, 50, 10)
	f.Add(1, 1000, 500)

	f.Fuzz(func(t *testing.T, numBatches, batchSize, deletionsPerBatch int) {
		if numBatches < 0 || numBatches > 100 {
			t.Skip("Invalid batch count")
		}
		if batchSize < 0 || batchSize > 10000 {
			t.Skip("Invalid batch size")
		}
		if deletionsPerBatch < 0 || deletionsPerBatch > 10000 {
			t.Skip("Invalid deletions")
		}

		tracker := NewFragmentationTracker()

		// Setup batches
		for i := 0; i < numBatches; i++ {
			tracker.SetBatchSize(i, batchSize)
			for j := 0; j < deletionsPerBatch; j++ {
				tracker.RecordDeletion(i)
			}
		}

		// Should not panic
		_ = tracker.GetFragmentedBatches(0.20)

		// Verify all densities are valid
		for i := 0; i < numBatches; i++ {
			density := tracker.GetDensity(i)
			if density < 0 || density > 1.0 {
				t.Errorf("Invalid density for batch %d: %f", i, density)
			}
		}
	})
}

// FuzzFragmentationTracker_ResetPattern fuzzes reset operations
func FuzzFragmentationTracker_ResetPattern(f *testing.F) {
	// Seed corpus
	f.Add(10, 5)
	f.Add(100, 50)
	f.Add(5, 10) // Reset more than exist

	f.Fuzz(func(t *testing.T, numBatches, numResets int) {
		if numBatches < 0 || numBatches > 100 {
			t.Skip("Invalid batch count")
		}
		if numResets < 0 || numResets > 100 {
			t.Skip("Invalid reset count")
		}

		tracker := NewFragmentationTracker()

		// Setup batches
		for i := 0; i < numBatches; i++ {
			tracker.SetBatchSize(i, 100)
			for j := 0; j < 50; j++ {
				tracker.RecordDeletion(i)
			}
		}

		// Reset some batches
		for i := 0; i < numResets; i++ {
			tracker.Reset(i)
		}

		// Should not panic
		_ = tracker.GetFragmentedBatches(0.20)

		// Verify reset batches have 0 density
		for i := 0; i < numResets && i < numBatches; i++ {
			density := tracker.GetDensity(i)
			if density != 0.0 {
				t.Errorf("Reset batch %d should have 0 density, got %f", i, density)
			}
		}
	})
}

// FuzzFragmentationTracker_Combined fuzzes combined operations
func FuzzFragmentationTracker_Combined(f *testing.F) {
	// Seed corpus
	f.Add(10, 100, 30, 0.25)
	f.Add(5, 50, 10, 0.15)

	f.Fuzz(func(t *testing.T, numBatches, batchSize, deletions int, threshold float64) {
		if numBatches < 0 || numBatches > 50 {
			t.Skip("Invalid batch count")
		}
		if batchSize < 0 || batchSize > 5000 {
			t.Skip("Invalid batch size")
		}
		if deletions < 0 || deletions > 5000 {
			t.Skip("Invalid deletions")
		}

		tracker := NewFragmentationTracker()

		// Setup and delete
		for i := 0; i < numBatches; i++ {
			tracker.SetBatchSize(i, batchSize)
			for j := 0; j < deletions; j++ {
				tracker.RecordDeletion(i)
			}
		}

		// Get fragmented batches
		fragmented := tracker.GetFragmentedBatches(threshold)

		// Validate - len() always returns non-negative
		_ = fragmented

		// All fragmented batches should meet threshold
		for _, batchIdx := range fragmented {
			density := tracker.GetDensity(batchIdx)
			if threshold >= 0 && threshold <= 1.0 && density < threshold {
				t.Errorf("Batch %d with density %f below threshold %f", batchIdx, density, threshold)
			}
		}
	})
}
