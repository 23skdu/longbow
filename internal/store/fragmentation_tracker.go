package store

import (
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// FragmentationTracker tracks tombstone density per batch for fragmentation-aware compaction
type FragmentationTracker struct {
	mu sync.RWMutex

	// Per-batch tracking
	batches map[int]*batchFragmentation

	// Dataset name for metrics
	datasetName string
}

// batchFragmentation tracks fragmentation for a single batch
type batchFragmentation struct {
	size      atomic.Int64 // Total records in batch
	deletions atomic.Int64 // Deleted records count
	hits      atomic.Int64 // Number of accesses (for hot/cold tracking)
}

// NewFragmentationTracker creates a new fragmentation tracker
func NewFragmentationTracker() *FragmentationTracker {
	return &FragmentationTracker{
		batches:     make(map[int]*batchFragmentation),
		datasetName: "default",
	}
}

// SetDatasetName sets the dataset name for metrics
func (f *FragmentationTracker) SetDatasetName(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.datasetName = name
}

// SetBatchSize sets the total size of a batch
func (f *FragmentationTracker) SetBatchSize(batchIdx, size int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch := f.getOrCreateBatch(batchIdx)
	batch.size.Store(int64(size))

	// Update metrics
	f.updateMetrics(batchIdx, batch)
}

// RecordDeletion records a deletion in the specified batch
func (f *FragmentationTracker) RecordDeletion(batchIdx int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch := f.getOrCreateBatch(batchIdx)
	batch.deletions.Add(1)

	// Update metrics
	f.updateMetrics(batchIdx, batch)
}

// RecordAccess records an access (hit) for vectors in this batch
func (f *FragmentationTracker) RecordAccess(batchIdx int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch := f.getOrCreateBatch(batchIdx)
	batch.hits.Add(1)
}

// GetDensity returns the tombstone density for a batch (0.0 to 1.0)
func (f *FragmentationTracker) GetDensity(batchIdx int) float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	batch, exists := f.batches[batchIdx]
	if !exists {
		return 0.0
	}

	size := batch.size.Load()
	if size == 0 {
		return 0.0
	}

	deletions := batch.deletions.Load()
	density := float64(deletions) / float64(size)

	// Cap at 1.0 (100%)
	if density > 1.0 {
		density = 1.0
	}

	return density
}

// GetFragmentedBatches returns batch indices where density >= threshold or read amplification is high
func (f *FragmentationTracker) GetFragmentedBatches(threshold float64) []int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	fragmented := []int{}

	for batchIdx := range f.batches {
		density := f.getDensityLocked(batchIdx)
		// Read Amplification Factor: 1 / (1 - density)
		// If density is 0.5 (50% deleted), we read 2x as much data as needed.
		// If density is 0.8, we read 5x as much data.
		readAmp := 1.0
		if density < 1.0 {
			readAmp = 1.0 / (1.0 - density)
		}

		// Threshold for read amplification (e.g., 2.0x)
		if density >= threshold || readAmp >= 2.0 {
			fragmented = append(fragmented, batchIdx)
		}
	}

	// Update fragmented count metric
	metrics.CompactionFragmentedBatches.WithLabelValues(f.datasetName).Set(float64(len(fragmented)))

	return fragmented
}

// GetHotBatches returns batch indices that are frequently accessed
func (f *FragmentationTracker) GetHotBatches(minHits int64) []int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	hot := []int{}
	for batchIdx, batch := range f.batches {
		if batch.hits.Load() >= minHits {
			hot = append(hot, batchIdx)
		}
	}
	return hot
}

// Reset resets the fragmentation tracking for a batch (e.g., after compaction)
func (f *FragmentationTracker) Reset(batchIdx int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	batch, exists := f.batches[batchIdx]
	if !exists {
		return
	}

	batch.deletions.Store(0)

	// Update metrics
	f.updateMetrics(batchIdx, batch)
}

// ResetAll resets tracking for all batches
func (f *FragmentationTracker) ResetAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = make(map[int]*batchFragmentation)
}

// getOrCreateBatch gets or creates a batch fragmentation tracker (must hold lock)
func (f *FragmentationTracker) getOrCreateBatch(batchIdx int) *batchFragmentation {
	batch, exists := f.batches[batchIdx]
	if !exists {
		batch = &batchFragmentation{}
		f.batches[batchIdx] = batch
	}
	return batch
}

// getDensityLocked returns density without acquiring lock (must hold read lock)
func (f *FragmentationTracker) getDensityLocked(batchIdx int) float64 {
	batch, exists := f.batches[batchIdx]
	if !exists {
		return 0.0
	}

	size := batch.size.Load()
	if size == 0 {
		return 0.0
	}

	deletions := batch.deletions.Load()
	density := float64(deletions) / float64(size)

	if density > 1.0 {
		density = 1.0
	}

	return density
}

// updateMetrics updates Prometheus metrics for a batch (must hold lock)
func (f *FragmentationTracker) updateMetrics(batchIdx int, batch *batchFragmentation) {
	size := batch.size.Load()
	if size == 0 {
		return
	}

	deletions := batch.deletions.Load()
	density := float64(deletions) / float64(size)

	if density > 1.0 {
		density = 1.0
	}

	// Update density metric
	batchLabel := string(rune('0' + batchIdx)) // Simple label for now
	metrics.CompactionTombstoneDensity.WithLabelValues(f.datasetName, batchLabel).Set(density)
}
