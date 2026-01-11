package store

// GetFragmentedBatches returns batches exceeding the fragmentation threshold
func (d *Dataset) GetFragmentedBatches(threshold float64) []int {
	if d.fragmentationTracker == nil {
		return nil
	}
	return d.fragmentationTracker.GetFragmentedBatches(threshold)
}

// RecordBatchDeletion records a deletion in a batch for fragmentation tracking
func (d *Dataset) RecordBatchDeletion(batchIdx int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.RecordDeletion(batchIdx)
	}
}

// UpdateBatchSize updates the size of a batch for fragmentation tracking
func (d *Dataset) UpdateBatchSize(batchIdx, size int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.SetBatchSize(batchIdx, size)
	}
}

// ResetBatchFragmentation resets fragmentation tracking for a batch (after compaction)
func (d *Dataset) ResetBatchFragmentation(batchIdx int) {
	if d.fragmentationTracker != nil {
		d.fragmentationTracker.Reset(batchIdx)
	}
}
