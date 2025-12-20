package store

import (
	"errors"
)

// compactionWorker is the background worker (added to VectorStore)
// This file provides the integration methods between VectorStore and CompactionWorker.

// initCompaction initializes the compaction subsystem.
func (vs *VectorStore) initCompaction(cfg CompactionConfig) {
	vs.compactionConfig = cfg
	vs.compactionWorker = NewCompactionWorker(cfg)
	vs.compactionWorker.Start()
}

// stopCompaction stops the compaction worker.
func (vs *VectorStore) stopCompaction() {
	if vs.compactionWorker != nil {
		vs.compactionWorker.Stop()
	}
}

// GetCompactionConfig returns the current compaction configuration.
func (vs *VectorStore) GetCompactionConfig() CompactionConfig {
	return vs.compactionConfig
}

// IsCompactionRunning returns true if the compaction worker is running.
func (vs *VectorStore) IsCompactionRunning() bool {
	if vs.compactionWorker == nil {
		return false
	}
	return vs.compactionWorker.IsRunning()
}

// GetCompactionStats returns compaction statistics.
func (vs *VectorStore) GetCompactionStats() CompactionStats {
	if vs.compactionWorker == nil {
		return CompactionStats{}
	}
	return vs.compactionWorker.Stats()
}

// CompactDataset manually triggers compaction for a specific dataset.
func (vs *VectorStore) CompactDataset(name string) error {
	ds, ok := vs.vectors.Get(name)
	if !ok || ds == nil {
		return errors.New("compaction: dataset not found")
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(ds.Records) < 2 {
		return nil // Nothing to compact
	}

	compacted := compactRecords(ds.Records, vs.compactionConfig.TargetBatchSize)

	// Release old records
	for _, r := range ds.Records {
		r.Release()
	}

	ds.Records = compacted
	return nil
}
