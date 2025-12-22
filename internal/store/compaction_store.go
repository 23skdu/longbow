package store

import (
	"errors"
)

// compactionWorker is the background worker (added to VectorStore)
// This file provides the integration methods between VectorStore and CompactionWorker.

// initCompaction initializes the compaction subsystem.
func (vs *VectorStore) initCompaction(cfg CompactionConfig) {
	vs.compactionConfig = cfg
	vs.compactionWorker = NewCompactionWorker(vs, cfg)
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
	ds, err := vs.getDataset(name)
	if err != nil || ds == nil {
		return errors.New("compaction: dataset not found")
	}

	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	if len(ds.Records) < 2 {
		return nil // Nothing to compact
	}

	// 1. Perform Incremental Compaction
	// Returns a NEW slice of records and a remapping table
	newRecords, remapping, err := compactRecords(ds.Records, vs.compactionConfig.TargetBatchSize)
	if err != nil {
		return err // e.g. nothing to compact
	}
	if newRecords == nil {
		return nil // No changes needed
	}

	// 2. Fix up Index Locations (if index exists)
	// We need to map old (BatchIdx, RowIdx) -> new (BatchIdx, RowIdx)
	// HNSWIndex stores locations in a slice indexed by VectorID.
	// We iterate locations and update them.
	// We need to convert our internal remapping format to the one HNSW expects.
	if hnsw, ok := ds.Index.(*HNSWIndex); ok {
		// Convert compaction.go's batchRemapInfo to hnsw.go's BatchRemapInfo
		// Currently they are identical but in different files/types if not exported.
		// Since they are in the same package 'store', we can use the same type if we define it nicely.
		// I defined BatchRemapInfo in hnsw.go just now. The one in compaction.go is unexported 'batchRemapInfo'.
		// Map compaction.batchRemapInfo -> hnsw.BatchRemapInfo
		hnswRemap := make(map[int]BatchRemapInfo, len(remapping))
		for oldIdx, info := range remapping {
			hnswRemap[oldIdx] = BatchRemapInfo{
				NewBatchIdx: info.NewBatchIdx,
				RowOffset:   info.RowOffset,
			}
		}
		hnsw.RemapLocations(hnswRemap)
	}

	// 3. Fix up Tombstones (Bitsets)
	// We need to merge bitsets for merged batches and shift keys for others.
	newTombstones := make(map[int]*Bitset)

	// Create bitsets for new batches if needed
	for i := range newRecords {
		// Initialize empty bitsets for new structure
		// Often tombstones are sparse, so we create on demand?
		// But here we might be merging existing deletions.
		_ = i
	}

	// Iterate over OLD batches to merge tombstones
	for oldBatchIdx, tombstone := range ds.Tombstones {
		if tombstone == nil || tombstone.Count() == 0 {
			continue
		}

		info, ok := remapping[oldBatchIdx]
		if !ok {
			// Dropped batch? Should not happen if compactRecords keeps everything.
			continue
		}

		// Get or create new tombstone
		newTomb := newTombstones[info.NewBatchIdx]
		if newTomb == nil {
			newTomb = NewBitset()
			newTombstones[info.NewBatchIdx] = newTomb
		}

		// Merge bits with offset
		// Bitset is roaring wrapper. We need to iterate and add.
		// Iterate over set bits
		iter := tombstone.bitmap.Iterator()
		for iter.HasNext() {
			oldRowIdx := iter.Next()
			newRowIdx := int(oldRowIdx) + info.RowOffset
			newTomb.Set(newRowIdx)
		}
	}

	// 4. Swap Data
	// Release old records that were NOT reused
	// compactRecords retains reused ones and creates new ones.
	// But we need to be careful with ref counting.
	// compactRecords implementation:
	// - "Copy untouched": calls Retain().
	// - "Merge": creates new batch (ref 1).
	// So newRecords owns one ref for everything.
	// We currently hold ds.Records. We should Release old ones.
	for _, r := range ds.Records {
		r.Release()
	}

	ds.Records = newRecords
	ds.Tombstones = newTombstones

	return nil
}
