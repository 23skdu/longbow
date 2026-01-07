package store

import (
	"errors"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
)

// compactionWorker is the background worker (added to VectorStore)
// This file provides the integration methods between VectorStore and CompactionWorker.

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
	start := time.Now()
	defer func() {
		metrics.CompactionDurationSeconds.WithLabelValues(name, "manual").Observe(time.Since(start).Seconds())
	}()
	ds, err := vs.getDataset(name)
	if err != nil || ds == nil {
		return errors.New("compaction: dataset not found")
	}

	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()

	if len(ds.Records) < 2 {
		return nil // Nothing to compact
	}

	// Check for memory headroom
	// Compaction requires approx 2x the size of the data being compacted (old + new) temporarily.
	// We check if we have enough free memory (limit - current) > current_dataset_size
	currentMem := vs.currentMemory.Load()
	maxMem := vs.maxMemory.Load()
	dsSize := ds.SizeBytes.Load()

	if maxMem > 0 && currentMem+dsSize > maxMem {
		// If we are already near limit, compaction might cause OOM.
		// However, compaction is also the way to reduce fragmentation.
		// For safety, we skip if we are dangerously close.
		// Detailed check: headroom vs required
		if float64(maxMem-currentMem) < float64(dsSize)*1.1 {
			metrics.CompactionErrorsTotal.Inc()
			return errors.New("compaction: insufficient memory headroom")
		}
	}

	// 1. Perform Incremental Compaction
	// Returns a NEW slice of records and a remapping table
	// We use vs.mem (the tracked allocator) instead of creating a new one
	newRecords, remapping := compactRecords(vs.mem, ds.Schema, ds.Records, ds.Tombstones, vs.compactionConfig.TargetBatchSize, name)

	if newRecords == nil {
		return nil // No changes needed
	}

	// 2. Fix up Index Locations (if index exists)
	// We need to map old (BatchIdx, RowIdx) -> new (BatchIdx, RowIdx)
	// HNSWIndex stores locations in a slice indexed by VectorID.
	// We iterate locations and update them.
	// We need to convert our internal remapping format to the one HNSW expects.
	if hnsw, ok := ds.Index.(*HNSWIndex); ok {
		_ = hnsw.RemapFromBatchInfo(remapping)
	}

	// 3. Fix up Tombstones (Bitsets)
	// Since we actively filtered tombstones, the new structure should be clean.
	// If any rows were missed (not expected), we would re-map them here.
	newTombstones := make(map[int]*query.Bitset)

	for oldBatchIdx, tombstone := range ds.Tombstones {
		if tombstone == nil || tombstone.Count() == 0 {
			continue
		}

		info, ok := remapping[oldBatchIdx]
		if !ok {
			continue
		}

		newTomb := newTombstones[info.NewBatchIdx]
		rows := tombstone.ToUint32Array()
		for _, oldRowIdx := range rows {
			if int(oldRowIdx) < len(info.NewRowIdxs) {
				newRowIdx := info.NewRowIdxs[oldRowIdx]
				if newRowIdx != -1 {
					if newTomb == nil {
						newTomb = query.NewBitset()
						newTombstones[info.NewBatchIdx] = newTomb
					}
					newTomb.Set(newRowIdx)
				}
			}
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

	// Update BatchNodes for the new record set
	// All these records were either created or retained by this worker on the current node
	currCPU := GetCurrentCPU()
	currNode := -1
	if vs.numaTopology != nil {
		currNode = vs.numaTopology.GetNodeForCPU(currCPU)
	}
	ds.BatchNodes = make([]int, len(newRecords))
	for i := range newRecords {
		ds.BatchNodes[i] = currNode
	}

	return nil
}

// VacuumDataset triggers graph vacuum (CleanupTombstones) for the named dataset.
func (vs *VectorStore) VacuumDataset(name string) error {
	start := time.Now()
	// Using a new metric label "vacuum"
	defer func() {
		metrics.CompactionDurationSeconds.WithLabelValues(name, "vacuum").Observe(time.Since(start).Seconds())
	}()

	ds, err := vs.getDataset(name)
	if err != nil || ds == nil {
		return errors.New("vacuum: dataset not found")
	}

	// Lock dataset metadata to read index safely
	ds.dataMu.RLock()
	index := ds.Index
	ds.dataMu.RUnlock()

	if index == nil {
		return nil
	}

	// Check if this is an ArrowHNSW which supports Vacuum
	if hnsw, ok := index.(*ArrowHNSW); ok {
		// Run Vacuum
		// We pass 0 to scan all nodes. Or we could be smarter?
		// For now, full vacuum.
		pruned := hnsw.CleanupTombstones(0)
		if pruned > 0 {
			metrics.CompactionOperationsTotal.WithLabelValues(name, "vacuum_pruned").Add(float64(pruned))
		}
	}

	return nil
}
