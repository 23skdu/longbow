package store

import (
	"github.com/23skdu/longbow/internal/store/types"
	"context"
	"sort"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Compact performs fragmentation-aware compaction on the dataset.
// It prioritizes hot batches and squashes fragmented ones.
// This implementation is asynchronous and non-blocking for the majority of its execution.
func (d *Dataset) Compact(fragmentedIdxs, hotIdxs []int) error {
	// 1. Snapshot current state for background processing
	d.dataMu.RLock()
	if len(d.Records) == 0 {
		d.dataMu.RUnlock()
		return nil
	}
	oldRecords := make([]arrow.RecordBatch, len(d.Records))
	for i, r := range d.Records {
		r.Retain()
		oldRecords[i] = r
	}
	oldTombstones := make(map[int]*types.Bitset)
	for i, ts := range d.Tombstones {
		if ts != nil {
			oldTombstones[i] = ts.Clone()
		}
	}
	// Copy PrimaryIndex for mapping discovery
	primarySnapshot := make(map[string]RowLocation)
	for id, loc := range d.PrimaryIndex {
		primarySnapshot[id] = loc
	}
	d.dataMu.RUnlock()

	defer func() {
		for _, r := range oldRecords {
			r.Release()
		}
	}()

	// 2. Identify strategy (in background)
	type batchInfo struct {
		idx   int
		hits  int64
		ratio float64
	}
	infos := make([]batchInfo, len(oldRecords))
	for i := range oldRecords {
		hits := int64(0)
		ratio := 0.0
		if d.fragmentationTracker != nil {
			f := d.fragmentationTracker.getOrCreateBatch(i)
			hits = f.hits.Load()
			ratio = d.fragmentationTracker.getDensityLocked(i)
		}
		infos[i] = batchInfo{idx: i, hits: hits, ratio: ratio}
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].hits != infos[j].hits {
			return infos[i].hits > infos[j].hits
		}
		return infos[i].ratio < infos[j].ratio
	})

	// 3. Build new state (heavy lifting outside lock)
	newRecords := make([]arrow.RecordBatch, 0, len(oldRecords))
	newTombstones := make(map[int]*types.Bitset)
	indexMapping := make(map[uint32]any)
	
	oldToNewBatchIdx := make(map[int]int)
	for i, info := range infos {
		oldToNewBatchIdx[info.idx] = i
	}

	reversePrimary := make(map[Location]string)
	for id, loc := range primarySnapshot {
		reversePrimary[Location{BatchIdx: loc.BatchIdx, RowIdx: loc.RowIdx}] = id
	}

	for newBatchIdx, info := range infos {
		oldBatchIdx := info.idx
		rec := oldRecords[oldBatchIdx]
		tombstones := oldTombstones[oldBatchIdx]

		if info.ratio > 0.1 && tombstones != nil && tombstones.Count() > 0 {
			newRec, rowMapping := d.squashBatch(rec, tombstones)
			newRecords = append(newRecords, newRec)

			for oldRow, newRow := range rowMapping {
				oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: oldRow}
				if _, ok := reversePrimary[oldLoc]; ok {
					if newRow != -1 {
						if d.Index != nil {
							if vid, ok := d.Index.GetVectorID(oldLoc); ok {
								indexMapping[vid] = Location{BatchIdx: newBatchIdx, RowIdx: newRow}
							}
						}
					}
				}
			}
		} else {
			rec.Retain()
			newRecords = append(newRecords, rec)
			if tombstones != nil {
				newTombstones[newBatchIdx] = tombstones.Clone()
			}
			for row := 0; row < int(rec.NumRows()); row++ {
				oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: row}
				if _, ok := reversePrimary[oldLoc]; ok {
					if d.Index != nil {
						if vid, ok := d.Index.GetVectorID(oldLoc); ok {
							indexMapping[vid] = Location{BatchIdx: newBatchIdx, RowIdx: row}
						}
					}
				}
			}
		}
	}

	// 4. Atomic Swap (Short Lock)
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	// Handle records added during compaction
	addedDuring := d.Records[len(oldRecords):]
	
	// Release old records that were part of compaction
	for _, r := range d.Records[:len(oldRecords)] {
		r.Release()
	}
	
	d.Records = append(newRecords, addedDuring...)
	d.Tombstones = newTombstones

	// Update PrimaryIndex for remapped IDs
	for _, locAny := range indexMapping {
		loc := locAny.(Location)
		// Find the string ID from the snapshot reverse map
		oldBatchIdx := infos[loc.BatchIdx].idx
		oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: loc.RowIdx}
		if id, ok := reversePrimary[oldLoc]; ok {
			d.PrimaryIndex[id] = RowLocation{BatchIdx: loc.BatchIdx, RowIdx: loc.RowIdx}
		}
	}

	// Remove deleted IDs from PrimaryIndex
	for oldBatchIdx, ts := range oldTombstones {
		if ts == nil {
			continue
		}
		// Find which new batch this corresponds to
		newBatchIdx, ok := oldToNewBatchIdx[oldBatchIdx]
		if !ok {
			continue
		}

		// Re-check deletions for squashed rows
		if infos[newBatchIdx].ratio > 0.1 {
			// This was a squashed batch, rows marked for deletion are gone
			for row := 0; row < int(oldRecords[oldBatchIdx].NumRows()); row++ {
				if ts.Contains(row) {
					oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: row}
					if id, ok := reversePrimary[oldLoc]; ok {
						delete(d.PrimaryIndex, id)
					}
				}
			}
		}
	}

	if d.fragmentationTracker != nil {
		d.fragmentationTracker.ResetAll()
	}

	if d.Index != nil {
		if err := d.Index.RemapLocations(context.Background(), indexMapping); err != nil {
			return err
		}
	}

	metrics.CompactionRunsTotal.WithLabelValues(d.Name).Inc()
	return nil
}


// squashBatch creates a new RecordBatch by removing rows marked in the bitset.
// It returns the new batch and a mapping of oldRowIdx -> newRowIdx.
func (d *Dataset) squashBatch(rec arrow.RecordBatch, tombstones *types.Bitset) (arrow.RecordBatch, map[int]int) {
	numRows := int(rec.NumRows())
	keepRows := []int{}
	mapping := make(map[int]int)

	for i := 0; i < numRows; i++ {
		if tombstones == nil || !tombstones.Contains(i) {
			mapping[i] = len(keepRows)
			keepRows = append(keepRows, i)
		} else {
			mapping[i] = -1 // Deleted
		}
	}

	if len(keepRows) == numRows {
		rec.Retain()
		return rec, mapping
	}

	// Build new columns
	newCols := make([]arrow.Array, rec.NumCols())
	pool := memory.NewGoAllocator()

	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		newCols[i] = d.filterArray(col, keepRows, pool)
	}

	newRec := array.NewRecordBatch(rec.Schema(), newCols, int64(len(keepRows)))
	for _, col := range newCols {
		col.Release()
	}

	return newRec, mapping
}

func (d *Dataset) filterArray(arr arrow.Array, indices []int, pool memory.Allocator) arrow.Array {
	switch a := arr.(type) {
	case *array.Float32:
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		for _, idx := range indices {
			if a.IsNull(idx) {
				b.AppendNull()
			} else {
				b.Append(a.Value(idx))
			}
		}
		return b.NewArray()
	case *array.Uint32:
		b := array.NewUint32Builder(pool)
		defer b.Release()
		for _, idx := range indices {
			if a.IsNull(idx) {
				b.AppendNull()
			} else {
				b.Append(a.Value(idx))
			}
		}
		return b.NewArray()
	case *array.Int64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		for _, idx := range indices {
			if a.IsNull(idx) {
				b.AppendNull()
			} else {
				b.Append(a.Value(idx))
			}
		}
		return b.NewArray()
	case *array.String:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		for _, idx := range indices {
			if a.IsNull(idx) {
				b.AppendNull()
			} else {
				b.Append(a.Value(idx))
			}
		}
		return b.NewArray()
	}

	// Fallback to simple Slice if indices are contiguous (best effort)
	if len(indices) > 0 {
		return array.NewSlice(arr, int64(indices[0]), int64(indices[len(indices)-1]+1))
	}
	return array.NewSlice(arr, 0, 0)
}
