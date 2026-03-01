package store

import (
	"context"
	"sort"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Compact performs fragmentation-aware compaction on the dataset.
// It prioritizes hot batches and squashes fragmented ones.
func (d *Dataset) Compact(fragmentedIdxs, hotIdxs []int) error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()

	if len(d.Records) == 0 {
		return nil
	}

	// 1. Determine new order
	// Strategy: [Hot Batches (sorted by heat)] + [Normal Batches] + [Cold/Fragmented (squashed)]

	// Strategy: Sort ALL batches by hits (hotness) descending, then by fragmentation density.
	type batchInfo struct {
		idx   int
		hits  int64
		ratio float64
	}
	infos := make([]batchInfo, len(d.Records))
	for i := range d.Records {
		hits := int64(0)
		ratio := 0.0
		if d.fragmentationTracker != nil {
			f := d.fragmentationTracker.getOrCreateBatch(i)
			hits = f.hits.Load()
			ratio = d.fragmentationTracker.getDensityLocked(i)
		}
		infos[i] = batchInfo{idx: i, hits: hits, ratio: ratio}
	}

	// Sort: higher hits first, then lower fragmentation ratio
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].hits != infos[j].hits {
			return infos[i].hits > infos[j].hits
		}
		return infos[i].ratio < infos[j].ratio
	})

	// 2. Rebuild Records and Track Mapping
	newRecords := make([]arrow.RecordBatch, 0, len(d.Records))
	newTombstones := make(map[int]*qry.Bitset)

	// mappingForIndex: id -> newLocation (for VectorIndex.RemapLocations)
	indexMapping := make(map[uint32]any)

	// Since we have PrimaryIndex (ID -> Location), we can update it by iterating.
	// We need a way to go from (BatchIdx, RowIdx) -> ID.
	// PrimaryIndex is ID -> (BatchIdx, RowIdx). We can invert it once.
	reversePrimary := make(map[Location]string)
	for id, loc := range d.PrimaryIndex {
		// Location is an alias for core.Location. RowLocation is in dataset.go.
		// They are structurally compatible.
		reversePrimary[Location{BatchIdx: loc.BatchIdx, RowIdx: loc.RowIdx}] = id
	}

	for newBatchIdx, info := range infos {
		oldBatchIdx := info.idx
		rec := d.Records[oldBatchIdx]
		tombstones := d.Tombstones[oldBatchIdx]

		if info.ratio > 0.1 && tombstones != nil && tombstones.Count() > 0 {
			// SQUASH: Create new batch without deleted rows
			newRec, rowMapping := d.squashBatch(rec, tombstones)
			newRecords = append(newRecords, newRec)

			// Update mappings for this batch
			for oldRow, newRow := range rowMapping {
				oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: oldRow}
				if id, ok := reversePrimary[oldLoc]; ok {
					if newRow != -1 {
						newLoc := RowLocation{BatchIdx: newBatchIdx, RowIdx: newRow}
						d.PrimaryIndex[id] = newLoc
						if d.Index != nil {
							if vid, ok := d.Index.GetVectorID(oldLoc); ok {
								indexMapping[vid] = Location{BatchIdx: newBatchIdx, RowIdx: newRow}
							}
						}
					} else {
						// Row was squashed (deleted), remove from PrimaryIndex
						delete(d.PrimaryIndex, id)
					}
				}
			}
			// Release old record
			rec.Release()
		} else {
			// MOVE: Just update indices
			newRecords = append(newRecords, rec)
			if tombstones != nil {
				newTombstones[newBatchIdx] = tombstones
			}

			for row := 0; row < int(rec.NumRows()); row++ {
				oldLoc := Location{BatchIdx: oldBatchIdx, RowIdx: row}
				if id, ok := reversePrimary[oldLoc]; ok {
					newLoc := RowLocation{BatchIdx: newBatchIdx, RowIdx: row}
					d.PrimaryIndex[id] = newLoc
					if d.Index != nil {
						if vid, ok := d.Index.GetVectorID(oldLoc); ok {
							indexMapping[vid] = Location{BatchIdx: newBatchIdx, RowIdx: row}
						}
					}
				}
			}
		}
	}

	// 3. Finalize
	d.Records = newRecords
	d.Tombstones = newTombstones

	// Reset fragmentation tracker for the new layout
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
func (d *Dataset) squashBatch(rec arrow.RecordBatch, tombstones *qry.Bitset) (arrow.RecordBatch, map[int]int) {
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
