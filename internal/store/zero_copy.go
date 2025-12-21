package store

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ZeroCopyRecordBatch creates a zero-copy view of a record batch with tombstone filtering.
// Uses Arrow's compute kernels for efficient slicing without deep copies.
func ZeroCopyRecordBatch(mem memory.Allocator, rec arrow.RecordBatch, deleted *Bitset) (arrow.RecordBatch, error) {
	if deleted == nil || deleted.Count() == 0 {
		// No filtering needed - retain and return original
		rec.Retain()
		return rec, nil
	}

	// Build index array of non-deleted rows
	numRows := int(rec.NumRows())
	deletedCount := int(deleted.Count())
	indices := make([]int64, 0, numRows-deletedCount)
	for i := 0; i < numRows; i++ {
		if !deleted.Contains(i) { // Use Contains instead of IsSet
			indices = append(indices, int64(i))
		}
	}

	if len(indices) == 0 {
		// All rows deleted - return empty batch
		return array.NewRecordBatch(rec.Schema(), nil, 0), nil
	}

	// Use Arrow's Take kernel for zero-copy slicing
	indicesArray := array.NewInt64Builder(mem)
	defer indicesArray.Release()
	indicesArray.AppendValues(indices, nil)
	indicesData := indicesArray.NewInt64Array()
	defer indicesData.Release()

	// Take each column
	ctx := context.Background()
	cols := make([]arrow.Array, rec.NumCols())
	for i, col := range rec.Columns() {
		// Use compute.Take for zero-copy slicing
		result, err := compute.Take(ctx, compute.TakeOptions{}, compute.NewDatumWithoutOwning(col), compute.NewDatumWithoutOwning(indicesData))
		if err != nil {
			// Cleanup already created columns
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, err
		}
		cols[i] = result.(*compute.ArrayDatum).MakeArray()
	}

	return array.NewRecordBatch(rec.Schema(), cols, int64(len(indices))), nil
}

// RetainRecordBatch simply retains the record batch for zero-copy access.
// Caller must call Release() when done.
func RetainRecordBatch(rec arrow.RecordBatch) arrow.RecordBatch {
	rec.Retain()
	return rec
}
