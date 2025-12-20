package store

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// EnsureTimestampZeroCopy ensures the record has a timestamp column, adding one if missing.
// This is the optimized zero-copy version that:
// 1. Returns original record immediately if timestamp exists (no work)
// 2. Pre-allocates timestamp builder with Reserve() for single allocation
// 3. Uses AppendValues for batch append instead of per-row Append
// 4. Retains existing columns for proper ref-counting (zero-copy)
// 5. Creates new RecordBatch with existing column references
func EnsureTimestampZeroCopy(mem memory.Allocator, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	// Fast path: if timestamp already exists, just retain and return
	if rec.Schema().HasField("timestamp") {
		rec.Retain()
		return rec, nil
	}

	numRows := rec.NumRows()
	if int(rec.NumCols()) != rec.Schema().NumFields() {
		return nil, fmt.Errorf("EnsureTimestampZeroCopy: columns/fields mismatch: cols=%d, fields=%d", rec.NumCols(), rec.Schema().NumFields())
	}

	// Build new schema with timestamp field
	fields := rec.Schema().Fields()
	tsField := arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false}
	fields = append(fields, tsField)
	meta := rec.Schema().Metadata()
	newSchema := arrow.NewSchema(fields, &meta)

	// Pre-allocate timestamp builder with exact capacity (single allocation)
	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer bldr.Release()

	// Reserve exact capacity - avoids reallocations during append
	bldr.Reserve(int(numRows))

	// Create timestamp value once
	now := time.Now()
	ts, _ := arrow.TimestampFromTime(now, arrow.Nanosecond)

	// Batch append: create slice of identical timestamps and append in one call
	if numRows > 0 {
		tsValues := make([]arrow.Timestamp, numRows)
		for i := range tsValues {
			tsValues[i] = ts
		}
		bldr.AppendValues(tsValues, nil) // nil validity = all valid
	}

	tsArr := bldr.NewArray()
	defer tsArr.Release()

	// Zero-copy: create new columns slice with existing column references
	// Retain each column to ensure ref-counting is correct
	numCols := int(rec.NumCols())
	cols := make([]arrow.Array, numCols+1)
	for i := 0; i < numCols; i++ {
		col := rec.Column(i)
		col.Retain() // Increment ref count for zero-copy safety
		cols[i] = col
	}

	// Add timestamp column (already has ref count of 1 from NewArray)
	tsArr.Retain()
	cols[numCols] = tsArr

	// Create new record batch with existing column references (zero-copy)
	newRec := array.NewRecordBatch(newSchema, cols, numRows)

	// Release our references (NewRecordBatch retained them)
	for i := 0; i < numCols; i++ {
		cols[i].Release()
	}
	cols[numCols].Release()

	return newRec, nil
}

// ensureTimestampOld is the original implementation for benchmark comparison.
// It uses per-row Append which is slower for large records.
func ensureTimestampOld(mem memory.Allocator, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.Schema().HasField("timestamp") {
		rec.Retain()
		return rec, nil
	}

	// Create new schema
	fields := rec.Schema().Fields()
	tsField := arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: false}
	fields = append(fields, tsField)
	meta := rec.Schema().Metadata()
	newSchema := arrow.NewSchema(fields, &meta)

	// Create timestamp column - per-row append (slower)
	bldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer bldr.Release()

	now := time.Now()
	ts, _ := arrow.TimestampFromTime(now, arrow.Nanosecond)

	// Per-row append - this is the slow path
	for i := 0; i < int(rec.NumRows()); i++ {
		bldr.Append(ts)
	}
	tsArr := bldr.NewArray()
	defer tsArr.Release()

	// Build new columns
	numCols := int(rec.NumCols())
	cols := make([]arrow.Array, numCols+1)
	for i := 0; i < numCols; i++ {
		cols[i] = rec.Column(i)
	}
	cols[numCols] = tsArr

	newRec := array.NewRecordBatch(newSchema, cols, rec.NumRows())
	return newRec, nil
}
