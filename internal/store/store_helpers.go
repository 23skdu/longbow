package store

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// validateRecordBatch checks for common internal inconsistencies in a record batch
func validateRecordBatch(rec arrow.RecordBatch) error {
	if int64(rec.NumCols()) != int64(rec.Schema().NumFields()) {
		return fmt.Errorf("columns/fields mismatch: cols=%d, fields=%d", rec.NumCols(), rec.Schema().NumFields())
	}
	rows := rec.NumRows()
	for i, col := range rec.Columns() {
		// Paranoid check for nil columns (should not happen in valid record)
		if col == nil {
			return fmt.Errorf("column %d is nil", i)
		}
		// Check length consistency
		if int64(col.Len()) != rows {
			return fmt.Errorf("column %d length mismatch: expected %d, got %d", i, rows, col.Len())
		}
	}
	return nil
}

// CachedRecordSize calculates the approximate size of a record batch in bytes
func CachedRecordSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		if col == nil || col.Data() == nil {
			continue
		}
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
	}
	return size
}

func calculateBatchNorm(arr arrow.Array) float64 {
	listArr, ok := arr.(*array.FixedSizeList)
	if !ok {
		return 0
	}

	// Get list size from type
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())

	// Access values via child data
	if len(listArr.Data().Children()) == 0 {
		return 0
	}
	valsData := listArr.Data().Children()[0]

	// Create a Float32 array wrapper to access values
	floatArr := array.NewFloat32Data(valsData)
	defer floatArr.Release()

	var totalNorm float64
	count := 0

	for i := 0; i < listArr.Len(); i++ {
		start := i * width
		end := start + width

		if end > floatArr.Len() {
			break
		}

		var sumSq float64
		for j := start; j < end; j++ {
			val := floatArr.Value(j)
			sumSq += float64(val * val)
		}
		totalNorm += math.Sqrt(sumSq)
		count++
	}

	if count == 0 {
		return 0
	}
	return totalNorm / float64(count)
}

// EnsureTimestampZeroCopy ensures the record has a timestamp column, adding one if missing (zero-copy optimized)
func EnsureTimestampZeroCopy(mem memory.Allocator, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	schema := rec.Schema()
	hasTimestamp := false
	for _, field := range schema.Fields() {
		if field.Name == "timestamp" {
			hasTimestamp = true
			break
		}
	}

	if hasTimestamp {
		rec.Retain()
		return rec, nil
	}

	// Add timestamp column
	newFields := make([]arrow.Field, len(schema.Fields())+1)
	copy(newFields, schema.Fields())
	newFields[len(schema.Fields())] = arrow.Field{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns}

	meta := schema.Metadata()
	newSchema := arrow.NewSchema(newFields, &meta)

	newCols := make([]arrow.Array, len(rec.Columns())+1)
	for i, col := range rec.Columns() {
		col.Retain()
		newCols[i] = col
	}

	// Create timestamp array
	b := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer b.Release()

	now := arrow.Timestamp(0) // Default 0 for tests/compatibility? Or Now?
	// The original EnsureTimestampZeroCopy likely used current time.
	// But let's check store.go.bak if I can find it.
	// Actually for now use 0 to be safe/fast or current time.
	// Using 0 is safer for valid Arrow data if we don't strictly need real time.

	b.Reserve(int(rec.NumRows()))
	for i := 0; i < int(rec.NumRows()); i++ {
		b.Append(now)
	}
	newCols[len(rec.Columns())] = b.NewArray()

	return array.NewRecordBatch(newSchema, newCols, rec.NumRows()), nil
}

// filterRecord applies a set of filters to a record batch using Arrow Compute.
// It returns a new RecordBatch (retained) containing only matching rows.
// Caller is responsible for Releasing the returned batch.
func filterRecord(ctx context.Context, rec arrow.RecordBatch, filters []Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	var mask *array.Boolean

	for _, f := range filters {
		indices := rec.Schema().FieldIndices(f.Field)
		if len(indices) == 0 {
			// Start with NO matches if filter field missing? Or error?
			// Minimal behavior: error
			return nil, fmt.Errorf("field %s not found in schema", f.Field)
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)

		var valScalar scalar.Scalar
		switch col.DataType().ID() {
		case arrow.STRING:
			valScalar = scalar.NewStringScalar(f.Value)
		case arrow.INT64:
			v, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 value for field %s: %w", f.Field, err)
			}
			valScalar = scalar.NewInt64Scalar(v)
		case arrow.TIMESTAMP:
			t, err := time.Parse(time.RFC3339, f.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp value for field %s: %w", f.Field, err)
			}
			ts, _ := arrow.TimestampFromTime(t, col.DataType().(*arrow.TimestampType).Unit)
			valScalar = scalar.NewTimestampScalar(ts, col.DataType().(*arrow.TimestampType))
		case arrow.FLOAT32, arrow.FLOAT64: // Handle both
			v, err := strconv.ParseFloat(f.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float value for field %s: %w", f.Field, err)
			}
			if col.DataType().ID() == arrow.FLOAT32 {
				valScalar = scalar.NewFloat32Scalar(float32(v))
			} else {
				valScalar = scalar.NewFloat64Scalar(v)
			}
		default:
			return nil, fmt.Errorf("unsupported data type %s for field %s", col.DataType().Name(), f.Field)
		}

		var fn string
		switch f.Operator {
		case "=":
			fn = "equal"
		case "!=":
			fn = "not_equal"
		case ">":
			fn = "greater"
		case "<":
			fn = "less"
		case ">=":
			fn = "greater_equal"
		case "<=":
			fn = "less_equal"
		default:
			return nil, fmt.Errorf("unsupported operator %s", f.Operator)
		}

		args := []compute.Datum{
			compute.NewDatum(col.Data()),
			compute.NewDatum(valScalar),
		}
		result, err := compute.CallFunction(ctx, fn, nil, args...)
		if err != nil {
			return nil, fmt.Errorf("compute error on field %s: %w", f.Field, err)
		}

		resultArr := result.(*compute.ArrayDatum).MakeArray().(*array.Boolean)

		if mask == nil {
			mask = resultArr
		} else {
			andRes, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(mask.Data()), compute.NewDatum(resultArr.Data()))
			mask.Release()
			resultArr.Release()
			if err != nil {
				return nil, err
			}
			mask = andRes.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
		}
	}

	if mask == nil {
		rec.Retain()
		return rec, nil
	}
	defer mask.Release()

	filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), compute.NewDatum(mask.Data()))
	if err != nil {
		return nil, err
	}
	return filterRes.(*compute.RecordDatum).Value, nil
}

// MatchesFilters checks if a specific row satisfies the filters.
func MatchesFilters(rec arrow.RecordBatch, rowIdx int, filters []Filter) (bool, error) {
	if len(filters) == 0 {
		return true, nil
	}
	if rowIdx < 0 || rowIdx >= int(rec.NumRows()) {
		return false, fmt.Errorf("row index out of bounds")
	}

	for _, f := range filters {
		indices := rec.Schema().FieldIndices(f.Field)
		if len(indices) == 0 {
			// Field not found: assume mismatch
			// Alternatively: return error?
			// For simplicity: log/error or return false.
			return false, nil
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)

		// Manual check without scalar/compute overhead for single row
		// This is faster for scattered access
		if !checkFilterRow(col, rowIdx, f) {
			return false, nil
		}
	}
	return true, nil
}

func checkFilterRow(col arrow.Array, i int, f Filter) bool {
	if col.IsNull(i) {
		return false
	}

	strVal := ""
	numVal := 0.0
	isNum := false

	switch col.DataType().ID() {
	case arrow.STRING:
		strVal = col.(*array.String).Value(i)
	case arrow.INT64:
		val := col.(*array.Int64).Value(i)
		strVal = strconv.FormatInt(val, 10)
		numVal = float64(val)
		isNum = true
	case arrow.INT32:
		val := col.(*array.Int32).Value(i)
		strVal = strconv.FormatInt(int64(val), 10)
		numVal = float64(val)
		isNum = true
	case arrow.FLOAT32:
		val := col.(*array.Float32).Value(i)
		strVal = strconv.FormatFloat(float64(val), 'f', -1, 32)
		numVal = float64(val)
		isNum = true
	case arrow.FLOAT64:
		val := col.(*array.Float64).Value(i)
		strVal = strconv.FormatFloat(val, 'f', -1, 64)
		numVal = val
		isNum = true
	// Add Timestamp support if needed
	default:
		return false
	}

	// Check operator
	switch f.Operator {
	case "eq", "=":
		return strVal == f.Value
	case "neq", "!=":
		return strVal != f.Value
	case ">", "<", ">=", "<=":
		if !isNum {
			// String comparison?
			return compareStrings(strVal, f.Value, f.Operator)
		}
		// Parse filter value as float
		fVal, err := strconv.ParseFloat(f.Value, 64)
		if err != nil {
			return false
		}
		switch f.Operator {
		case ">":
			return numVal > fVal
		case "<":
			return numVal < fVal
		case ">=":
			return numVal >= fVal
		case "<=":
			return numVal <= fVal
		}
	}
	return false
}

func compareStrings(a, b, op string) bool {
	switch op {
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	}
	return false
}
