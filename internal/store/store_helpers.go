package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

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

	b := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer b.Release()

	now := arrow.Timestamp(0)
	b.Reserve(int(rec.NumRows()))
	for i := 0; i < int(rec.NumRows()); i++ {
		b.Append(now)
	}
	newCols[len(rec.Columns())] = b.NewArray()

	return array.NewRecordBatch(newSchema, newCols, rec.NumRows()), nil
}

// MatchesFilters checks if a specific row satisfies the filters.
func MatchesFilters(rec arrow.RecordBatch, rowIdx int, filters []query.Filter) (bool, error) {
	if len(filters) == 0 {
		return true, nil
	}
	if rowIdx < 0 || rowIdx >= int(rec.NumRows()) {
		return false, fmt.Errorf("row index out of bounds")
	}

	for _, f := range filters {
		indices := rec.Schema().FieldIndices(f.Field)
		if len(indices) == 0 {
			return false, nil
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)

		match := checkFilterRow(col, rowIdx, &f)
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func checkFilterRow(col arrow.Array, i int, f *query.Filter) bool {
	if col.IsNull(i) {
		return false
	}

	switch c := col.(type) {
	case *array.String:
		val := c.Value(i)
		switch f.Operator {
		case "eq", "=":
			return val == f.Value
		case "neq", "!=":
			return val != f.Value
		case ">", "<", ">=", "<=":
			return compareStrings(val, f.Value, f.Operator)
		}
	case *array.Int64:
		val := c.Value(i)
		fVal, err := strconv.ParseInt(f.Value, 10, 64)
		if err != nil {
			return false
		}
		switch f.Operator {
		case "eq", "=":
			return val == fVal
		case "neq", "!=":
			return val != fVal
		case ">":
			return val > fVal
		case "<":
			return val < fVal
		case ">=":
			return val >= fVal
		case "<=":
			return val <= fVal
		}
	case *array.Int32:
		val := c.Value(i)
		fVal, err := strconv.ParseInt(f.Value, 10, 32)
		if err != nil {
			return false
		}
		switch f.Operator {
		case "eq", "=":
			return int64(val) == fVal
		case "neq", "!=":
			return int64(val) != fVal
		case ">":
			return int64(val) > fVal
		case "<":
			return int64(val) < fVal
		case ">=":
			return int64(val) >= fVal
		case "<=":
			return int64(val) <= fVal
		}
	case *array.Float32:
		val := c.Value(i)
		fVal, err := strconv.ParseFloat(f.Value, 32)
		if err != nil {
			return false
		}
		fv := float32(fVal)
		switch f.Operator {
		case "eq", "=":
			return val == fv
		case "neq", "!=":
			return val != fv
		case ">":
			return val > fv
		case "<":
			return val < fv
		case ">=":
			return val >= fv
		case "<=":
			return val <= fv
		}
	case *array.Float64:
		val := c.Value(i)
		fVal, err := strconv.ParseFloat(f.Value, 64)
		if err != nil {
			return false
		}
		switch f.Operator {
		case "eq", "=":
			return val == fVal
		case "neq", "!=":
			return val != fVal
		case ">":
			return val > fVal
		case "<":
			return val < fVal
		case ">=":
			return val >= fVal
		case "<=":
			return val <= fVal
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

func extractVectorFromCol(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" || field.Name == "embedding" {
			vecCol = rec.Column(i)
			break
		}
	}
	if vecCol == nil {
		return nil, fmt.Errorf("vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil, fmt.Errorf("invalid vector column type")
	}

	values := listArr.ListValues().(*array.Float32).Float32Values()
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())

	start := rowIdx * width
	end := start + width
	if start < 0 || end > len(values) {
		return nil, fmt.Errorf("index out of bounds")
	}

	vec := make([]float32, width)
	copy(vec, values[start:end])
	return vec, nil
}

// filterRecord applies filters to a batch using query.VectorizedFilter.
func filterRecord(ctx context.Context, mem memory.Allocator, rec arrow.RecordBatch, filters []query.Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	vf := query.NewVectorizedFilter(mem)
	return vf.Apply(ctx, rec, filters)
}

// filterRecordWithMask applies a pre-computed boolean mask to filter the record batch.
func filterRecordWithMask(ctx context.Context, _ memory.Allocator, rec arrow.RecordBatch, mask *array.Boolean) (arrow.RecordBatch, error) {
	if mask == nil {
		rec.Retain()
		return rec, nil
	}
	// compute.Filter expects a Datum for the selection
	maskDatum := compute.NewDatum(mask.Data())
	filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), maskDatum)
	if err != nil {
		return nil, err
	}
	return filterRes.(*compute.RecordDatum).Value, nil
}

// castRecordToSchema aligns a record batch to the target schema.
// For now, it performs checking and basic re-ordering.
func castRecordToSchema(mem memory.Allocator, rec arrow.RecordBatch, targetSchema *arrow.Schema) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(targetSchema) {
		rec.Retain()
		return rec, nil
	}

	cols := make([]arrow.Array, targetSchema.NumFields())
	for i, field := range targetSchema.Fields() {
		indices := rec.Schema().FieldIndices(field.Name)
		if len(indices) == 0 {
			// Field missing in record, fill with nulls
			cols[i] = createNullArray(mem, field.Type, int(rec.NumRows()))
			continue
		}
		col := rec.Column(indices[0])

		// Strict type check for now
		if !arrow.TypeEqual(col.DataType(), field.Type) {
			return nil, fmt.Errorf("type mismatch for field %s: expected %s, got %s", field.Name, field.Type, col.DataType())
		}

		col.Retain()
		cols[i] = col
	}

	return array.NewRecordBatch(targetSchema, cols, rec.NumRows()), nil
}

func createNullArray(mem memory.Allocator, dt arrow.DataType, length int) arrow.Array {
	b := array.NewBuilder(mem, dt)
	defer b.Release()
	b.Reserve(length)
	for i := 0; i < length; i++ {
		b.AppendNull()
	}
	return b.NewArray()
}
