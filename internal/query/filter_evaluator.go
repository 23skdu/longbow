package query

import (
	"fmt"
	"strconv"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// filterOp represents a typed operation on a specific column
type filterOp interface {
	Match(rowIdx int) bool
	MatchBitmap(dst []byte)
}

// int64FilterOp avoids string conversion for int64 columns
type int64FilterOp struct {
	col      *array.Int64
	val      int64
	operator string
}

func (o *int64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">":
		return v > o.val
	case "<":
		return v < o.val
	case ">=":
		return v >= o.val
	case "<=":
		return v <= o.val
	}
	return false
}

func (o *int64FilterOp) MatchBitmap(dst []byte) {
	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">":
		op = simd.CompareGt
	case ">=":
		op = simd.CompareGe
	case "<":
		op = simd.CompareLt
	case "<=":
		op = simd.CompareLe
	default:
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
		return
	}

	simd.MatchInt64(o.col.Int64Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

// float32FilterOp avoids string conversion for float32 columns
type float32FilterOp struct {
	col      *array.Float32
	val      float32
	operator string
}

func (o *float32FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">":
		return v > o.val
	case "<":
		return v < o.val
	case ">=":
		return v >= o.val
	case "<=":
		return v <= o.val
	}
	return false
}

func (o *float32FilterOp) MatchBitmap(dst []byte) {
	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">":
		op = simd.CompareGt
	case ">=":
		op = simd.CompareGe
	case "<":
		op = simd.CompareLt
	case "<=":
		op = simd.CompareLe
	default:
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
		return
	}

	simd.MatchFloat32(o.col.Float32Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

// stringFilterOp optimizes string comparisons
type stringFilterOp struct {
	col      *array.String
	val      string
	operator string
}

func (o *stringFilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">":
		return v > o.val
	case "<":
		return v < o.val
	case ">=":
		return v >= o.val
	case "<=":
		return v <= o.val
	}
	return false
}

func (o *stringFilterOp) MatchBitmap(dst []byte) {
	for i := 0; i < len(dst); i++ {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

// FilterEvaluator pre-processes filters for a specific RecordBatch to enable fast scanning
type FilterEvaluator struct {
	ops []filterOp
}

// NewFilterEvaluator creates a new evaluator, pre-binding filters to RecordBatch columns
func NewFilterEvaluator(rec arrow.RecordBatch, filters []Filter) (*FilterEvaluator, error) {
	if len(filters) == 0 {
		return &FilterEvaluator{}, nil
	}

	ops := make([]filterOp, 0, len(filters))
	schema := rec.Schema()

	for _, f := range filters {
		indices := schema.FieldIndices(f.Field)
		if len(indices) == 0 {
			continue // Or return error? For now follow store_helpers.go behavior
		}
		colIdx := indices[0]
		col := rec.Column(colIdx)

		switch col.DataType().ID() {
		case arrow.INT64:
			val, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
			}
			ops = append(ops, &int64FilterOp{
				col:      col.(*array.Int64),
				val:      val,
				operator: f.Operator,
			})
		case arrow.FLOAT32:
			val, err := strconv.ParseFloat(f.Value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
			}
			ops = append(ops, &float32FilterOp{
				col:      col.(*array.Float32),
				val:      float32(val),
				operator: f.Operator,
			})
		case arrow.STRING:
			ops = append(ops, &stringFilterOp{
				col:      col.(*array.String),
				val:      f.Value,
				operator: f.Operator,
			})
		default:
			// Fallback to slow evaluator or error
			// For now, let's keep it simple and handle common types
		}
	}
	if len(ops) == 0 && len(filters) > 0 {
		return nil, fmt.Errorf("failed to bind any filters to schema fields")
	}
	return &FilterEvaluator{ops: ops}, nil
}

// Matches returns true if the row satisfies all filters
func (e *FilterEvaluator) Matches(rowIdx int) bool {
	// Unrolled check for performance (Go compiler can optimize this)
	for i := 0; i < len(e.ops); i++ {
		if !e.ops[i].Match(rowIdx) {
			return false
		}
	}
	return true
}

// MatchesBatch evaluates filters for a slice of row indices and returns a subset of matching indices.
// This is the "SIMD-friendly" entry point for batch processing.
func (e *FilterEvaluator) MatchesBatch(rowIndices []int) []int {
	if len(e.ops) == 0 {
		return rowIndices
	}

	filtered := make([]int, 0, len(rowIndices))
	for _, idx := range rowIndices {
		match := true
		for i := 0; i < len(e.ops); i++ {
			if !e.ops[i].Match(idx) {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, idx)
		}
	}
	return filtered
}

// MatchesAll evaluates all filters on the entire batch using SIMD and returns matching row indices.
// This is optimal for dense scans.
func (e *FilterEvaluator) MatchesAll(batchLen int) []int {
	if len(e.ops) == 0 {
		// Return all indices
		indices := make([]int, batchLen)
		for i := 0; i < batchLen; i++ {
			indices[i] = i
		}
		return indices
	}

	// Allocate bitmaps
	// Note: batchLen serves as capacity.
	// We assume all columns in the batch have the same length.
	// Ops access columns which know their length, but dst must be sized correctly.
	// We pass dst of batchLen.

	bitmap := make([]byte, batchLen)
	// Initialize first op
	e.ops[0].MatchBitmap(bitmap)

	if len(e.ops) > 1 {
		tmp := make([]byte, batchLen)
		for i := 1; i < len(e.ops); i++ {
			e.ops[i].MatchBitmap(tmp)
			// Intersect (AND)
			simd.AndBytes(bitmap, tmp)
		}
	}

	// Collect indices
	indices := make([]int, 0, batchLen/2) // Pre-allocate estimate
	for i, b := range bitmap {
		if b != 0 {
			indices = append(indices, i)
		}
	}
	return indices
}
