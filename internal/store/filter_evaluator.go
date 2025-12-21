package store

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// filterOp represents a typed operation on a specific column
type filterOp interface {
	Match(rowIdx int) bool
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
