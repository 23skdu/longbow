package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// FilterOperator represents a vectorized filter comparison operator
type FilterOperator int

const (
	FilterOpEqual FilterOperator = iota
	FilterOpNotEqual
	FilterOpGreater
	FilterOpLess
	FilterOpGreaterEqual
	FilterOpLessEqual
	FilterOpIn
	FilterOpNotIn
	FilterOpContains
)

// String returns the string representation of the operator
func (op FilterOperator) String() string {
	switch op {
	case FilterOpEqual:
		return "="
	case FilterOpNotEqual:
		return "!="
	case FilterOpGreater:
		return ">"
	case FilterOpLess:
		return "<"
	case FilterOpGreaterEqual:
		return ">="
	case FilterOpLessEqual:
		return "<="
	case FilterOpIn:
		return "IN"
	case FilterOpNotIn:
		return "NOT IN"
	case FilterOpContains:
		return "CONTAINS"
	default:
		return "UNKNOWN"
	}
}

// ComputeFunc returns the Arrow compute function name for the operator
func (op FilterOperator) ComputeFunc() string {
	switch op {
	case FilterOpEqual:
		return "equal"
	case FilterOpNotEqual:
		return "not_equal"
	case FilterOpGreater:
		return "greater"
	case FilterOpLess:
		return "less"
	case FilterOpGreaterEqual:
		return "greater_equal"
	case FilterOpLessEqual:
		return "less_equal"
	default:
		return ""
	}
}

// ParseFilterOperator parses a string operator into FilterOperator
func ParseFilterOperator(op string) (FilterOperator, error) {
	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "=", "==":
		return FilterOpEqual, nil
	case "!=", "<>":
		return FilterOpNotEqual, nil
	case ">":
		return FilterOpGreater, nil
	case "<":
		return FilterOpLess, nil
	case ">=":
		return FilterOpGreaterEqual, nil
	case "<=":
		return FilterOpLessEqual, nil
	case "IN":
		return FilterOpIn, nil
	case "NOT IN":
		return FilterOpNotIn, nil
	case "CONTAINS", "LIKE":
		return FilterOpContains, nil
	default:
		return 0, fmt.Errorf("unknown filter operator: %s", op)
	}
}

// VectorizedFilter provides optimized Arrow Compute based filtering
type VectorizedFilter struct {
	alloc memory.Allocator
}

// NewVectorizedFilter creates a new vectorized filter
func NewVectorizedFilter(alloc memory.Allocator) *VectorizedFilter {
	return &VectorizedFilter{alloc: alloc}
}

// Apply applies filters to a record batch using vectorized Arrow Compute operations
func (vf *VectorizedFilter) Apply(ctx context.Context, rec arrow.RecordBatch, filters []Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	masks := make([]arrow.Array, 0, len(filters))
	defer func() {
		for _, m := range masks {
			if m != nil {
				m.Release()
			}
		}
	}()

	for _, f := range filters {
		mask, err := vf.applyFilter(ctx, rec, f)
		if err != nil {
			return nil, err
		}
		if mask != nil {
			masks = append(masks, mask)
		}
	}

	if len(masks) == 0 {
		rec.Retain()
		return rec, nil
	}

	// Combine all masks with AND
	finalMask, err := vf.combineMasks(ctx, masks)
	if err != nil {
		return nil, err
	}
	defer finalMask.Release()

	// Apply final filter
	return vf.filterRecordBatch(ctx, rec, finalMask)
}

// applyFilter applies a single filter and returns a boolean mask
func (vf *VectorizedFilter) applyFilter(ctx context.Context, rec arrow.RecordBatch, f Filter) (arrow.Array, error) {
	// Find column
	indices := rec.Schema().FieldIndices(f.Field)
	if len(indices) == 0 {
		return nil, nil // Skip unknown fields
	}
	col := rec.Column(indices[0])

	// Parse operator
	op, err := ParseFilterOperator(f.Operator)
	if err != nil {
		return nil, err
	}

	// Handle special operators
	switch op {
	case FilterOpIn:
		return vf.applyInFilter(ctx, col, f.Value, false)
	case FilterOpNotIn:
		return vf.applyInFilter(ctx, col, f.Value, true)
	case FilterOpContains:
		return vf.applyContainsFilter(col, f.Value)
	default:
		return vf.applyComparisonFilter(ctx, col, op, f.Value)
	}
}

// applyComparisonFilter applies comparison operators.
// Uses fast path for primitives with equality, bypasses Arrow Compute overhead.
func (vf *VectorizedFilter) applyComparisonFilter(ctx context.Context, col arrow.Array, op FilterOperator, value string) (arrow.Array, error) {
	// Fast path for primitive types with equality operators (2-5x speedup)
	if IsFastPathSupported(col.DataType(), op) {
		if op == FilterOpEqual {
			return FastPathEqual(ctx, col, value)
		}
		if op == FilterOpNotEqual {
			return FastPathNotEqual(ctx, col, value)
		}
	}

	valScalar, err := vf.createScalar(col.DataType(), value)
	if err != nil {
		return nil, err
	}

	funcName := op.ComputeFunc()
	if funcName == "" {
		return nil, fmt.Errorf("no compute function for operator %v", op)
	}

	result, err := compute.CallFunction(ctx, funcName, nil,
		compute.NewDatum(col.Data()),
		compute.NewDatum(valScalar),
	)
	if err != nil {
		return nil, fmt.Errorf("compute %s error: %w", funcName, err)
	}

	return result.(*compute.ArrayDatum).MakeArray(), nil
}

// applyInFilter applies IN or NOT IN using vectorized operations
// IN: OR of equals (match ANY value)
// NOT IN: AND of not_equals (match NONE of the values)
func (vf *VectorizedFilter) applyInFilter(ctx context.Context, col arrow.Array, value string, negate bool) (arrow.Array, error) {
	values := strings.Split(value, ",")
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}

	if len(values) == 0 {
		// Empty IN set - return all false; NOT IN() = true
		builder := array.NewBooleanBuilder(vf.alloc)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			builder.Append(negate)
		}
		return builder.NewArray(), nil
	}

	// Choose comparison function and combiner based on negate
	// IN: equal + OR (match any)
	// NOT IN: not_equal + AND (match none)
	compareFunc := "equal"
	combineFunc := "or_kleene"
	if negate {
		compareFunc = "not_equal"
		combineFunc = "and_kleene"
	}

	masks := make([]arrow.Array, 0, len(values))
	defer func() {
		for _, m := range masks {
			if m != nil {
				m.Release()
			}
		}
	}()

	for _, v := range values {
		valScalar, err := vf.createScalar(col.DataType(), v)
		if err != nil {
			continue // Skip invalid values
		}

		result, err := compute.CallFunction(ctx, compareFunc, nil,
			compute.NewDatum(col.Data()),
			compute.NewDatum(valScalar),
		)
		if err != nil {
			return nil, fmt.Errorf("%s compute error: %w", compareFunc, err)
		}
		masks = append(masks, result.(*compute.ArrayDatum).MakeArray())
	}

	if len(masks) == 0 {
		// No valid values
		builder := array.NewBooleanBuilder(vf.alloc)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			builder.Append(negate)
		}
		return builder.NewArray(), nil
	}

	// Combine masks
	combined := masks[0]
	combined.Retain()

	for i := 1; i < len(masks); i++ {
		combineResult, err := compute.CallFunction(ctx, combineFunc, nil,
			compute.NewDatum(combined.Data()),
			compute.NewDatum(masks[i].Data()),
		)
		combined.Release()
		if err != nil {
			return nil, fmt.Errorf("%s compute error: %w", combineFunc, err)
		}
		combined = combineResult.(*compute.ArrayDatum).MakeArray()
	}

	return combined, nil
}

// applyContainsFilter applies substring matching using vectorized comparison
func (vf *VectorizedFilter) applyContainsFilter(col arrow.Array, pattern string) (arrow.Array, error) {
	if col.DataType().ID() != arrow.STRING {
		return nil, fmt.Errorf("CONTAINS operator only supported for string columns")
	}

	// Build mask by checking each string for substring
	strArr := col.(*array.String)
	builder := array.NewBooleanBuilder(vf.alloc)
	defer builder.Release()

	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(strings.Contains(strArr.Value(i), pattern))
		}
	}

	return builder.NewArray(), nil
}

// createScalar creates a scalar value from string based on data type
func (vf *VectorizedFilter) createScalar(dt arrow.DataType, value string) (scalar.Scalar, error) {
	switch dt.ID() {
	case arrow.STRING:
		return scalar.NewStringScalar(value), nil
	case arrow.INT64:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 value: %s", value)
		}
		return scalar.NewInt64Scalar(v), nil
	case arrow.FLOAT64:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 value: %s", value)
		}
		return scalar.NewFloat64Scalar(v), nil
	case arrow.TIMESTAMP:
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp value: %s", value)
		}
		tsType := dt.(*arrow.TimestampType)
		ts, _ := arrow.TimestampFromTime(t, tsType.Unit)
		return scalar.NewTimestampScalar(ts, tsType), nil
	case arrow.BOOL:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid bool value: %s", value)
		}
		return scalar.NewBooleanScalar(v), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dt)
	}
}

// combineMasks combines multiple boolean masks with AND
func (vf *VectorizedFilter) combineMasks(ctx context.Context, masks []arrow.Array) (arrow.Array, error) {
	if len(masks) == 0 {
		return nil, nil
	}
	if len(masks) == 1 {
		masks[0].Retain()
		return masks[0], nil
	}

	// Combine all masks using and_kleene for null handling
	result := masks[0]
	result.Retain()

	for i := 1; i < len(masks); i++ {
		andResult, err := compute.CallFunction(ctx, "and_kleene", nil,
			compute.NewDatum(result.Data()),
			compute.NewDatum(masks[i].Data()),
		)
		result.Release()
		if err != nil {
			return nil, fmt.Errorf("and_kleene compute error: %w", err)
		}
		result = andResult.(*compute.ArrayDatum).MakeArray()
	}

	return result, nil
}

// filterRecordBatch applies a boolean mask to filter a record batch
func (vf *VectorizedFilter) filterRecordBatch(ctx context.Context, rec arrow.RecordBatch, mask arrow.Array) (arrow.RecordBatch, error) {
	filterRes, err := compute.CallFunction(ctx, "filter", nil,
		compute.NewDatum(rec),
		compute.NewDatum(mask.Data()),
	)
	if err != nil {
		return nil, fmt.Errorf("filter compute error: %w", err)
	}
	return filterRes.(*compute.RecordDatum).Value, nil
}
