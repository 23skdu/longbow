package query

import (
	"fmt"
	"log"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func matchInt64WithError(src []int64, val int64, op simd.CompareOp, dst []byte) {
	if err := simd.MatchInt64(src, val, op, dst); err != nil {
		log.Printf("SIMD MatchInt64 error: %v", err)
	}
}

// matchFloat32WithError wraps SIMD MatchFloat32 and logs errors
func matchFloat32WithError(src []float32, val float32, op simd.CompareOp, dst []byte) {
	if err := simd.MatchFloat32(src, val, op, dst); err != nil {
		log.Printf("SIMD MatchFloat32 error: %v", err)
	}
}

// filterOp represents a typed operation on a specific column

type int64FilterOp struct {
	col      *array.Int64
	val      int64
	operator string
	colIdx   int
}

func (o *int64FilterOp) Compound() bool { return false }
func (o *int64FilterOp) MatchValue(val interface{}) bool {
	switch v := val.(type) {
	case int64:
		return o.compareInt64(v)
	case int32:
		return o.compareInt64(int64(v))
	case int16:
		return o.compareInt64(int64(v))
	case int8:
		return o.compareInt64(int64(v))
	case float64:
		return o.compareInt64(int64(v))
	case float32:
		return o.compareInt64(int64(v))
	}
	return false
}
func (o *int64FilterOp) compareInt64(v int64) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}
func (o *int64FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.INT64 {
		return fmt.Errorf("expected int64 column, got %s", col.DataType())
	}
	o.col = col.(*array.Int64)
	return nil
}

func (o *int64FilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}

func (o *int64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	if rowIdx == 20 || rowIdx == 95 {
		log.Printf("DEBUG: int64FilterOp.Match Index %d, val %d, src %d", rowIdx, o.val, v)
	}
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
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
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
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

	matchInt64WithError(o.col.Int64Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

func (o *int64FilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}

	values := make([]int64, len(indices))
	for i, idx := range indices {
		values[i] = o.col.Value(idx)
	}

	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		result := make([]int, 0, len(indices))
		for _, idx := range indices {
			if o.Match(idx) {
				result = append(result, idx)
			}
		}
		return result
	}

	bitmap := make([]byte, len(indices))
	matchInt64WithError(values, o.val, op, bitmap)

	result := make([]int, 0, len(indices))
	hasNulls := o.col.NullN() > 0

	for i, b := range bitmap {
		if b == 1 {
			idx := indices[i]
			if !hasNulls || !o.col.IsNull(idx) {
				result = append(result, idx)
			}
		}
	}
	return result
}

type int32FilterOp struct {
	col      *array.Int32
	val      int32
	operator string
	colIdx   int
}

func (o *int32FilterOp) Compound() bool { return false }
func (o *int32FilterOp) MatchValue(val interface{}) bool {
	switch v := val.(type) {
	case int32:
		return o.compareInt32(v)
	case int64:
		return o.compareInt32(int32(v)) // #nosec G115
	}
	return false
}
func (o *int32FilterOp) compareInt32(v int32) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}
func (o *int32FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.INT32 {
		return fmt.Errorf("expected int32 column, got %s", col.DataType())
	}
	o.col = col.(*array.Int32)
	return nil
}

func (o *int32FilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}
func (o *int32FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	return o.compareInt32(v)
}
func (o *int32FilterOp) MatchBitmap(dst []byte) {
	for i := range dst {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}
func (o *int32FilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

type uint64FilterOp struct {
	col      *array.Uint64
	val      uint64
	operator string
	colIdx   int
}

func (o *uint64FilterOp) Compound() bool { return false }
func (o *uint64FilterOp) MatchValue(val interface{}) bool {
	switch v := val.(type) {
	case uint64:
		return o.compareUint64(v)
	}
	return false
}
func (o *uint64FilterOp) compareUint64(v uint64) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}
func (o *uint64FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.UINT64 {
		return fmt.Errorf("expected uint64 column, got %s", col.DataType())
	}
	o.col = col.(*array.Uint64)
	return nil
}

func (o *uint64FilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}
func (o *uint64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	return o.compareUint64(v)
}
func (o *uint64FilterOp) MatchBitmap(dst []byte) {
	for i := range dst {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}
func (o *uint64FilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

type float32FilterOp struct {
	col      *array.Float32
	val      float32
	operator string
	colIdx   int
}

func (o *float32FilterOp) Compound() bool { return false }
func (o *float32FilterOp) MatchValue(val interface{}) bool {
	switch v := val.(type) {
	case float64:
		return o.compareFloat32(float32(v))
	case float32:
		return o.compareFloat32(v)
	case int64:
		return o.compareFloat32(float32(v))
	}
	return false
}
func (o *float32FilterOp) compareFloat32(v float32) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}
func (o *float32FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.FLOAT32 {
		return fmt.Errorf("expected float32 column, got %s", col.DataType())
	}
	o.col = col.(*array.Float32)
	return nil
}

func (o *float32FilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}

func (o *float32FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	return o.compareFloat32(o.col.Value(rowIdx))
}

func (o *float32FilterOp) MatchBitmap(dst []byte) {
	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
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

	matchFloat32WithError(o.col.Float32Values(), o.val, op, dst)

	if o.col.NullN() > 0 {
		offset := o.col.Data().Offset()
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i + offset) {
				dst[i] = 0
			}
		}
	}
}

func (o *float32FilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}

	values := make([]float32, len(indices))
	for i, idx := range indices {
		values[i] = o.col.Value(idx)
	}

	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
		op = simd.CompareLe
	default:
		result := make([]int, 0, len(indices))
		for _, idx := range indices {
			if o.Match(idx) {
				result = append(result, idx)
			}
		}
		return result
	}

	bitmap := make([]byte, len(indices))
	matchFloat32WithError(values, o.val, op, bitmap)

	result := make([]int, 0, len(indices))
	hasNulls := o.col.NullN() > 0

	for i, b := range bitmap {
		if b == 1 {
			idx := indices[i]
			if !hasNulls || !o.col.IsNull(idx) {
				result = append(result, idx)
			}
		}
	}
	return result
}

type float64FilterOp struct {
	col      *array.Float64
	val      float64
	operator string
	colIdx   int
}

func (o *float64FilterOp) Compound() bool { return false }
func (o *float64FilterOp) MatchValue(val interface{}) bool {
	switch v := val.(type) {
	case float64:
		return o.compareFloat64(v)
	case float32:
		return o.compareFloat64(float64(v))
	case int64:
		return o.compareFloat64(float64(v))
	}
	return false
}
func (o *float64FilterOp) compareFloat64(v float64) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}
func (o *float64FilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.FLOAT64 {
		return fmt.Errorf("expected float64 column, got %s", col.DataType())
	}
	o.col = col.(*array.Float64)
	return nil
}

func (o *float64FilterOp) Reset(rec arrow.RecordBatch) error {
	if o.colIdx < 0 || o.colIdx >= int(rec.NumCols()) {
		return fmt.Errorf("column index %d out of bounds", o.colIdx)
	}
	return o.Bind(rec.Column(o.colIdx))
}

func (o *float64FilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	v := o.col.Value(rowIdx)
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	case ">", "gt":
		return v > o.val
	case "<", "lt":
		return v < o.val
	case ">=", "ge":
		return v >= o.val
	case "<=", "le":
		return v <= o.val
	}
	return false
}

func (o *float64FilterOp) MatchBitmap(dst []byte) {
	if len(dst) == 0 {
		return
	}

	var op simd.CompareOp
	switch o.operator {
	case "=", "eq", "==":
		op = simd.CompareEq
	case "!=", "neq":
		op = simd.CompareNeq
	case ">", "gt":
		op = simd.CompareGt
	case ">=", "ge":
		op = simd.CompareGe
	case "<", "lt":
		op = simd.CompareLt
	case "<=", "le":
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

	data := o.col.Float64Values()
	if len(data) < len(dst) {
		// Should not happen with correct dst length, but handle safely
		_ = simd.MatchFloat64(data, o.val, op, dst[:len(data)])
		for i := len(data); i < len(dst); i++ {
			dst[i] = 0
		}
	} else {
		_ = simd.MatchFloat64(data[:len(dst)], o.val, op, dst)
	}

	// Handle nulls
	if o.col.NullN() > 0 {
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i) {
				dst[i] = 0
			}
		}
	}
}

func (o *float64FilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}
