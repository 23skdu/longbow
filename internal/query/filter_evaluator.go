package query

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// matchInt64WithError wraps SIMD MatchInt64 and logs errors
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
type filterOp interface {
	Match(rowIdx int) bool
	MatchBitmap(dst []byte)
	FilterBatch(indices []int) []int
	Bind(col arrow.Array) error
	Compound() bool
	MatchValue(val interface{}) bool
}

// compoundFilterOp handles AND/OR/NOT logic over child filterOps
type compoundFilterOp struct {
	logic    string // "AND", "OR", "NOT"
	children []filterOp
}

func (c *compoundFilterOp) Compound() bool { return true }

func (c *compoundFilterOp) Match(rowIdx int) bool {
	switch c.logic {
	case "AND":
		for _, child := range c.children {
			if !child.Match(rowIdx) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.children {
			if child.Match(rowIdx) {
				return true
			}
		}
		return false
	case "NOT":
		if len(c.children) == 0 {
			return true
		}
		return !c.children[0].Match(rowIdx)
	default:
		return false
	}
}

func (c *compoundFilterOp) MatchBitmap(dst []byte) {
	if len(dst) == 0 {
		return
	}

	temp := make([]byte, len(dst))

	switch c.logic {
	case "AND":
		// Initialize with all 1s
		for i := range dst {
			dst[i] = 1
		}
		for _, child := range c.children {
			child.MatchBitmap(temp)
			_ = simd.AndBytes(dst, temp)
		}
	case "OR":
		// Initialize with all 0s
		for i := range dst {
			dst[i] = 0
		}
		for _, child := range c.children {
			child.MatchBitmap(temp)
			_ = simd.OrBytes(dst, temp)
		}
	case "NOT":
		if len(c.children) > 0 {
			c.children[0].MatchBitmap(dst)
			_ = simd.NotBytes(dst)
		} else {
			for i := range dst {
				dst[i] = 1
			}
		}
	}
}

func (c *compoundFilterOp) FilterBatch(indices []int) []int {
	if len(indices) == 0 {
		return nil
	}
	switch c.logic {
	case "AND":
		result := indices
		for _, child := range c.children {
			result = child.FilterBatch(result)
			if len(result) == 0 {
				return nil
			}
		}
		return result
	case "OR":
		seen := make(map[int]bool)
		var result []int
		for _, child := range c.children {
			matches := child.FilterBatch(indices)
			for _, idx := range matches {
				if !seen[idx] {
					seen[idx] = true
					result = append(result, idx)
				}
			}
		}
		// Maintain original order
		order := make(map[int]int)
		for i, idx := range indices {
			order[idx] = i
		}
		sorted := make([]int, len(result))
		copy(sorted, result)
		for i := 0; i < len(sorted)-1; i++ {
			for j := i + 1; j < len(sorted); j++ {
				if order[sorted[i]] > order[sorted[j]] {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		return sorted
	case "NOT":
		if len(c.children) == 0 {
			return indices
		}
		excluded := c.children[0].FilterBatch(indices)
		excludedMap := make(map[int]bool)
		for _, idx := range excluded {
			excludedMap[idx] = true
		}
		var result []int
		for _, idx := range indices {
			if !excludedMap[idx] {
				result = append(result, idx)
			}
		}
		return result
	default:
		return indices
	}
}

func (c *compoundFilterOp) Bind(col arrow.Array) error {
	for _, child := range c.children {
		if err := child.Bind(col); err != nil {
			return err
		}
	}
	return nil
}

func (c *compoundFilterOp) MatchValue(val interface{}) bool {
	switch c.logic {
	case "AND":
		for _, child := range c.children {
			if !child.MatchValue(val) {
				return false
			}
		}
		return true
	case "OR":
		for _, child := range c.children {
			if child.MatchValue(val) {
				return true
			}
		}
		return false
	case "NOT":
		if len(c.children) == 0 {
			return true
		}
		return !c.children[0].MatchValue(val)
	default:
		return false
	}
}

func resolveNestedField(schema arrow.Schema, fieldPath string) ([]int, arrow.DataType, error) {
	parts := strings.Split(fieldPath, ".")
	return resolveNestedFieldParts(schema, parts, 0)
}

func resolveNestedFieldParts(schema arrow.Schema, parts []string, depth int) ([]int, arrow.DataType, error) {
	if len(parts) == 0 {
		return nil, nil, fmt.Errorf("empty field path")
	}

	idx := schema.FieldIndices(parts[0])
	if len(idx) == 0 {
		return idx, nil, fmt.Errorf("field %q not found at depth %d", parts[0], depth)
	}
	field := schema.Field(idx[0])

	if len(parts) == 1 {
		return idx, field.Type, nil
	}

	switch field.Type.ID() {
	case arrow.STRUCT:
		childType := field.Type.(*arrow.StructType)
		childSchema := arrow.NewSchema(childType.Fields(), nil)
		rest, dt, err := resolveNestedFieldParts(*childSchema, parts[1:], depth+1)
		if err != nil {
			return nil, nil, err
		}
		return append(idx, rest...), dt, nil
	case arrow.LIST:
		childType := field.Type.(*arrow.ListType)
		elemField := childType.ElemField()
		childSchema := arrow.NewSchema([]arrow.Field{elemField}, nil)
		rest, dt, err := resolveNestedFieldParts(*childSchema, parts[1:], depth+1)
		if err != nil {
			return nil, nil, err
		}
		return append(idx, rest...), dt, nil
	case arrow.FIXED_SIZE_LIST:
		childType := field.Type.(*arrow.FixedSizeListType)
		elemField := childType.ElemField()
		childSchema := arrow.NewSchema([]arrow.Field{elemField}, nil)
		rest, dt, err := resolveNestedFieldParts(*childSchema, parts[1:], depth+1)
		if err != nil {
			return nil, nil, err
		}
		return append(idx, rest...), dt, nil
	}

	return nil, nil, fmt.Errorf("field %q is not nested at depth %d", parts[0], depth)
}

func extractNestedValue(col arrow.Array, rowIdx int, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	if len(parts) > 1 {
		parts = parts[1:]
	}
	return extractNestedValueParts(col, rowIdx, parts)
}

func extractNestedValueParts(col arrow.Array, rowIdx int, parts []string) interface{} {
	if len(parts) == 0 {
		return extractScalarValue(col, rowIdx)
	}

	switch col.DataType().ID() {
	case arrow.STRUCT:
		s := col.(*array.Struct)
		childType := s.DataType().(*arrow.StructType)
		childIdx := -1
		for i := 0; i < childType.NumFields(); i++ {
			if childType.Fields()[i].Name == parts[0] {
				childIdx = i
				break
			}
		}
		if childIdx < 0 {
			return nil
		}
		childCol := s.Field(childIdx)
		if len(parts) == 1 {
			return extractScalarValue(childCol, rowIdx)
		}
		return extractNestedValueParts(childCol, rowIdx, parts[1:])
	case arrow.LIST:
		l := col.(*array.List)
		offsets := l.Offsets()
		if int(offsets[rowIdx]) >= l.Len() {
			return nil
		}
		childCol := l.ListValues()
		childRow := int(offsets[rowIdx])
		if len(parts) == 1 {
			return extractScalarValue(childCol, childRow)
		}
		return extractNestedValueParts(childCol, childRow, parts[1:])
	case arrow.FIXED_SIZE_LIST:
		fl := col.(*array.FixedSizeList)
		childCol := fl.ListValues()
		size := int(fl.DataType().(*arrow.FixedSizeListType).Len())
		start := rowIdx * size
		if len(parts) == 1 {
			return extractScalarValue(childCol, start)
		}
		return extractNestedValueParts(childCol, start, parts[1:])
	default:
		return extractScalarValue(col, rowIdx)
	}
}

func extractScalarValue(col arrow.Array, rowIdx int) interface{} {
	if col.IsNull(rowIdx) {
		return nil
	}
	switch col.DataType().ID() {
	case arrow.INT64:
		return col.(*array.Int64).Value(rowIdx)
	case arrow.FLOAT32:
		return col.(*array.Float32).Value(rowIdx)
	case arrow.FLOAT64:
		return col.(*array.Float64).Value(rowIdx)
	case arrow.STRING:
		return col.(*array.String).Value(rowIdx)
	case arrow.UINT64:
		return col.(*array.Uint64).Value(rowIdx)
	case arrow.INT32:
		return col.(*array.Int32).Value(rowIdx)
	case arrow.UINT32:
		return col.(*array.Uint32).Value(rowIdx)
	case arrow.INT16:
		return col.(*array.Int16).Value(rowIdx)
	case arrow.UINT16:
		return col.(*array.Uint16).Value(rowIdx)
	case arrow.INT8:
		return col.(*array.Int8).Value(rowIdx)
	case arrow.UINT8:
		return col.(*array.Uint8).Value(rowIdx)
	case arrow.BOOL:
		return col.(*array.Boolean).Value(rowIdx)
	default:
		return nil
	}
}

// nestedFilterOp handles filtering on nested field paths (dot-notation).
type nestedFilterOp struct {
	fieldPath  string
	colIndices []int
	op         filterOp
	outerCol   arrow.Array
}

func (n *nestedFilterOp) Compound() bool { return false }

func (n *nestedFilterOp) Match(rowIdx int) bool {
	if n.outerCol == nil {
		return false
	}
	parts := strings.Split(n.fieldPath, ".")
	if len(parts) > 1 {
		parts = parts[1:]
	}

	if n.outerCol.DataType().ID() == arrow.LIST {
		return n.matchAnyInList(rowIdx, parts)
	}

	val := extractNestedValueParts(n.outerCol, rowIdx, parts)
	if val == nil {
		return false
	}
	return n.op.MatchValue(val)
}

func (n *nestedFilterOp) matchAnyInList(rowIdx int, parts []string) bool {
	l := n.outerCol.(*array.List)
	offsets := l.Offsets()
	start := int(offsets[rowIdx])
	end := int(offsets[rowIdx+1])

	childCol := l.ListValues()

	for i := start; i < end; i++ {
		val := extractNestedValueParts(childCol, i, parts)
		if val != nil && n.op.MatchValue(val) {
			return true
		}
	}
	return false
}

func (n *nestedFilterOp) MatchBitmap(dst []byte) {
	for i := range dst {
		if n.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func (n *nestedFilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if n.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

func (n *nestedFilterOp) Bind(col arrow.Array) error {
	n.outerCol = col
	return nil
}

func (n *nestedFilterOp) MatchValue(val interface{}) bool {
	return n.op.MatchValue(val)
}

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

type stringFilterOp struct {
	col      *array.String
	val      string
	operator string
	colIdx   int
}

func (o *stringFilterOp) Compound() bool { return false }
func (o *stringFilterOp) MatchValue(val interface{}) bool {
	v, ok := val.(string)
	if !ok {
		return false
	}
	return o.compareString(v)
}
func (o *stringFilterOp) compareString(v string) bool {
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
func (o *stringFilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.STRING {
		return fmt.Errorf("expected String column, got %s", col.DataType())
	}
	o.col = col.(*array.String)
	return nil
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

func (o *stringFilterOp) MatchBitmap(dst []byte) {
	start := time.Now()
	defer func() {
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "optimized").Inc()
		metrics.StringFilterDurationSeconds.WithLabelValues(o.operator, "optimized").Observe(time.Since(start).Seconds())
	}()

	valLen := len(o.val)

	switch o.operator {
	case "=", "eq", "==":
		metrics.StringFilterEqualLengthTotal.Inc()

		if valLen == 0 {
			for i := 0; i < len(dst); i++ {
				switch {
				case o.col.IsNull(i):
					dst[i] = 0
				case o.col.ValueLen(i) == 0:
					dst[i] = 1
				default:
					dst[i] = 0
				}
			}
			return
		}

		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i) {
				dst[i] = 0
				continue
			}
			if o.col.ValueLen(i) != valLen {
				dst[i] = 0
				continue
			}

			s := o.col.Value(i)
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))

			match := true
			for j := 0; j < valLen; j++ {
				if s[j] != o.val[j] {
					match = false
					break
				}
			}
			if match {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}

	case "!=", "neq":
		for i := 0; i < len(dst); i++ {
			if o.col.IsNull(i) {
				dst[i] = 0
				continue
			}
			if o.col.ValueLen(i) != valLen {
				dst[i] = 1
				continue
			}

			s := o.col.Value(i)
			metrics.StringFilterComparisonsTotal.Inc()
			metrics.StringFilterBytesComparedTotal.Add(float64(valLen))

			match := true
			for j := 0; j < valLen; j++ {
				if s[j] != o.val[j] {
					match = false
					break
				}
			}
			if match {
				dst[i] = 0
			} else {
				dst[i] = 1
			}
		}

	case ">", "gt":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) > o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case "<", "lt":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) < o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case ">=", "ge":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) >= o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	case "<=", "le":
		for i := 0; i < len(dst); i++ {
			switch {
			case o.col.IsNull(i):
				dst[i] = 0
			case o.col.Value(i) <= o.val:
				dst[i] = 1
			default:
				dst[i] = 0
			}
		}

	default:
		metrics.StringFilterOpsTotal.WithLabelValues(o.operator, "slow").Inc()
		for i := 0; i < len(dst); i++ {
			if o.Match(i) {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
}

func (o *stringFilterOp) FilterBatch(indices []int) []int {
	// String comparison is hard to vectorize without fancy SIMD (PCMPESTRM) or fixed width.
	// Fallback to loop for now.
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

// FilterEvaluator pre-processes filters for a specific RecordBatch to enable fast scanning
type FilterEvaluator struct {
	ops []filterOp
}

// NewFilterEvaluator creates a new evaluator, pre-binding filters to RecordBatch columns.
// Supports compound expressions (AND/OR/NOT) with nested field paths (dot notation).
func NewFilterEvaluator(rec arrow.RecordBatch, filters []Filter) (*FilterEvaluator, error) {
	if len(filters) == 0 {
		return &FilterEvaluator{}, nil
	}

	ops := make([]filterOp, 0, len(filters))
	schema := rec.Schema()

	for _, f := range filters {
		op, err := buildFilterOp(*schema, rec, &f)
		if err != nil {
			return nil, err
		}
		if op != nil {
			ops = append(ops, op)
		}
	}

	if len(ops) == 0 && len(filters) > 0 {
		return nil, fmt.Errorf("failed to bind any filters to schema fields")
	}
	return &FilterEvaluator{ops: ops}, nil
}

func buildFilterOp(schema arrow.Schema, rec arrow.RecordBatch, f *Filter) (filterOp, error) {
	logic := strings.ToUpper(f.Logic)
	if logic != "" {
		return buildCompoundOp(schema, rec, logic, f.Filters)
	}

	if f.Subquery != nil {
		return buildSubqueryOp(schema, rec, f)
	}

	isNested := strings.Contains(f.Field, ".")

	if isNested {
		colIndices, col, nestedType, err := resolveFilterColumnEx(schema, rec, f.Field)
		if err != nil || col == nil {
			return nil, nil
		}

		opStr := strings.ToLower(f.Operator)
		_ = colIndices
		colIdx := colIndices[0]

		var innerOp filterOp
		switch nestedType.ID() {
		case arrow.INT64:
			val, err := strconv.ParseInt(f.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &int64FilterOp{val: val, operator: opStr, colIdx: colIdx}
		case arrow.FLOAT32:
			val, err := strconv.ParseFloat(f.Value, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &float32FilterOp{val: float32(val), operator: opStr, colIdx: colIdx}
		case arrow.FLOAT64:
			val, err := strconv.ParseFloat(f.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float64 value %q for field %s", f.Value, f.Field)
			}
			innerOp = &float64FilterOp{val: val, operator: opStr, colIdx: colIdx}
		case arrow.STRING:
			innerOp = &stringFilterOp{val: f.Value, operator: opStr, colIdx: colIdx}
		default:
			return nil, nil
		}

		return &nestedFilterOp{fieldPath: f.Field, colIndices: colIndices, op: innerOp, outerCol: col}, nil
	}

	colIndices, col, nestedType, err := resolveFilterColumnEx(schema, rec, f.Field)
	if err != nil || col == nil {
		return nil, nil
	}

	opStr := strings.ToLower(f.Operator)
	colIdx := colIndices[0]
	
	switch nestedType.ID() {

	case arrow.INT64:
		val, err := strconv.ParseInt(f.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int64 value %q for field %s", f.Value, f.Field)
		}
		return &int64FilterOp{col: col.(*array.Int64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.INT32:
		val, err := strconv.ParseInt(f.Value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid int32 value %q for field %s", f.Value, f.Field)
		}
		return &int32FilterOp{col: col.(*array.Int32), val: int32(val), operator: opStr, colIdx: colIdx}, nil
	case arrow.UINT64:
		val, err := strconv.ParseUint(f.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid uint64 value %q for field %s", f.Value, f.Field)
		}
		return &uint64FilterOp{col: col.(*array.Uint64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.FLOAT32:
		val, err := strconv.ParseFloat(f.Value, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid float32 value %q for field %s", f.Value, f.Field)
		}
		return &float32FilterOp{col: col.(*array.Float32), val: float32(val), operator: opStr, colIdx: colIdx}, nil
	case arrow.FLOAT64:
		val, err := strconv.ParseFloat(f.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float64 value %q for field %s", f.Value, f.Field)
		}
		return &float64FilterOp{col: col.(*array.Float64), val: val, operator: opStr, colIdx: colIdx}, nil
	case arrow.STRING:
		return &stringFilterOp{col: col.(*array.String), val: f.Value, operator: opStr, colIdx: colIdx}, nil
	case arrow.BOOL:
		val := strings.ToLower(f.Value) == "true"
		return &boolFilterOp{col: col.(*array.Boolean), val: val, operator: opStr, colIdx: colIdx}, nil
	default:
		return nil, nil
	}
}

type boolFilterOp struct {
	col      *array.Boolean
	val      bool
	operator string
	colIdx   int
}

func (o *boolFilterOp) Compound() bool { return false }
func (o *boolFilterOp) MatchValue(val interface{}) bool {
	if v, ok := val.(bool); ok {
		return o.compareBool(v)
	}
	return false
}
func (o *boolFilterOp) compareBool(v bool) bool {
	switch o.operator {
	case "=", "eq", "==":
		return v == o.val
	case "!=", "neq":
		return v != o.val
	}
	return false
}
func (o *boolFilterOp) Bind(col arrow.Array) error {
	if col.DataType().ID() != arrow.BOOL {
		return fmt.Errorf("expected boolean column, got %s", col.DataType())
	}
	o.col = col.(*array.Boolean)
	return nil
}
func (o *boolFilterOp) Match(rowIdx int) bool {
	if o.col.IsNull(rowIdx) {
		return false
	}
	return o.compareBool(o.col.Value(rowIdx))
}
func (o *boolFilterOp) MatchBitmap(dst []byte) {
	for i := range dst {
		if o.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}
func (o *boolFilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if o.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

func buildSubqueryOp(schema arrow.Schema, rec arrow.RecordBatch, f *Filter) (filterOp, error) {
	if f.Subquery == nil {
		return nil, nil
	}

	_, col, dt, err := resolveFilterColumnEx(schema, rec, f.Field)
	if err != nil || col == nil {
		return nil, nil
	}

	// Create a map for fast lookups of resolved subquery results
	valueSet := make(map[any]struct{})
	for _, v := range f.ResolvedValues {
		valueSet[v] = struct{}{}
	}

	return &subqueryFilterOp{
		field:    f.Field,
		col:      col,
		dataType: dt,
		valueSet: valueSet,
		op:       strings.ToLower(f.Operator),
	}, nil
}

type subqueryFilterOp struct {
	field    string
	col      arrow.Array
	dataType arrow.DataType
	valueSet map[any]struct{}
	op       string
}

func (s *subqueryFilterOp) Compound() bool { return false }

func (s *subqueryFilterOp) Match(rowIdx int) bool {
	if s.col.IsNull(rowIdx) {
		return false
	}
	val := extractScalarValue(s.col, rowIdx)
	if val == nil {
		return false
	}

	_, match := s.valueSet[val]
	if s.op == "not in" {
		return !match
	}
	return match
}

func (s *subqueryFilterOp) MatchBitmap(dst []byte) {
	for i := range dst {
		if s.Match(i) {
			dst[i] = 1
		} else {
			dst[i] = 0
		}
	}
}

func (s *subqueryFilterOp) FilterBatch(indices []int) []int {
	result := make([]int, 0, len(indices))
	for _, idx := range indices {
		if s.Match(idx) {
			result = append(result, idx)
		}
	}
	return result
}

func (s *subqueryFilterOp) Bind(col arrow.Array) error {
	s.col = col
	return nil
}

func (s *subqueryFilterOp) MatchValue(val interface{}) bool {
	_, match := s.valueSet[val]
	if s.op == "not in" {
		return !match
	}
	return match
}

func resolveFilterColumnEx(schema arrow.Schema, rec arrow.RecordBatch, fieldPath string) ([]int, arrow.Array, arrow.DataType, error) {
	parts := strings.Split(fieldPath, ".")
	if len(parts) == 0 {
		return nil, nil, nil, fmt.Errorf("empty field path")
	}

	rootIdx := schema.FieldIndices(parts[0])
	if len(rootIdx) == 0 {
		return nil, nil, nil, nil
	}

	indices, dt, err := resolveNestedField(schema, fieldPath)
	if err != nil {
		return nil, nil, nil, err
	}

	return indices, rec.Column(rootIdx[0]), dt, nil
}

func buildCompoundOp(schema arrow.Schema, rec arrow.RecordBatch, logic string, childFilters []Filter) (filterOp, error) {
	children := make([]filterOp, 0, len(childFilters))
	for i := range childFilters {
		child, err := buildFilterOp(schema, rec, &childFilters[i])
		if err != nil {
			return nil, err
		}
		if child != nil {
			children = append(children, child)
		}
	}
	return &compoundFilterOp{logic: logic, children: children}, nil
}

func resolveFilterColumn(schema arrow.Schema, rec arrow.RecordBatch, fieldPath string) ([]int, arrow.Array, error) {
	indices, _, err := resolveNestedField(schema, fieldPath)
	if err != nil {
		return nil, nil, err
	}
	if len(indices) == 0 {
		return nil, nil, nil
	}
	return indices, rec.Column(indices[0]), nil
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
// This uses vectorized FilterBatch operations for improved performance.
func (e *FilterEvaluator) MatchesBatch(rowIndices []int) []int {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesBatch").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesBatch").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		return rowIndices
	}

	result := rowIndices
	// Chain filters: output of one is input to next
	// This reduces the working set size progressively
	for _, op := range e.ops {
		result = op.FilterBatch(result)
		if len(result) == 0 {
			metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatch", "intermediate").Add(float64(len(result)))
			return nil
		}
	}
	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatch", "intermediate").Add(float64(len(result)))
	return result
}

// MatchesBatchFused evaluates all filters in a single pass without creating intermediate slices.
// This reduces memory allocations and improves cache locality compared to MatchesBatch.
func (e *FilterEvaluator) MatchesBatchFused(rowIndices []int) []int {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesBatchFused").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesBatchFused").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		return rowIndices
	}

	if len(rowIndices) == 0 {
		return nil
	}

	result := make([]int, 0, len(rowIndices))

	for _, idx := range rowIndices {
		matches := true
		for _, op := range e.ops {
			if !op.Match(idx) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, idx)
		}
	}

	if len(result) == 0 {
		return nil
	}

	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesBatchFused", "indices").Add(float64(len(result)))
	return result
}

// MatchesAll evaluates all filters on the entire batch using SIMD and returns matching row indices.
// Compound filters (AND/OR/NOT) and flat filters are handled separately for optimal performance.
func (e *FilterEvaluator) MatchesAll(batchLen int) ([]int, error) {
	start := time.Now()
	defer func() {
		metrics.FilterEvaluatorOpsTotal.WithLabelValues("MatchesAll").Inc()
		metrics.FilterEvaluatorDurationSeconds.WithLabelValues("MatchesAll").Observe(time.Since(start).Seconds())
	}()

	if len(e.ops) == 0 {
		indices := make([]int, batchLen)
		for i := 0; i < batchLen; i++ {
			indices[i] = i
		}
		return indices, nil
	}

	flatOps := make([]filterOp, 0, len(e.ops))
	compoundOps := make([]filterOp, 0, len(e.ops))
	for _, op := range e.ops {
		if op.Compound() {
			compoundOps = append(compoundOps, op)
		} else {
			flatOps = append(flatOps, op)
		}
	}

	var bitmap []byte
	if len(flatOps) > 0 {
		sortedFlat := selectOpsBySelectivity(flatOps)
		bitmap = make([]byte, batchLen)
		sortedFlat[0].MatchBitmap(bitmap)

		if isBitmapAllZeros(bitmap) {
			metrics.BloomFilterEarlyExitsTotal.Inc()
			return []int{}, nil
		}

		if len(sortedFlat) > 1 {
			tmp := make([]byte, batchLen)
			for i := 1; i < len(sortedFlat); i++ {
				sortedFlat[i].MatchBitmap(tmp)
				if err := simd.AndBytes(bitmap, tmp); err != nil {
					return nil, err
				}
				if isBitmapAllZeros(bitmap) {
					metrics.BloomFilterEarlyExitsTotal.Inc()
					return []int{}, nil
				}
			}
		}
	}

	if len(compoundOps) > 0 {
		compoundBitmap := make([]byte, batchLen)
		for _, cop := range compoundOps {
			cop.MatchBitmap(compoundBitmap)
			if bitmap == nil {
				bitmap = compoundBitmap
			} else {
				if err := simd.AndBytes(bitmap, compoundBitmap); err != nil {
					return nil, err
				}
			}
			if isBitmapAllZeros(bitmap) {
				metrics.BloomFilterEarlyExitsTotal.Inc()
				return []int{}, nil
			}
		}
	}

	if bitmap == nil {
		bitmap = make([]byte, batchLen)
		for i := range bitmap {
			bitmap[i] = 1
		}
	}

	indices := make([]int, 0, batchLen/2)
	for i, b := range bitmap {
		if b != 0 {
			indices = append(indices, i)
		}
	}
	metrics.FilterEvaluatorAllocations.WithLabelValues("MatchesAll", "indices").Add(float64(len(indices)))
	return indices, nil
}

// Reset binds the evaluator to a new record batch, reusing the existing filter operations.
func (e *FilterEvaluator) Reset(rec arrow.RecordBatch) error {
	if len(e.ops) == 0 {
		return nil
	}

	for _, op := range e.ops {
		if op.Compound() {
			if err := op.Bind(rec.Columns()[0]); err != nil {
				continue
			}
			continue
		}

		var colIdx int
		switch o := op.(type) {
		case *int64FilterOp:
			colIdx = o.colIdx
		case *float32FilterOp:
			colIdx = o.colIdx
		case *float64FilterOp:
			colIdx = o.colIdx
		case *stringFilterOp:
			colIdx = o.colIdx
		default:
			continue
		}

		if colIdx < 0 || colIdx >= int(rec.NumCols()) {
			return fmt.Errorf("column index %d out of bounds", colIdx)
		}

		col := rec.Column(colIdx)
		if err := op.Bind(col); err != nil {
			return err
		}
	}
	return nil
}

func (e *FilterEvaluator) EvaluateToArrowBoolean(mem memory.Allocator, rows int) (*array.Boolean, error) {
	if len(e.ops) == 0 {
		b := array.NewBooleanBuilder(mem)
		b.Reserve(rows)
		for i := 0; i < rows; i++ {
			b.Append(true)
		}
		return b.NewBooleanArray(), nil
	}

	flatOps := make([]filterOp, 0, len(e.ops))
	compoundOps := make([]filterOp, 0, len(e.ops))
	for _, op := range e.ops {
		if op.Compound() {
			compoundOps = append(compoundOps, op)
		} else {
			flatOps = append(flatOps, op)
		}
	}

	var bitmap []byte
	if len(flatOps) > 0 {
		bitmap = make([]byte, rows)
		flatOps[0].MatchBitmap(bitmap)
		if len(flatOps) > 1 {
			tmp := make([]byte, rows)
			for i := 1; i < len(flatOps); i++ {
				flatOps[i].MatchBitmap(tmp)
				if err := simd.AndBytes(bitmap, tmp); err != nil {
					return nil, err
				}
			}
		}
	}

	if len(compoundOps) > 0 {
		cb := make([]byte, rows)
		for _, cop := range compoundOps {
			cop.MatchBitmap(cb)
			if bitmap == nil {
				bitmap = cb
			} else {
				if err := simd.AndBytes(bitmap, cb); err != nil {
					return nil, err
				}
			}
		}
	}

	if bitmap == nil {
		bitmap = make([]byte, rows)
		for i := range bitmap {
			bitmap[i] = 1
		}
	}

	b := array.NewBooleanBuilder(mem)
	b.Reserve(rows)
	bools := make([]bool, rows)
	for i, v := range bitmap {
		bools[i] = v != 0
	}
	b.AppendValues(bools, nil)
	return b.NewBooleanArray(), nil
}

func estimateSelectivity(op filterOp, sampleSize int) float64 {
	if op.Compound() {
		return 0.5
	}

	var filterType string
	var sampleCount int

	switch o := op.(type) {
	case *int64FilterOp:
		filterType = "int64"
		sampleCount = o.col.Len()
	case *float32FilterOp:
		filterType = "float32"
		sampleCount = o.col.Len()
	case *float64FilterOp:
		filterType = "float64"
		sampleCount = o.col.Len()
	case *stringFilterOp:
		filterType = "string"
		sampleCount = o.col.Len()
	case *nestedFilterOp:
		filterType = "nested"
		return estimateSelectivity(o.op, sampleSize)
	default:
		return 0.5
	}

	if sampleCount == 0 {
		metrics.BloomFilterSelectivityHistogram.WithLabelValues(filterType).Observe(1.0)
		return 1.0
	}

	sample := sampleSize
	if sample > sampleCount {
		sample = sampleCount
	}

	matchCount := 0
	for i := 0; i < sample; i++ {
		if op.Match(i) {
			matchCount++
		}
	}

	selectivity := float64(matchCount) / float64(sample)
	metrics.BloomFilterSelectivityHistogram.WithLabelValues(filterType).Observe(selectivity)
	return selectivity
}

// isBitmapAllZeros checks if a bitmap contains all zeros (no matches).
// Uses a fast SIMD-like approach for better performance.
func isBitmapAllZeros(bitmap []byte) bool {
	metrics.BloomFilterBitmapZeroChecksTotal.Inc()

	for _, b := range bitmap {
		if b != 0 {
			return false
		}
	}
	return true
}

// selectOpsBySelectivity reorders filter operations by estimated selectivity.
// Filters with higher selectivity (fewer matches) should run first for better early exit.
func selectOpsBySelectivity(ops []filterOp) []filterOp {
	if len(ops) <= 1 {
		return ops
	}

	type selectivityPair struct {
		op          filterOp
		selectivity float64
	}

	pairs := make([]selectivityPair, len(ops))
	for i, op := range ops {
		pairs[i] = selectivityPair{op: op, selectivity: estimateSelectivity(op, 100)}
	}

	// Sort by selectivity ascending (higher selectivity = fewer matches = run first)
	// This means filters that reject more rows run first, enabling early exit.
	for i := 0; i < len(pairs)-1; i++ {
		for j := i + 1; j < len(pairs); j++ {
			if pairs[j].selectivity < pairs[i].selectivity {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}

	result := make([]filterOp, len(ops))
	for i, p := range pairs {
		result[i] = p.op
	}
	return result
}
