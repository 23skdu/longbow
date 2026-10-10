package query

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

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

func (n *nestedFilterOp) Reset(rec arrow.RecordBatch) error {
	// Re-resolve nested field for new record batch
	indices, col, _, err := resolveFilterColumnEx(*rec.Schema(), rec, n.fieldPath)
	if err != nil {
		return err
	}
	if col == nil {
		return fmt.Errorf("nested field %s not found in record batch", n.fieldPath)
	}
	n.outerCol = col
	n.colIndices = indices
	return n.op.Reset(rec)
}

func (n *nestedFilterOp) MatchValue(val interface{}) bool {
	return n.op.MatchValue(val)
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

func (s *subqueryFilterOp) Reset(rec arrow.RecordBatch) error {
	_, col, _, err := resolveFilterColumnEx(*rec.Schema(), rec, s.field)
	if err != nil {
		return err
	}
	if col == nil {
		return fmt.Errorf("subquery field %s not found in record batch", s.field)
	}
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
