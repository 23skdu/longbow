package query

import (
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterEvaluator_AllTypes(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			{Name: "str", Type: arrow.BinaryTypes.String},
			{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
	builder.Field(1).(*array.Uint64Builder).AppendValues([]uint64{100, 200, 300}, nil)
	builder.Field(2).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2, 3.3}, nil)
	builder.Field(3).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	builder.Field(4).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	t.Run("Int32_Eq", func(t *testing.T) {
		filters := []core.Filter{{Field: "i32", Operator: "=", Value: "20"}}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		assert.False(t, eval.Matches(0))
		assert.True(t, eval.Matches(1))
		assert.False(t, eval.Matches(2))
	})

	t.Run("Uint64_Gt", func(t *testing.T) {
		filters := []core.Filter{{Field: "u64", Operator: ">", Value: "150"}}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		assert.False(t, eval.Matches(0))
		assert.True(t, eval.Matches(1))
		assert.True(t, eval.Matches(2))
	})

	t.Run("Float32_Lt", func(t *testing.T) {
		filters := []core.Filter{{Field: "f32", Operator: "<", Value: "2.5"}}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		assert.True(t, eval.Matches(0))
		assert.True(t, eval.Matches(1))
		assert.False(t, eval.Matches(2))
	})

	t.Run("String_Neq", func(t *testing.T) {
		filters := []core.Filter{{Field: "str", Operator: "!=", Value: "b"}}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		assert.True(t, eval.Matches(0))
		assert.False(t, eval.Matches(1))
		assert.True(t, eval.Matches(2))
	})

	t.Run("Bool_Eq", func(t *testing.T) {
		filters := []core.Filter{{Field: "bool", Operator: "=", Value: "true"}}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		assert.True(t, eval.Matches(0))
		assert.False(t, eval.Matches(1))
		assert.True(t, eval.Matches(2))
	})
}

func TestFilterEvaluator_Compound(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "val", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	t.Run("AND", func(t *testing.T) {
		filters := []core.Filter{
			{
				Logic: "AND",
				Filters: []core.Filter{
					{Field: "val", Operator: ">", Value: "2"},
					{Field: "val", Operator: "<", Value: "5"},
				},
			},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		matches := eval.MatchesBatch([]int{0, 1, 2, 3, 4})
		assert.Equal(t, []int{2, 3}, matches)
	})

	t.Run("OR", func(t *testing.T) {
		filters := []core.Filter{
			{
				Logic: "OR",
				Filters: []core.Filter{
					{Field: "val", Operator: "=", Value: "1"},
					{Field: "val", Operator: "=", Value: "5"},
				},
			},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		matches := eval.MatchesBatch([]int{0, 1, 2, 3, 4})
		assert.Equal(t, []int{0, 4}, matches)
	})

	t.Run("NOT", func(t *testing.T) {
		filters := []core.Filter{
			{
				Logic: "NOT",
				Filters: []core.Filter{
					{Field: "val", Operator: ">", Value: "2"},
				},
			},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)
		matches := eval.MatchesBatch([]int{0, 1, 2, 3, 4})
		assert.Equal(t, []int{0, 1}, matches)
	})
}

func TestFilterEvaluator_Nested(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(
		arrow.Field{Name: "sub", Type: arrow.PrimitiveTypes.Int32},
	)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "nested", Type: structType},
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	sb := builder.Field(0).(*array.StructBuilder)
	subB := sb.FieldBuilder(0).(*array.Int32Builder)
	
	sb.Append(true)
	subB.Append(10)
	sb.Append(true)
	subB.Append(20)

	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	filters := []core.Filter{{Field: "nested.sub", Operator: "=", Value: "20"}}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)
	assert.False(t, eval.Matches(0))
	assert.True(t, eval.Matches(1))
}

func TestFilterEvaluator_MatchesAll_Extended(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "val", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	filters := []core.Filter{{Field: "val", Operator: ">", Value: "3"}}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	matches, err := eval.MatchesAll(5)
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 4}, matches)
}
