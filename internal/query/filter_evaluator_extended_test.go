package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterEvaluator_VariousTypes(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32},
			{Name: "uint64_col", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64},
			{Name: "string_col", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
	b.Field(1).(*array.Uint64Builder).AppendValues([]uint64{100, 200, 300}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	b.Field(3).(*array.StringBuilder).AppendValues([]string{"apple", "banana", "cherry"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	tests := []struct {
		name     string
		filters  []Filter
		expected []int
	}{
		{
			"int32 match",
			[]Filter{{Field: "int32_col", Operator: "=", Value: "20"}},
			[]int{1},
		},
		{
			"uint64 match",
			[]Filter{{Field: "uint64_col", Operator: ">", Value: "150"}},
			[]int{1, 2},
		},
		{
			"float64 match",
			[]Filter{{Field: "float64_col", Operator: "<", Value: "2.5"}},
			[]int{0, 1},
		},
		{
			"string match",
			[]Filter{{Field: "string_col", Operator: "=", Value: "banana"}},
			[]int{1},
		},
		{
			"string complex match",
			[]Filter{{Field: "string_col", Operator: "!=", Value: "apple"}},
			[]int{1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eval, err := NewFilterEvaluator(rec, tt.filters)
			require.NoError(t, err)
			
			matches, err := eval.MatchesAll(int(rec.NumRows()))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, matches)
		})
	}
}

/*
func TestFilterEvaluator_NestedFields(t *testing.T) {
	pool := memory.NewGoAllocator()
	// Metadata column usually contains JSON strings in Longbow
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "metadata", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{
		`{"user": {"id": 1, "name": "alice"}}`,
		`{"user": {"id": 2, "name": "bob"}}`,
	}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	// Testing nested field resolution
	filters := []Filter{{Field: "metadata.user.id", Operator: "=", Value: "2"}}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	matches, err := eval.MatchesAll(int(rec.NumRows()))
	require.NoError(t, err)
	assert.Equal(t, []int{1}, matches)
}
*/
