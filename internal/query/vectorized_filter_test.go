package query

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorizedFilter_Comprehensive(t *testing.T) {
	ctx := context.Background()
	pool := memory.NewGoAllocator()
	vf := NewVectorizedFilter(pool)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)

	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	bld.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 1, 2}, nil)
	bld.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6}, nil)
	bld.Field(2).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "a", "b"}, nil)
	bld.Field(3).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false, true, false}, nil)

	batch := bld.NewRecordBatch()
	defer batch.Release()

	t.Run("BasicOperators", func(t *testing.T) {
		filters := []Filter{
			{Field: "i64", Operator: "=", Value: "1"},
		}
		res, err := vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(2), res.NumRows())
		res.Release()
		
		filters = []Filter{
			{Field: "f64", Operator: ">", Value: "3.0"},
		}
		res, err = vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(4), res.NumRows())
		res.Release()
	})

	t.Run("InOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "str", Operator: "IN", Value: "a,c"},
		}
		res, err := vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(3), res.NumRows()) // a, c, a
		res.Release()
	})

	t.Run("CompositeFilter", func(t *testing.T) {
		filters := []Filter{
			{
				Logic: "AND",
				Filters: []Filter{
					{Field: "i64", Operator: "=", Value: "1"},
					{Field: "str", Operator: "=", Value: "a"},
				},
			},
		}
		res, err := vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(2), res.NumRows())
		res.Release()
		
		filters = []Filter{
			{
				Logic: "OR",
				Filters: []Filter{
					{Field: "i64", Operator: "=", Value: "1"},
					{Field: "i64", Operator: "=", Value: "2"},
				},
			},
		}
		res, err = vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(4), res.NumRows())
		res.Release()
	})
	
	t.Run("ContainsOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "str", Operator: "CONTAINS", Value: "a"},
		}
		res, err := vf.Apply(ctx, batch, filters)
		require.NoError(t, err)
		assert.Equal(t, int64(2), res.NumRows())
		res.Release()
	})
}
