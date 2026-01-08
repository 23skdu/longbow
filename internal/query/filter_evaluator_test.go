package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestFilterEvaluator_Reuse(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	// Batch 1
	b1 := array.NewRecordBuilder(mem, schema)
	b1.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 10}, nil)
	b1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2, 3.3, 10.1}, nil)
	rec1 := b1.NewRecordBatch()
	defer rec1.Release()

	// Batch 2
	b2 := array.NewRecordBuilder(mem, schema)
	b2.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30, 40}, nil)
	b2.Field(1).(*array.Float32Builder).AppendValues([]float32{10.1, 20.2, 30.3, 40.4}, nil)
	rec2 := b2.NewRecordBatch()
	defer rec2.Release()

	filters := []Filter{
		{Field: "id", Operator: "=", Value: "10"},
	}

	// 1. New Evaluator bound to rec1
	eval, err := NewFilterEvaluator(rec1, filters)
	require.NoError(t, err)

	// Verify matches on rec1 (index 3 is 10)
	require.False(t, eval.Matches(0))
	require.True(t, eval.Matches(3))

	// Verify EvaluateToArrowBoolean on rec1
	mask1, err := eval.EvaluateToArrowBoolean(mem, int(rec1.NumRows()))
	require.NoError(t, err)
	defer mask1.Release()
	require.True(t, mask1.IsValid(3))
	require.True(t, mask1.Value(3))
	require.False(t, mask1.Value(0))

	// 2. Reuse Evaluator on rec2
	err = eval.Reset(rec2)
	require.NoError(t, err)

	// Verify matches on rec2 (index 0 is 10)
	require.True(t, eval.Matches(0))
	require.False(t, eval.Matches(1))

	// Verify EvaluateToArrowBoolean on rec2
	mask2, err := eval.EvaluateToArrowBoolean(mem, int(rec2.NumRows()))
	require.NoError(t, err)
	defer mask2.Release()
	require.True(t, mask2.Value(0))
	require.False(t, mask2.Value(1))
}
