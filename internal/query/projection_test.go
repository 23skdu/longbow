package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestNewProjection(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	t.Run("AllColumns", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{"id", "name", "value"})
		require.NoError(t, err)
		require.Equal(t, []string{"id", "name", "value"}, proj.Columns)
	})

	t.Run("SingleColumn", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{"id"})
		require.NoError(t, err)
		require.Equal(t, []string{"id"}, proj.Columns)
	})

	t.Run("EmptyColumns", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{})
		require.NoError(t, err)
		require.Nil(t, proj.Columns)
	})

	t.Run("InvalidColumn", func(t *testing.T) {
		_, err := NewProjection(schema, []string{"id", "invalid"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})
}

func TestProjection_Apply(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	t.Run("SelectTwoColumns", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{"id", "name"})
		require.NoError(t, err)

		result, err := proj.Apply(rec)
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(2), result.NumCols())
		require.Equal(t, int64(3), result.NumRows())
	})

	t.Run("SelectOneColumn", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{"value"})
		require.NoError(t, err)

		result, err := proj.Apply(rec)
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(1), result.NumCols())
	})

	t.Run("EmptyProjection", func(t *testing.T) {
		proj, err := NewProjection(schema, []string{})
		require.NoError(t, err)

		result, err := proj.Apply(rec)
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(3), result.NumCols())
	})
}

func TestProjection_CanPushdown(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	t.Run("AllFilterColsInProjection", func(t *testing.T) {
		proj, _ := NewProjection(schema, []string{"id", "category"})
		filters := []Filter{
			{Field: "id", Operator: ">", Value: "10"},
			{Field: "category", Operator: "=", Value: "1"},
		}
		require.True(t, proj.CanPushdown(filters))
	})

	t.Run("FilterColNotInProjection", func(t *testing.T) {
		proj, _ := NewProjection(schema, []string{"id"})
		filters := []Filter{
			{Field: "category", Operator: "=", Value: "1"},
		}
		require.False(t, proj.CanPushdown(filters))
	})

	t.Run("EmptyProjection", func(t *testing.T) {
		proj, _ := NewProjection(schema, []string{})
		filters := []Filter{
			{Field: "id", Operator: ">", Value: "10"},
		}
		require.True(t, proj.CanPushdown(filters))
	})
}

func TestProjectionEvaluator(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	t.Run("ApplyToRecord", func(t *testing.T) {
		proj, _ := NewProjection(schema, []string{"id", "name"})
		eval, err := NewProjectionEvaluator(proj, schema)
		require.NoError(t, err)

		result, err := eval.ApplyToRecord(rec)
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(2), result.NumCols())
	})

	t.Run("ApplyToBatch", func(t *testing.T) {
		proj, _ := NewProjection(schema, []string{"id", "value"})
		eval, err := NewProjectionEvaluator(proj, schema)
		require.NoError(t, err)

		batch := array.NewRecordBatch(schema, []arrow.Array{
			rec.Column(0),
			rec.Column(1),
			rec.Column(2),
		}, 2)

		result, err := eval.ApplyToBatch(batch)
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(2), result.NumCols())
	})
}

func TestProjectRecord(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	t.Run("SelectColumns", func(t *testing.T) {
		result, err := ProjectRecord(mem, rec, []string{"id", "name"})
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(2), result.NumCols())
		require.Equal(t, int64(3), result.NumRows())
	})

	t.Run("EmptyColumns", func(t *testing.T) {
		result, err := ProjectRecord(mem, rec, []string{})
		require.NoError(t, err)
		defer result.Release()

		require.Equal(t, int64(3), result.NumCols())
	})
}

func BenchmarkProjection_Apply(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "f1", Type: arrow.PrimitiveTypes.Float64},
		{Name: "f2", Type: arrow.PrimitiveTypes.Float64},
		{Name: "f3", Type: arrow.PrimitiveTypes.Float64},
		{Name: "f4", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	numRows := 10000
	builder := array.NewRecordBuilder(mem, schema)
	for i := 0; i < numRows; i++ {
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{int64(i)}, nil)
		for j := 1; j < 4; j++ {
			builder.Field(j).(*array.Float64Builder).AppendValues([]float64{float64(i)}, nil)
		}
	}
	rec := builder.NewRecord()
	defer rec.Release()

	proj, _ := NewProjection(schema, []string{"id", "f1", "f2"})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = proj.Apply(rec)
	}
}
