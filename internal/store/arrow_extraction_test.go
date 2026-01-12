package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractVectorAny(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("Float32_ZeroCopy", func(t *testing.T) {
		dims := 4
		builder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
		defer builder.Release()

		vb := builder.ValueBuilder().(*array.Float32Builder)
		vb.AppendValues([]float32{1.0, 2.0, 3.0, 4.0}, nil)
		builder.Append(true)
		vb.AppendValues([]float32{5.0, 6.0, 7.0, 8.0}, nil)
		builder.Append(true)

		arr := builder.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arr.DataType()}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 2)
		defer rec.Release()

		// Extract first row
		anyVec, err := ExtractVectorAny(rec, 0, 0)
		assert.NoError(t, err)
		vec, ok := anyVec.([]float32)
		require.True(t, ok)
		assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0}, vec)

		// Extract second row
		anyVec, err = ExtractVectorAny(rec, 1, 0)
		assert.NoError(t, err)
		vec, ok = anyVec.([]float32)
		require.True(t, ok)
		assert.Equal(t, []float32{5.0, 6.0, 7.0, 8.0}, vec)
	})

	t.Run("Int8_ZeroCopy", func(t *testing.T) {
		dims := 3
		builder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Int8)
		defer builder.Release()

		vb := builder.ValueBuilder().(*array.Int8Builder)
		vb.AppendValues([]int8{1, 2, 3}, nil)
		builder.Append(true)

		arr := builder.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arr.DataType()}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
		defer rec.Release()

		anyVec, err := ExtractVectorAny(rec, 0, 0)
		assert.NoError(t, err)
		vec, ok := anyVec.([]int8)
		require.True(t, ok)
		assert.Equal(t, []int8{1, 2, 3}, vec)
	})

	t.Run("Casting_To_Float32", func(t *testing.T) {
		dims := 2
		builder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Int8)
		defer builder.Release()

		vb := builder.ValueBuilder().(*array.Int8Builder)
		vb.AppendValues([]int8{10, 20}, nil)
		builder.Append(true)

		arr := builder.NewArray().(*array.FixedSizeList)
		defer arr.Release()

		schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arr.DataType()}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
		defer rec.Release()

		// Call the regular ExtractVectorFromArrow which should cast
		vec, err := ExtractVectorFromArrow(rec, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, []float32{10.0, 20.0}, vec)
	})
}
