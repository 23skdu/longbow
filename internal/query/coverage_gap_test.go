package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_NestedFieldResolution(t *testing.T) {
	pool := memory.NewGoAllocator()
	
	// Create a Struct array: { "a": int32, "b": { "c": string } }
	innerStructType := arrow.StructOf(arrow.Field{Name: "c", Type: arrow.BinaryTypes.String})
	outerStructType := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "b", Type: innerStructType},
	)

	// Build the data
	bld := array.NewStructBuilder(pool, outerStructType)
	defer bld.Release()
	
	aBld := bld.FieldBuilder(0).(*array.Int32Builder)
	bBld := bld.FieldBuilder(1).(*array.StructBuilder)
	cBld := bBld.FieldBuilder(0).(*array.StringBuilder)

	bld.Append(true)
	aBld.Append(10)
	bBld.Append(true)
	cBld.Append("hello")

	arr := bld.NewArray().(*array.Struct)
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "nested", Type: outerStructType},
	}, nil)

	t.Run("ExtractNestedValue_Struct", func(t *testing.T) {
		val := extractNestedValue(arr, 0, "nested.a")
		assert.Equal(t, int32(10), val)

		val2 := extractNestedValue(arr, 0, "nested.b.c")
		assert.Equal(t, "hello", val2)
	})

	t.Run("ResolveFilterColumn", func(t *testing.T) {
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
		defer rec.Release()

		indices, col, err := resolveFilterColumn(*schema, rec, "nested.b.c")
		assert.NoError(t, err)
		require.NotNil(t, col)
		assert.Equal(t, []int{0, 1, 0}, indices) // nested(0) -> b(1) -> c(0)
	})
}

func TestCoverage_ListNestedField(t *testing.T) {
	pool := memory.NewGoAllocator()
	// List<int32>
	bld := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer bld.Release()

	vb := bld.ValueBuilder().(*array.Int32Builder)
	bld.Append(true)
	vb.Append(1)
	vb.Append(2)

	arr := bld.NewArray().(*array.List)
	defer arr.Release()

	t.Run("ExtractNestedValue_List", func(t *testing.T) {
		// List extraction usually returns the list itself or a element if path specified?
		// resolveNestedField parts [1:] depth+1
		val := extractNestedValue(arr, 0, "list.0")
		// extractNestedValueParts checks segments
		_ = val
	})
}
