package query

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/23skdu/longbow/internal/store/types"
)

func TestQuery_EvaluatorBranches(t *testing.T) {
	pool := memory.NewGoAllocator()
	
	// Complex schema with overlapping field names and nesting
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "meta", Type: arrow.StructOf(
			arrow.Field{Name: "user", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		)},
	}, nil)

	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()

	bld.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	
	metaBld := bld.Field(1).(*array.StructBuilder)
	userBld := metaBld.FieldBuilder(0).(*array.StringBuilder)
	tagsBld := metaBld.FieldBuilder(1).(*array.ListBuilder)
	tagValBld := tagsBld.ValueBuilder().(*array.StringBuilder)

	metaBld.Append(true)
	userBld.Append("alice")
	tagsBld.Append(true)
	tagValBld.Append("admin")
	tagValBld.Append("dev")

	metaBld.Append(true)
	userBld.Append("bob")
	tagsBld.Append(true)
	tagValBld.Append("dev")

	metaBld.Append(false) // NULL meta

	rec := bld.NewRecord()
	defer rec.Release()

	t.Run("NestedResolution_Depth", func(t *testing.T) {
		indices, dt, err := resolveNestedField(*schema, "meta.user")
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 0}, indices)
		assert.Equal(t, arrow.BinaryTypes.String, dt)

		indices, dt, err = resolveNestedField(*schema, "meta.tags")
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 1}, indices)
		assert.Equal(t, arrow.ListOf(arrow.BinaryTypes.String), dt)
	})

	t.Run("CompoundFilter", func(t *testing.T) {
		// Test filter evaluation
		filter := Filter{
			Field:    "id",
			Operator: "=",
			Value:    "1",
		}
		eval, err := NewFilterEvaluator(rec, []Filter{filter})
		require.NoError(t, err)

		// Check if row 0 matches (id=1)
		assert.True(t, eval.Matches(0))
		assert.False(t, eval.Matches(1)) // bob (id=2)
	})
}

func TestQuery_ZeroAllocParsers(t *testing.T) {
	logger := zerolog.Nop()
	
	t.Run("VectorSearchParser", func(t *testing.T) {
		parser := NewZeroAllocVectorSearchParser(128, &logger)
		data := []byte(`{"dataset":"test","k":10,"vector":[0.1,0.2],"alpha":0.5}`)
		
		req, err := parser.Parse(data)
		assert.NoError(t, err)
		assert.Equal(t, "test", req.Dataset)
		assert.Equal(t, 10, req.K)
		assert.Equal(t, []float32{0.1, 0.2}, req.Vector)
		assert.InDelta(t, 0.5, req.Alpha, 0.001)

		// Test error path: unknown field
		_, err = parser.Parse([]byte(`{"dataset":"test","unknown":123}`))
		assert.Error(t, err)
	})

	t.Run("TicketParser", func(t *testing.T) {
		parser := NewZeroAllocTicketParser(&logger)
		data := []byte(`{"name":"t1","limit":100}`)
		
		req, err := parser.Parse(data)
		assert.NoError(t, err)
		assert.Equal(t, "t1", req.Name)
		assert.Equal(t, int64(100), req.Limit)
	})
}

func TestQuery_FilterOperators(t *testing.T) {
	ops := []string{"=", "!=", ">", ">=", "<", "<=", "IN", "NOT IN", "CONTAINS", "LIKE"}
	for _, op := range ops {
		res, err := ParseFilterOperator(op)
		assert.NoError(t, err, "Failed for op: %s", op)
		assert.GreaterOrEqual(t, int(res), 0)
	}
	
	_, err := ParseFilterOperator("INVALID")
	assert.Error(t, err)
}

func TestQuery_BitmapLarge(t *testing.T) {
	b := types.NewBitset()
	for i := 0; i < 1000; i += 2 {
		b.Set(i)
	}
	
	assert.Equal(t, uint64(500), b.Count())
	
	clone := b.Clone()
	assert.Equal(t, uint64(500), clone.Count())
	
	b.Clear(0)
	assert.Equal(t, uint64(499), b.Count())
}

func TestQuery_ProjectionApply(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}}, nil)
	bld := array.NewRecordBuilder(pool, schema)
	defer bld.Release()
	bld.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	rec := bld.NewRecord()
	defer rec.Release()

	proj, err := NewProjection(schema, []string{"a"})
	res, err := proj.Apply(rec)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), res.NumRows())
	res.Release()

	t.Run("FastPath_Int32", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32}}, nil)
		bld := array.NewRecordBuilder(pool, schema)
		defer bld.Release()
		bld.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20, 30}, nil)
		rec := bld.NewRecordBatch()
		defer rec.Release()

		filter := Filter{Field: "a", Operator: ">", Value: "15"}
		eval, err := NewFilterEvaluator(rec, []Filter{filter})
		require.NoError(t, err)
		assert.False(t, eval.Matches(0))
		assert.True(t, eval.Matches(1))
	})

	t.Run("FastPath_Uint64", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Uint64}}, nil)
		bld := array.NewRecordBuilder(pool, schema)
		defer bld.Release()
		bld.Field(0).(*array.Uint64Builder).AppendValues([]uint64{100, 200}, nil)
		rec := bld.NewRecordBatch()
		defer rec.Release()

		filter := Filter{Field: "a", Operator: "<=", Value: "150"}
		eval, err := NewFilterEvaluator(rec, []Filter{filter})
		require.NoError(t, err)
		assert.True(t, eval.Matches(0))
		assert.False(t, eval.Matches(1))
	})

	t.Run("Float64_String_Tests", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			{Name: "str", Type: arrow.BinaryTypes.String},
		}, nil)
		bld := array.NewRecordBuilder(pool, schema)
		defer bld.Release()
		bld.Field(0).(*array.Float64Builder).AppendValues([]float64{1.23, 4.56}, nil)
		bld.Field(1).(*array.StringBuilder).AppendValues([]string{"foo", "bar"}, nil)
		rec := bld.NewRecordBatch()
		defer rec.Release()

		// Float64
		eval, err := NewFilterEvaluator(rec, []Filter{{Field: "f64", Operator: "gt", Value: "2.0"}})
		require.NoError(t, err)
		assert.True(t, eval.Matches(1))

		// String
		eval, err = NewFilterEvaluator(rec, []Filter{{Field: "str", Operator: "=", Value: "foo"}})
		require.NoError(t, err)
		assert.True(t, eval.Matches(0))
	})

	t.Run("NestedField_Resolution", func(t *testing.T) {
		// Mock a nested schema: user.profile.age
		innerProfile := arrow.StructOf(arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int64})
		userType := arrow.StructOf(arrow.Field{Name: "profile", Type: innerProfile})
		schema := arrow.NewSchema([]arrow.Field{{Name: "user", Type: userType}}, nil)
		
		// This is complex to build properly with builders for coverage, 
		// but we can at least attempt to call resolveNestedField.
		_, _, err := resolveNestedField(*schema, "user.profile.age")
		assert.NoError(t, err)

		_, _, err = resolveNestedField(*schema, "invalid.field")
		assert.Error(t, err)
	})
}
