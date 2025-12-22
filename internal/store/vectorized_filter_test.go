package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// VectorizedFilter Tests
// =============================================================================

func TestVectorizedFilterOperators(t *testing.T) {
	t.Run("parses equality operator", func(t *testing.T) {
		op, err := ParseFilterOperator("=")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if op != FilterOpEqual {
			t.Errorf("expected FilterOpEqual, got %v", op)
		}
	})

	t.Run("parses all comparison operators", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected FilterOperator
		}{
			{"=", FilterOpEqual},
			{"!=", FilterOpNotEqual},
			{">", FilterOpGreater},
			{"<", FilterOpLess},
			{">=", FilterOpGreaterEqual},
			{"<=", FilterOpLessEqual},
			{"IN", FilterOpIn},
			{"NOT IN", FilterOpNotIn},
			{"CONTAINS", FilterOpContains},
		}

		for _, tc := range testCases {
			op, err := ParseFilterOperator(tc.input)
			if err != nil {
				t.Errorf("ParseFilterOperator(%q) error: %v", tc.input, err)
				continue
			}
			if op != tc.expected {
				t.Errorf("ParseFilterOperator(%q) = %v, want %v", tc.input, op, tc.expected)
			}
		}
	})

	t.Run("returns error for invalid operator", func(t *testing.T) {
		_, err := ParseFilterOperator("INVALID")
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})
}

func TestVectorizedFilter(t *testing.T) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	// Helper to create test record
	createTestRecord := func(names []string, ages []int64, cities []string) arrow.RecordBatch {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "city", Type: arrow.BinaryTypes.String},
		}, nil)

		builder := array.NewRecordBuilder(alloc, schema)
		defer builder.Release()

		builder.Field(0).(*array.StringBuilder).AppendValues(names, nil)
		builder.Field(1).(*array.Int64Builder).AppendValues(ages, nil)
		builder.Field(2).(*array.StringBuilder).AppendValues(cities, nil)

		return builder.NewRecordBatch()
	}

	t.Run("filters with equality on string", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie"},
			[]int64{25, 30, 35},
			[]string{"NYC", "LA", "NYC"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "city", Operator: "=", Value: "NYC"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows, got %d", result.NumRows())
		}
	})

	t.Run("filters with greater than on int64", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie", "Diana"},
			[]int64{25, 30, 35, 28},
			[]string{"NYC", "LA", "NYC", "SF"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "age", Operator: ">", Value: "28"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows (Bob=30, Charlie=35), got %d", result.NumRows())
		}
	})

	t.Run("filters with IN operator", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie", "Diana"},
			[]int64{25, 30, 35, 28},
			[]string{"NYC", "LA", "Chicago", "SF"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		// IN operator uses comma-separated values
		filters := []Filter{{Field: "city", Operator: "IN", Value: "NYC,LA,SF"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 3 {
			t.Errorf("expected 3 rows (NYC, LA, SF), got %d", result.NumRows())
		}
	})

	t.Run("filters with NOT IN operator", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie", "Diana"},
			[]int64{25, 30, 35, 28},
			[]string{"NYC", "LA", "Chicago", "SF"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "city", Operator: "NOT IN", Value: "NYC,LA"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows (Chicago, SF), got %d", result.NumRows())
		}
	})

	t.Run("filters with CONTAINS operator", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice Smith", "Bob Jones", "Charlie Smith", "Diana Lee"},
			[]int64{25, 30, 35, 28},
			[]string{"NYC", "LA", "Chicago", "SF"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "name", Operator: "CONTAINS", Value: "Smith"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows (Alice Smith, Charlie Smith), got %d", result.NumRows())
		}
	})

	t.Run("combines multiple filters with AND", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie", "Diana"},
			[]int64{25, 30, 35, 25},
			[]string{"NYC", "NYC", "NYC", "LA"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{
			{Field: "city", Operator: "=", Value: "NYC"},
			{Field: "age", Operator: ">=", Value: "30"},
		}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows (Bob, Charlie), got %d", result.NumRows())
		}
	})

	t.Run("returns all rows when no filters", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob", "Charlie"},
			[]int64{25, 30, 35},
			[]string{"NYC", "LA", "Chicago"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		result, err := vf.Apply(ctx, rec, nil)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 3 {
			t.Errorf("expected 3 rows, got %d", result.NumRows())
		}
	})

	t.Run("handles missing field gracefully", func(t *testing.T) {
		rec := createTestRecord(
			[]string{"Alice", "Bob"},
			[]int64{25, 30},
			[]string{"NYC", "LA"},
		)
		defer rec.Release()

		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "nonexistent", Operator: "=", Value: "test"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		// Missing field should be skipped, returning all rows
		if result.NumRows() != 2 {
			t.Errorf("expected 2 rows, got %d", result.NumRows())
		}
	})
}

func TestVectorizedFilterFloat64(t *testing.T) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "B", "C", "D"}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{85.5, 90.0, 72.3, 95.8}, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	t.Run("filters float64 with greater than", func(t *testing.T) {
		vf := NewVectorizedFilter(alloc)
		filters := []Filter{{Field: "score", Operator: ">", Value: "85.0"}}

		result, err := vf.Apply(ctx, rec, filters)
		if err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		defer result.Release()

		if result.NumRows() != 3 {
			t.Errorf("expected 3 rows (85.5, 90.0, 95.8), got %d", result.NumRows())
		}
	})
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkVectorizedFilter_Equality(b *testing.B) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	// Create large test record
	n := 100000
	names := make([]string, n)
	ages := make([]int64, n)
	cities := make([]string, n)
	cityOptions := []string{"NYC", "LA", "Chicago", "SF", "Seattle"}

	for i := 0; i < n; i++ {
		names[i] = "User" + string(rune(i%26+65))
		ages[i] = int64(20 + i%50)
		cities[i] = cityOptions[i%len(cityOptions)]
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		{Name: "city", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.StringBuilder).AppendValues(names, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(ages, nil)
	builder.Field(2).(*array.StringBuilder).AppendValues(cities, nil)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	vf := NewVectorizedFilter(alloc)
	filters := []Filter{{Field: "city", Operator: "=", Value: "NYC"}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := vf.Apply(ctx, rec, filters)
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkVectorizedFilter_IN(b *testing.B) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	n := 100000
	cities := make([]string, n)
	cityOptions := []string{"NYC", "LA", "Chicago", "SF", "Seattle", "Boston", "Miami", "Denver"}

	for i := 0; i < n; i++ {
		cities[i] = cityOptions[i%len(cityOptions)]
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "city", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.StringBuilder).AppendValues(cities, nil)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	vf := NewVectorizedFilter(alloc)
	filters := []Filter{{Field: "city", Operator: "IN", Value: "NYC,LA,SF"}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := vf.Apply(ctx, rec, filters)
		if result != nil {
			result.Release()
		}
	}
}

func BenchmarkVectorizedFilter_MultipleFilters(b *testing.B) {
	alloc := memory.NewGoAllocator()
	ctx := context.Background()

	n := 100000
	ages := make([]int64, n)
	cities := make([]string, n)
	cityOptions := []string{"NYC", "LA", "Chicago", "SF", "Seattle"}

	for i := 0; i < n; i++ {
		ages[i] = int64(20 + i%50)
		cities[i] = cityOptions[i%len(cityOptions)]
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		{Name: "city", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues(ages, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(cities, nil)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	vf := NewVectorizedFilter(alloc)
	filters := []Filter{
		{Field: "city", Operator: "=", Value: "NYC"},
		{Field: "age", Operator: ">=", Value: "30"},
		{Field: "age", Operator: "<", Value: "50"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := vf.Apply(ctx, rec, filters)
		if result != nil {
			result.Release()
		}
	}
}
