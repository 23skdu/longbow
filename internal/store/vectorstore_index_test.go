package store


import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/rs/zerolog"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestVectorStore_IndexRecordColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	// Set columns to index
	store.SetIndexedColumns([]string{"category"})

	// Create test record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B", "A"}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Index the record
	store.IndexRecordColumns("test-dataset", rec, 0)

	// Verify index was created
	if !store.columnIndex.HasIndex("test-dataset", "category") {
		t.Error("Expected index to be created for category column")
	}

	// Lookup should return correct rows
	rows := store.columnIndex.GetMatchingRowIndices("test-dataset", 0, "category", "A")
	if len(rows) != 2 {
		t.Errorf("Expected 2 matching rows for category=A, got %d", len(rows))
	}
}

func TestVectorStore_IndexRecordColumns_NoIndexedColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	// Don't set any indexed columns - should be a no-op
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Should not panic when no indexed columns configured
	store.IndexRecordColumns("test-dataset", rec, 0)

	// Verify no index was created
	if store.columnIndex.HasIndex("test-dataset", "id") {
		t.Error("Expected no index to be created when indexedColumns is empty")
	}
}

func TestVectorStore_GetSetIndexedColumns(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	// Initially empty
	if len(store.GetIndexedColumns()) != 0 {
		t.Error("Expected initially empty indexed columns")
	}

	// Set columns
	store.SetIndexedColumns([]string{"col1", "col2"})
	cols := store.GetIndexedColumns()
	if len(cols) != 2 || cols[0] != "col1" || cols[1] != "col2" {
		t.Errorf("Expected [col1, col2], got %v", cols)
	}
}

func TestVectorStore_FilterRecordOptimized_NoFilters(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// No filters should return record as-is
	result, err := store.filterRecordOptimized(context.Background(), "test", rec, 0, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows())
	}
}

func TestVectorStore_FilterRecordOptimized_WithIndex(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	store.SetIndexedColumns([]string{"category"})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "A"}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Index the record first
	store.IndexRecordColumns("test-dataset", rec, 0)

	// Filter using indexed column
	filters := []query.Filter{{Field: "category", Operator: "=", Value: "TARGET"}}
	result, err := store.filterRecordOptimized(context.Background(), "test-dataset", rec, 0, filters)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer result.Release()

	// Should return rows with category=A (rows 0, 2, 4)
	if result.NumRows() != 3 {
		t.Errorf("Expected 3 rows with category=A, got %d", result.NumRows())
	}
}

func TestVectorStore_FilterRecordOptimized_FallbackToCompute(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	// Don't index any columns - will fallback to Arrow compute
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Filter on non-indexed column using non-equality operator
	filters := []query.Filter{{Field: "value", Operator: ">", Value: "15"}}
	result, err := store.filterRecordOptimized(context.Background(), "test", rec, 0, filters)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer result.Release()

	// Should return rows where value > 15 (rows 1, 2)
	if result.NumRows() != 2 {
		t.Errorf("Expected 2 rows with value>15, got %d", result.NumRows())
	}
}

func BenchmarkFilterRecordOptimized_WithIndex(b *testing.B) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	store.SetIndexedColumns([]string{"category"})

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create large record for benchmarking
	builder := array.NewRecordBuilder(mem, schema)
	ids := make([]int64, 10000)
	categories := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		ids[i] = int64(i)
		if i%100 == 0 {
			categories[i] = "TARGET"
		} else {
			categories[i] = "OTHER"
		}
	}
	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(categories, nil)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	// Index the record
	store.IndexRecordColumns("benchmark", rec, 0)

	filters := []query.Filter{{Field: "category", Operator: "=", Value: "TARGET"}}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := store.filterRecordOptimized(ctx, "benchmark", rec, 0, filters)
		result.Release()
	}
}

func BenchmarkFilterRecord_WithoutIndex(b *testing.B) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 0, 0, 0)
	defer func() { _ = store.Close() }()

	// No indexed columns - uses full Arrow compute
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create large record for benchmarking
	builder := array.NewRecordBuilder(mem, schema)
	ids := make([]int64, 10000)
	categories := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		ids[i] = int64(i)
		if i%100 == 0 {
			categories[i] = "TARGET"
		} else {
			categories[i] = "OTHER"
		}
	}
	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(categories, nil)
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	filters := []query.Filter{{Field: "category", Operator: "=", Value: "TARGET"}}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := filterRecord(ctx, store.mem, rec, filters)
		result.Release()
	}
}
