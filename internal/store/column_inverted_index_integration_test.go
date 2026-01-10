package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestColumnInvertedIndex_FilterWithIndex tests filterRecord using index
func TestColumnInvertedIndex_FilterWithIndex(t *testing.T) {
	mem := memory.NewGoAllocator()
	idx := NewColumnInvertedIndex()

	// Create test record with 5 rows
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "A"}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"active", "inactive", "active", "active", "pending"}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Index the category and status columns
	idx.IndexRecord("test", 0, rec, []string{"category", "status"})

	// Test: lookup category="A" should return 3 rows (indices 0, 2, 4)
	rows := idx.Lookup("test", "category", "A")
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows for category=A, got %d", len(rows))
	}

	// Verify the row indices
	expected := map[int]bool{0: true, 2: true, 4: true}
	for _, r := range rows {
		if !expected[r.RowIdx] {
			t.Errorf("Unexpected row index: %d", r.RowIdx)
		}
	}
}

// TestColumnInvertedIndex_BuildFilterMask tests building a filter mask from index
func TestColumnInvertedIndex_BuildFilterMask(t *testing.T) {
	mem := memory.NewGoAllocator()
	idx := NewColumnInvertedIndex()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "A"}, nil)
	rec := bldr.NewRecordBatch()
	bldr.Release()
	defer rec.Release()

	idx.IndexRecord("test", 0, rec, []string{"category"})

	// Build filter mask for category="A"
	mask := idx.BuildFilterMask("test", 0, "category", "A", int(rec.NumRows()), mem)
	if mask == nil {
		t.Fatal("BuildFilterMask returned nil")
	}
	defer mask.Release()

	if mask.Len() != 5 {
		t.Errorf("Expected mask length 5, got %d", mask.Len())
	}

	// Check mask values: true at 0, 2, 4; false at 1, 3
	expectedMask := []bool{true, false, true, false, true}
	for i, exp := range expectedMask {
		if mask.Value(i) != exp {
			t.Errorf("Mask[%d]: expected %v, got %v", i, exp, mask.Value(i))
		}
	}
}

// TestColumnInvertedIndex_FilterRecordWithIndex tests full filter integration
func TestColumnInvertedIndex_FilterRecordWithIndex(t *testing.T) {
	mem := memory.NewGoAllocator()
	idx := NewColumnInvertedIndex()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{100, 200, 300, 400, 500}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"X", "Y", "X", "Z", "X"}, nil)
	rec := bldr.NewRecordBatch()
	bldr.Release()
	defer rec.Release()

	idx.IndexRecord("ds", 0, rec, []string{"category"})

	// Use FilterRecordWithIndex - should use O(1) lookup
	filter := query.Filter{Field: "category", Operator: "=", Value: "X"}
	filtered, err := idx.FilterRecordWithIndex(context.Background(), "ds", 0, rec, &filter, mem)
	if err != nil {
		t.Fatalf("FilterRecordWithIndex failed: %v", err)
	}
	defer filtered.Release()

	// Should have 3 rows with ids 100, 300, 500
	if filtered.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", filtered.NumRows())
	}

	idCol := filtered.Column(0).(*array.Int64)
	for i := 0; i < int(filtered.NumRows()); i++ {
		val := idCol.Value(i)
		if val != 100 && val != 300 && val != 500 {
			t.Errorf("Unexpected id value: %d", val)
		}
	}
}

// BenchmarkColumnInvertedIndex_FilterWithIndex benchmarks indexed vs non-indexed
func BenchmarkColumnInvertedIndex_FilterWithIndex(b *testing.B) {
	mem := memory.NewGoAllocator()
	idx := NewColumnInvertedIndex()

	// Create large record (10k rows)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	values := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		values[i] = "cat" + string(rune('0'+i%10))
	}
	bldr.Field(0).(*array.StringBuilder).AppendValues(values, nil)
	rec := bldr.NewRecordBatch()
	bldr.Release()
	defer rec.Release()

	idx.IndexRecord("ds", 0, rec, []string{"category"})

	filter := query.Filter{Field: "category", Operator: "=", Value: "cat5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filtered, _ := idx.FilterRecordWithIndex(context.Background(), "ds", 0, rec, &filter, mem)
		filtered.Release()
	}
}
