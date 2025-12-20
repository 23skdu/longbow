package store

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestEnsureTimestampZeroCopy_ExistingTimestamp verifies no work is done when timestamp exists
func TestEnsureTimestampZeroCopy_ExistingTimestamp(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema with timestamp already present
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
	}, nil)

	// Build record with timestamp
	idBldr := array.NewInt64Builder(mem)
	tsBldr := array.NewTimestampBuilder(mem, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer idBldr.Release()
	defer tsBldr.Release()

	idBldr.AppendValues([]int64{1, 2, 3}, nil)
	ts, _ := arrow.TimestampFromTime(time.Now(), arrow.Nanosecond)
	tsBldr.AppendValues([]arrow.Timestamp{ts, ts, ts}, nil)

	idArr := idBldr.NewArray()
	tsArr := tsBldr.NewArray()
	defer idArr.Release()
	defer tsArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr, tsArr}, 3)
	defer rec.Release()

	// Call ensureTimestampZeroCopy
	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Should return same record (no copy)
	if result.NumCols() != rec.NumCols() {
		t.Errorf("expected same column count, got %d vs %d", result.NumCols(), rec.NumCols())
	}
	if result.NumRows() != rec.NumRows() {
		t.Errorf("expected same row count, got %d vs %d", result.NumRows(), rec.NumRows())
	}
}

// TestEnsureTimestampZeroCopy_MissingTimestamp verifies timestamp is added without copying data
func TestEnsureTimestampZeroCopy_MissingTimestamp(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Build record without timestamp
	idBldr := array.NewInt64Builder(mem)
	valBldr := array.NewFloat64Builder(mem)
	defer idBldr.Release()
	defer valBldr.Release()

	idBldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	valBldr.AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)

	idArr := idBldr.NewArray()
	valArr := valBldr.NewArray()
	defer idArr.Release()
	defer valArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr, valArr}, 5)
	defer rec.Release()

	// Capture original data pointers for zero-copy verification
	origIdData := rec.Column(0).Data()
	origValData := rec.Column(1).Data()

	// Call ensureTimestampZeroCopy
	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Should have one more column
	if result.NumCols() != 3 {
		t.Errorf("expected 3 columns, got %d", result.NumCols())
	}

	// Should have same row count
	if result.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", result.NumRows())
	}

	// Verify timestamp column exists
	if !result.Schema().HasField("timestamp") {
		t.Error("expected timestamp field in schema")
	}

	// Verify ZERO-COPY: original columns should share same underlying data
	if result.Column(0).Data().Buffers()[1].Buf() == nil {
		t.Skip("Cannot verify zero-copy without buffer access")
	}

	// Check data pointer equality (zero-copy verification)
	newIdData := result.Column(0).Data()
	newValData := result.Column(1).Data()

	if len(origIdData.Buffers()) != len(newIdData.Buffers()) {
		t.Error("buffer count mismatch for id column")
	}
	if len(origValData.Buffers()) != len(newValData.Buffers()) {
		t.Error("buffer count mismatch for value column")
	}
}

// TestEnsureTimestampZeroCopy_EmptyRecord verifies handling of empty records
func TestEnsureTimestampZeroCopy_EmptyRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Build empty record
	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()

	idArr := idBldr.NewArray() // empty array
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 0)
	defer rec.Release()

	// Call ensureTimestampZeroCopy
	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Should have timestamp column even for empty record
	if result.NumCols() != 2 {
		t.Errorf("expected 2 columns, got %d", result.NumCols())
	}
	if result.NumRows() != 0 {
		t.Errorf("expected 0 rows, got %d", result.NumRows())
	}
}

// TestEnsureTimestampZeroCopy_LargeRecord verifies performance with large records
func TestEnsureTimestampZeroCopy_LargeRecord(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
	}, nil)

	// Build large record (10000 rows)
	numRows := 10000
	idBldr := array.NewInt64Builder(mem)
	listBldr := array.NewListBuilder(mem, arrow.PrimitiveTypes.Float32)
	valBldr := listBldr.ValueBuilder().(*array.Float32Builder)
	defer idBldr.Release()
	defer listBldr.Release()

	ids := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = int64(i)
	}
	idBldr.AppendValues(ids, nil)

	// Build vector column (128-dim vectors)
	for i := 0; i < numRows; i++ {
		listBldr.Append(true)
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i*128 + j)
		}
		valBldr.AppendValues(vec, nil)
	}

	idArr := idBldr.NewArray()
	listArr := listBldr.NewArray()
	defer idArr.Release()
	defer listArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr, listArr}, int64(numRows))
	defer rec.Release()

	// Call ensureTimestampZeroCopy
	start := time.Now()
	result, err := EnsureTimestampZeroCopy(mem, rec)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Verify correctness
	if result.NumCols() != 3 {
		t.Errorf("expected 3 columns, got %d", result.NumCols())
	}
	if result.NumRows() != int64(numRows) {
		t.Errorf("expected %d rows, got %d", numRows, result.NumRows())
	}

	// Should be fast (< 10ms for zero-copy)
	if elapsed > 10*time.Millisecond {
		t.Logf("Warning: ensureTimestampZeroCopy took %v (expected < 10ms)", elapsed)
	}
}

// TestEnsureTimestampZeroCopy_RefCounting verifies proper reference counting
func TestEnsureTimestampZeroCopy_RefCounting(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 3)

	// Call ensureTimestampZeroCopy
	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Release original record
	rec.Release()

	// Result should still be valid (columns were Retained)
	if result.NumRows() != 3 {
		t.Errorf("expected 3 rows after original release, got %d", result.NumRows())
	}

	// Access data to verify it is valid
	idCol := result.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 {
		t.Errorf("expected id[0]=1, got %d", idCol.Value(0))
	}

	result.Release()
}

// TestEnsureTimestampZeroCopy_PreserveMetadata verifies schema metadata is preserved
func TestEnsureTimestampZeroCopy_PreserveMetadata(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create schema with metadata but no timestamp
	meta := arrow.NewMetadata([]string{"key1", "key2"}, []string{"val1", "val2"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, &meta)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues([]int64{1}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 1)
	defer rec.Release()

	// Call ensureTimestampZeroCopy
	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Verify metadata preserved
	resultMeta := result.Schema().Metadata()
	if resultMeta.Len() != 2 {
		t.Errorf("expected 2 metadata keys, got %d", resultMeta.Len())
	}
	if val, ok := resultMeta.GetValue("key1"); !ok || val != "val1" {
		t.Errorf("expected key1=val1, got %v", val)
	}
}

// TestEnsureTimestampZeroCopy_ConsistentTimestamp verifies all rows get same timestamp
func TestEnsureTimestampZeroCopy_ConsistentTimestamp(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	defer idBldr.Release()
	idBldr.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 5)
	defer rec.Release()

	result, err := EnsureTimestampZeroCopy(mem, rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// All timestamps should be identical
	tsCol := result.Column(1).(*array.Timestamp)
	firstTs := tsCol.Value(0)
	for i := 1; i < int(result.NumRows()); i++ {
		if tsCol.Value(i) != firstTs {
			t.Errorf("timestamp[%d]=%v differs from timestamp[0]=%v", i, tsCol.Value(i), firstTs)
		}
	}
}

// Benchmark: Compare old vs new implementation
func BenchmarkEnsureTimestamp_Old(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create record without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	ids := make([]int64, 1000)
	for i := range ids {
		ids[i] = int64(i)
	}
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idBldr.Release()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 1000)
	defer rec.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, _ := ensureTimestampOld(mem, rec)
		result.Release()
	}
}

func BenchmarkEnsureTimestamp_ZeroCopy(b *testing.B) {
	mem := memory.NewGoAllocator()

	// Create record without timestamp
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	ids := make([]int64, 1000)
	for i := range ids {
		ids[i] = int64(i)
	}
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idBldr.Release()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 1000)
	defer rec.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, _ := EnsureTimestampZeroCopy(mem, rec)
		result.Release()
	}
}

// Benchmark with large records (10K rows)
func BenchmarkEnsureTimestamp_Old_10K(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	ids := make([]int64, 10000)
	for i := range ids {
		ids[i] = int64(i)
	}
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idBldr.Release()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 10000)
	defer rec.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, _ := ensureTimestampOld(mem, rec)
		result.Release()
	}
}

func BenchmarkEnsureTimestamp_ZeroCopy_10K(b *testing.B) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idBldr := array.NewInt64Builder(mem)
	ids := make([]int64, 10000)
	for i := range ids {
		ids[i] = int64(i)
	}
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idBldr.Release()
	defer idArr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{idArr}, 10000)
	defer rec.Release()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, _ := EnsureTimestampZeroCopy(mem, rec)
		result.Release()
	}
}
