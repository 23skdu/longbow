package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestRecordSizeCache tests the size caching mechanism
func TestRecordSizeCache(t *testing.T) {
	cache := NewRecordSizeCache()
	if cache == nil {
		t.Fatal("NewRecordSizeCache returned nil")
	}

	// Create a test record
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// First call should compute and cache
	size1 := cache.GetOrCompute(rec)
	if size1 <= 0 {
		t.Errorf("Expected positive size, got %d", size1)
	}

	// Second call should return cached value
	size2 := cache.GetOrCompute(rec)
	if size1 != size2 {
		t.Errorf("Cache inconsistent: %d vs %d", size1, size2)
	}

	// Verify cache hit count
	if cache.Hits() != 1 {
		t.Errorf("Expected 1 cache hit, got %d", cache.Hits())
	}
	if cache.Misses() != 1 {
		t.Errorf("Expected 1 cache miss, got %d", cache.Misses())
	}
}

// TestRecordSizeCacheInvalidate tests cache invalidation
func TestRecordSizeCacheInvalidate(t *testing.T) {
	cache := NewRecordSizeCache()

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Populate cache
	_ = cache.GetOrCompute(rec)

	// Invalidate
	cache.Invalidate(rec)

	// Should recompute
	missesBefore := cache.Misses()
	_ = cache.GetOrCompute(rec)
	if cache.Misses() != missesBefore+1 {
		t.Error("Expected cache miss after invalidation")
	}
}

// TestRecordSizeCacheClear tests clearing entire cache
func TestRecordSizeCacheClear(t *testing.T) {
	cache := NewRecordSizeCache()

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	_ = cache.GetOrCompute(rec)
	if cache.Len() != 1 {
		t.Errorf("Expected cache len 1, got %d", cache.Len())
	}

	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("Expected cache len 0 after clear, got %d", cache.Len())
	}
}

// TestEstimateRecordSize tests the fast heuristic estimator
func TestEstimateRecordSize(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// Fixed-width columns only
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},      // 8 bytes
		{Name: "value", Type: arrow.PrimitiveTypes.Float32}, // 4 bytes
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	numRows := int64(1000)
	ids := make([]int64, numRows)
	vals := make([]float32, numRows)
	for i := range ids {
		ids[i] = int64(i)
		vals[i] = float32(i)
	}

	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.Float32Builder).AppendValues(vals, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	estimated := EstimateRecordSize(rec)
	actual := calculateRecordSize(rec)

	// Estimate should be within 20% of actual for fixed-width
	ratio := float64(estimated) / float64(actual)
	if ratio < 0.8 || ratio > 1.2 {
		t.Errorf("Estimate %d too far from actual %d (ratio: %.2f)", estimated, actual, ratio)
	}
}

// TestEstimateRecordSizeNil tests nil handling
func TestEstimateRecordSizeNil(t *testing.T) {
	if EstimateRecordSize(nil) != 0 {
		t.Error("Expected 0 for nil record")
	}
}

// TestCachedRecordSize tests the main API function
func TestCachedRecordSize(t *testing.T) {
	// Reset global cache for test isolation
	ResetGlobalSizeCache()

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// First call computes
	size1 := CachedRecordSize(rec)
	if size1 <= 0 {
		t.Errorf("Expected positive size, got %d", size1)
	}

	// Second call uses cache
	size2 := CachedRecordSize(rec)
	if size1 != size2 {
		t.Errorf("Cached size mismatch: %d vs %d", size1, size2)
	}
}

// BenchmarkCalculateRecordSize benchmarks the original O(N) approach
func BenchmarkCalculateRecordSize(b *testing.B) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "flag", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	numRows := 10000
	ids := make([]int64, numRows)
	vals := make([]float64, numRows)
	flags := make([]int32, numRows)
	for i := range ids {
		ids[i] = int64(i)
		vals[i] = float64(i)
		flags[i] = int32(i % 100)
	}

	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues(flags, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = calculateRecordSize(rec)
	}
}

// BenchmarkCachedRecordSize benchmarks the cached approach
func BenchmarkCachedRecordSize(b *testing.B) {
	ResetGlobalSizeCache()

	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "flag", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	numRows := 10000
	ids := make([]int64, numRows)
	vals := make([]float64, numRows)
	flags := make([]int32, numRows)
	for i := range ids {
		ids[i] = int64(i)
		vals[i] = float64(i)
		flags[i] = int32(i % 100)
	}

	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues(flags, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Warm up cache
	_ = CachedRecordSize(rec)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CachedRecordSize(rec)
	}
}

// BenchmarkEstimateRecordSize benchmarks the heuristic estimator
func BenchmarkEstimateRecordSize(b *testing.B) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "flag", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	numRows := 10000
	ids := make([]int64, numRows)
	vals := make([]float64, numRows)
	flags := make([]int32, numRows)
	for i := range ids {
		ids[i] = int64(i)
		vals[i] = float64(i)
		flags[i] = int32(i % 100)
	}

	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	builder.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
	builder.Field(2).(*array.Int32Builder).AppendValues(flags, nil)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EstimateRecordSize(rec)
	}
}
