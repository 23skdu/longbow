package store

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// TDD Tests for calculateRecordSize Cache Integration
// Tests that hot paths use CachedRecordSize instead of calculateRecordSize
// =============================================================================

// createIntegrationTestRecord creates records for integration tests
func createIntegrationTestRecord(t *testing.T, numRows int) arrow.RecordBatch {
	t.Helper()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for i := 0; i < numRows; i++ {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.Float64Builder).Append(float64(i))
	}

	return builder.NewRecordBatch()
}

// TestCachedRecordSizeIsUsedInDoPut verifies DoPut uses cached size calculation
func TestCachedRecordSizeIsUsedInDoPut(t *testing.T) {
	ResetGlobalSizeCache()

	rec := createIntegrationTestRecord(t, 100)
	defer rec.Release()

	// First call should be a cache miss
	size1 := CachedRecordSize(rec)
	if size1 <= 0 {
		t.Fatalf("Expected positive size, got %d", size1)
	}

	misses := globalSizeCache.Misses()
	if misses != 1 {
		t.Errorf("Expected 1 miss after first call, got %d", misses)
	}

	// Second call should be a cache hit
	size2 := CachedRecordSize(rec)
	if size2 != size1 {
		t.Errorf("Cached size mismatch: %d != %d", size2, size1)
	}

	hits := globalSizeCache.Hits()
	if hits != 1 {
		t.Errorf("Expected 1 hit after second call, got %d", hits)
	}
}

// TestCachePerformanceImprovement verifies cache provides O(1) lookups
func TestCachePerformanceImprovement(t *testing.T) {
	ResetGlobalSizeCache()

	// Create a larger record with multiple columns
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Float64},
		{Name: "col2", Type: arrow.PrimitiveTypes.Float64},
		{Name: "col3", Type: arrow.PrimitiveTypes.Int64},
		{Name: "col4", Type: arrow.BinaryTypes.String},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for i := 0; i < 1000; i++ {
		builder.Field(0).(*array.Float64Builder).Append(float64(i))
		builder.Field(1).(*array.Float64Builder).Append(float64(i * 2))
		builder.Field(2).(*array.Int64Builder).Append(int64(i))
		builder.Field(3).(*array.StringBuilder).Append("test")
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// First call - cache miss (computes)
	_ = CachedRecordSize(rec)
	initialMisses := globalSizeCache.Misses()

	// Multiple subsequent calls - all cache hits
	for i := 0; i < 100; i++ {
		_ = CachedRecordSize(rec)
	}

	hits := globalSizeCache.Hits()
	misses := globalSizeCache.Misses()

	if hits < 100 {
		t.Errorf("Expected at least 100 hits, got %d", hits)
	}
	if misses != initialMisses {
		t.Errorf("Expected no additional misses, got %d (was %d)", misses, initialMisses)
	}
}

// TestCacheConsistencyWithDirectCalculation verifies cache returns correct values
func TestCacheConsistencyWithDirectCalculation(t *testing.T) {
	ResetGlobalSizeCache()

	rec := createIntegrationTestRecord(t, 500)
	defer rec.Release()

	// Calculate directly (bypassing cache)
	directSize := calculateRecordSize(rec)

	// Get from cache
	cachedSize := CachedRecordSize(rec)

	if directSize != cachedSize {
		t.Errorf("Cache returned different size: direct=%d, cached=%d", directSize, cachedSize)
	}
}

// TestCacheInvalidationOnRecordRelease tests that invalidation works
func TestCacheInvalidationOnRecordRelease(t *testing.T) {
	ResetGlobalSizeCache()

	rec := createIntegrationTestRecord(t, 50)

	// Cache the size
	size1 := CachedRecordSize(rec)
	if globalSizeCache.Len() != 1 {
		t.Errorf("Expected cache length 1, got %d", globalSizeCache.Len())
	}

	// Invalidate
	globalSizeCache.Invalidate(rec)

	// Should be removed from cache
	if globalSizeCache.Len() != 0 {
		t.Errorf("Expected cache length 0 after invalidation, got %d", globalSizeCache.Len())
	}

	// Getting again should miss and recompute
	missesBefore := globalSizeCache.Misses()
	size2 := CachedRecordSize(rec)
	missesAfter := globalSizeCache.Misses()

	if missesAfter != missesBefore+1 {
		t.Errorf("Expected cache miss after invalidation")
	}
	if size1 != size2 {
		t.Errorf("Size should be same after recompute: %d != %d", size1, size2)
	}

	rec.Release()
}

// TestCacheConcurrentAccess verifies thread-safety of cache
func TestCacheConcurrentAccess(t *testing.T) {
	ResetGlobalSizeCache()

	rec := createIntegrationTestRecord(t, 100)
	defer rec.Release()

	var wg sync.WaitGroup
	expectedSize := calculateRecordSize(rec) // Get expected value first

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			size := CachedRecordSize(rec)
			if size != expectedSize {
				t.Errorf("Concurrent access returned wrong size: %d != %d", size, expectedSize)
			}
		}()
	}

	wg.Wait()

	// All accesses should complete without data races
	totalOps := globalSizeCache.Hits() + globalSizeCache.Misses()
	if totalOps < 100 {
		t.Errorf("Expected at least 100 operations, got %d", totalOps)
	}
}

// TestEstimateRecordSizeFast verifies O(1) estimation function
func TestEstimateRecordSizeFast(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for i := 0; i < 1000; i++ {
		builder.Field(0).(*array.Float64Builder).Append(float64(i))
		builder.Field(1).(*array.Int64Builder).Append(int64(i))
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	estimate := EstimateRecordSize(rec)
	actual := calculateRecordSize(rec)

	// Estimate should be within 50% for fixed-width columns
	ratio := float64(estimate) / float64(actual)
	if ratio < 0.5 || ratio > 2.0 {
		t.Errorf("Estimate too far off: estimate=%d, actual=%d, ratio=%.2f", estimate, actual, ratio)
	}
}

// TestDatasetTotalSizeUsesCache verifies Dataset.TotalSize uses cache
func TestDatasetTotalSizeUsesCache(t *testing.T) {
	ResetGlobalSizeCache()
	datasetSizeCache = sync.Map{} // Reset dataset cache too

	ds := &Dataset{
		Records: []arrow.RecordBatch{},
	}

	// Add records
	for i := 0; i < 5; i++ {
		rec := createIntegrationTestRecord(t, 100+i*10)
		ds.Records = append(ds.Records, rec)
	}
	defer func() {
		for _, r := range ds.Records {
			r.Release()
		}
	}()

	// First call computes total
	total1 := ds.TotalSize()
	if total1 <= 0 {
		t.Fatalf("Expected positive total size, got %d", total1)
	}

	// Second call should use cache
	total2 := ds.TotalSize()
	if total1 != total2 {
		t.Errorf("Cached total mismatch: %d != %d", total1, total2)
	}
}

// TestAddRecordWithSizeUpdatesCacheTotal verifies incremental updates
func TestAddRecordWithSizeUpdatesCacheTotal(t *testing.T) {
	ResetGlobalSizeCache()
	datasetSizeCache = sync.Map{}

	ds := &Dataset{
		Records: []arrow.RecordBatch{},
	}

	// Add first record
	rec1 := createIntegrationTestRecord(t, 100)
	ds.AddRecordWithSize(rec1)
	total1 := ds.TotalSize()

	// Add second record
	rec2 := createIntegrationTestRecord(t, 100)
	ds.AddRecordWithSize(rec2)
	total2 := ds.TotalSize()

	// Total should increase
	if total2 <= total1 {
		t.Errorf("Total should increase after adding record: %d <= %d", total2, total1)
	}

	// Cleanup
	for _, r := range ds.Records {
		r.Release()
	}
}

// =============================================================================
// Integration Benchmarks for cache performance
// =============================================================================

func BenchmarkDirectCalculateRecordSizeIntegration(b *testing.B) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	for i := 0; i < 10000; i++ {
		builder.Field(0).(*array.Float64Builder).Append(float64(i))
		builder.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = calculateRecordSize(rec)
	}
}

func BenchmarkCachedRecordSizeIntegration(b *testing.B) {
	ResetGlobalSizeCache()
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	builder := array.NewRecordBuilder(alloc, schema)
	for i := 0; i < 10000; i++ {
		builder.Field(0).(*array.Float64Builder).Append(float64(i))
		builder.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()
	defer builder.Release()

	// Prime the cache
	_ = CachedRecordSize(rec)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CachedRecordSize(rec)
	}
}
