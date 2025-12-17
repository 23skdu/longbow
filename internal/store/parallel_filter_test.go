package store

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// createTestStore creates a VectorStore for testing
func createTestStore(t *testing.T) *VectorStore {
	t.Helper()
	mem := memory.NewGoAllocator()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewVectorStore(mem, logger, 1024*1024*100, 0, 0) // 100MB limit
}

// Helper to create test records with identifiable data
func makeTestRecordWithID(mem memory.Allocator, id int64, category string, value float64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).Append(id)
	bldr.Field(1).(*array.StringBuilder).Append(category)
	bldr.Field(2).(*array.Float64Builder).Append(value)

	return bldr.NewRecordBatch()
}

func TestFilterRecordsParallel_Basic(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	// Create 10 records with alternating categories
	recs := make([]arrow.RecordBatch, 10)
	for i := 0; i < 10; i++ {
		category := "A"
		if i%2 == 1 {
			category = "B"
		}
		recs[i] = makeTestRecordWithID(mem, int64(i), category, float64(i)*1.5)
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// Filter for category = "A"
	filters := []Filter{{Field: "category", Operator: "=", Value: "A"}}

	ctx := context.Background()
	resultCh := store.filterRecordsParallel(ctx, recs, filters)

	results := make([]arrow.RecordBatch, 0, len(recs))
	for rec := range resultCh {
		results = append(results, rec)
	}

	// Should get 5 records (ids 0, 2, 4, 6, 8)
	if len(results) != 5 {
		t.Errorf("expected 5 filtered records, got %d", len(results))
	}

	for _, r := range results {
		r.Release()
	}
}

func TestFilterRecordsParallel_OrderPreserved(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	// Create 20 records
	recs := make([]arrow.RecordBatch, 20)
	for i := 0; i < 20; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "all", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// No filter - all records should pass in order
	ctx := context.Background()
	resultCh := store.filterRecordsParallel(ctx, recs, nil)

	results := make([]arrow.RecordBatch, 0, len(recs))
	for rec := range resultCh {
		results = append(results, rec)
	}

	if len(results) != 20 {
		t.Fatalf("expected 20 records, got %d", len(results))
	}

	// Verify order is preserved
	for i, rec := range results {
		idCol := rec.Column(0).(*array.Int64)
		if idCol.Value(0) != int64(i) {
			t.Errorf("order not preserved: expected id %d at position %d, got %d", i, i, idCol.Value(0))
		}
		rec.Release()
	}
}

func TestFilterRecordsParallel_EmptyInput(t *testing.T) {
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	resultCh := store.filterRecordsParallel(ctx, nil, nil)

	count := 0
	for range resultCh {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 records for empty input, got %d", count)
	}
}

func TestFilterRecordsParallel_AllFiltered(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 5)
	for i := 0; i < 5; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "X", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// Filter for non-existent category
	filters := []Filter{{Field: "category", Operator: "=", Value: "NONE"}}

	ctx := context.Background()
	resultCh := store.filterRecordsParallel(ctx, recs, filters)

	count := 0
	for range resultCh {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 records when all filtered, got %d", count)
	}
}

func TestFilterRecordsParallel_ContextCancellation(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	// Large dataset
	recs := make([]arrow.RecordBatch, 100)
	for i := 0; i < 100; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "test", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := store.filterRecordsParallel(ctx, recs, nil)

	// Read a few then cancel
	count := 0
	for rec := range resultCh {
		rec.Release()
		count++
		if count >= 5 {
			cancel()
			break
		}
	}

	// Drain remaining to prevent goroutine leak
	for rec := range resultCh {
		rec.Release()
	}
}

func TestFilterRecordsParallel_ConcurrentSafety(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 50)
	for i := 0; i < 50; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "test", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// Run multiple parallel filter operations concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			resultCh := store.filterRecordsParallel(ctx, recs, nil)
			for rec := range resultCh {
				rec.Release()
			}
		}()
	}
	wg.Wait()
}

func TestFilterRecordsParallel_MultipleFilters(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := createTestStore(t)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 20)
	for i := 0; i < 20; i++ {
		category := "A"
		if i >= 10 {
			category = "B"
		}
		recs[i] = makeTestRecordWithID(mem, int64(i), category, float64(i)*0.5)
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	// Filter: category = "A" AND value < 3.0 (ids 0-5 have values 0, 0.5, 1, 1.5, 2, 2.5)
	filters := []Filter{
		{Field: "category", Operator: "=", Value: "A"},
		{Field: "value", Operator: "<", Value: "3.0"},
	}

	ctx := context.Background()
	resultCh := store.filterRecordsParallel(ctx, recs, filters)

	results := make([]arrow.RecordBatch, 0, len(recs))
	for rec := range resultCh {
		results = append(results, rec)
	}

	// Should get 6 records (ids 0-5)
	if len(results) != 6 {
		t.Errorf("expected 6 records with multiple filters, got %d", len(results))
	}

	for _, r := range results {
		r.Release()
	}
}

// Benchmark helpers
func createBenchStore(b *testing.B) *VectorStore {
	b.Helper()
	mem := memory.NewGoAllocator()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewVectorStore(mem, logger, 1024*1024*100, 0, 0)
}

// Benchmark: Sequential vs Parallel
func BenchmarkFilterRecords_Sequential(b *testing.B) {
	mem := memory.NewGoAllocator()
	store := createBenchStore(b)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 100)
	for i := 0; i < 100; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "test", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	filters := []Filter{{Field: "value", Operator: ">", Value: "50"}}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, rec := range recs {
			filtered, err := store.filterRecord(ctx, rec, filters)
			if err == nil && filtered.NumRows() > 0 {
				filtered.Release()
			}
		}
	}
}

func BenchmarkFilterRecords_Parallel(b *testing.B) {
	mem := memory.NewGoAllocator()
	store := createBenchStore(b)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 100)
	for i := 0; i < 100; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "test", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	filters := []Filter{{Field: "value", Operator: ">", Value: "50"}}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultCh := store.filterRecordsParallel(ctx, recs, filters)
		for rec := range resultCh {
			rec.Release()
		}
	}
}

func BenchmarkFilterRecords_Parallel_LargeDataset(b *testing.B) {
	mem := memory.NewGoAllocator()
	store := createBenchStore(b)
	defer func() { _ = store.Close() }()

	recs := make([]arrow.RecordBatch, 1000)
	for i := 0; i < 1000; i++ {
		recs[i] = makeTestRecordWithID(mem, int64(i), "test", float64(i))
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	filters := []Filter{{Field: "value", Operator: ">", Value: "500"}}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultCh := store.filterRecordsParallel(ctx, recs, filters)
		for rec := range resultCh {
			rec.Release()
		}
	}
}
