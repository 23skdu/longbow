package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSearch_EmptyIndex(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	query := []float32{1.0, 2.0, 3.0}
	results, err := index.Search(context.Background(), query, 10, 20, nil)

	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results from empty index, got %d", len(results))
	}
}

func TestSearch_InvalidK(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	query := []float32{1.0, 2.0, 3.0}

	// k = 0 should return empty results
	results, err := index.Search(context.Background(), query, 0, 20, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for k=0, got %d", len(results))
	}

	// k < 0 should return empty results
	results, err = index.Search(context.Background(), query, -1, 20, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for k<0, got %d", len(results))
	}
}

func TestArrowSearchContext_Pooling(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// Get context
	ctx1 := pool.Get().(*ArrowSearchContext)
	if ctx1 == nil {
		t.Fatal("pool.Get() returned nil")
	}

	// Use context
	ctx1.candidates.Push(Candidate{ID: 1, Dist: 1.0})
	ctx1.visited.Set(5)

	// Return to pool
	pool.Put(ctx1)

	// Get again - should be reused
	ctx2 := pool.Get().(*ArrowSearchContext)
	if ctx2 == nil {
		t.Fatal("pool.Get() returned nil on second call")
	}

	// Context is not auto-cleared by pool. Verify Reset works.
	ctx2.Reset()
	if ctx2.candidates.Len() != 0 {
		t.Error("candidates not cleared after Reset")
	}
	if ctx2.visited.IsSet(5) {
		t.Error("visited not cleared after Reset")
	}
}

func BenchmarkSearch_SmallIndex(b *testing.B) {
	// Setup 1000 vectors
	n := 1000
	dim := 128

	dataset, err := makeBenchmarkDataset(n, dim)
	if err != nil {
		b.Fatalf("failed to create dataset: %v", err)
	}

	config := DefaultArrowHNSWConfig()
	config.EfConstruction = 100
	index := NewArrowHNSW(dataset, config, nil)

	// Bulk Insert
	// Assuming location 0..n map to vectors
	for i := 0; i < n; i++ {
		_, err := index.AddByLocation(0, i)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}

	query := make([]float32, dim)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := index.Search(context.Background(), query, 10, 50, nil)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
	}
}

func makeBenchmarkDataset(n, dim int) (*Dataset, error) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	listB.Reserve(n)
	valB.Reserve(n * dim)

	for i := 0; i < n; i++ {
		listB.Append(true)
		for j := 0; j < dim; j++ {
			valB.UnsafeAppend(float32(i+j) * 0.01)
		}
	}

	rec := b.NewRecordBatch()
	// Note: In real usage, we should manage lifecycle. Here leaks are acceptable for short benchmark.
	// But let's be clean-ish. Caller won't release it easily.

	return &Dataset{
		Schema:  schema,
		Records: []arrow.RecordBatch{rec},
	}, nil
}

func BenchmarkSearch_LargeIndex(b *testing.B) {
	// Setup 10,000 vectors
	n := 10000
	dim := 128

	dataset, err := makeBenchmarkDataset(n, dim)
	if err != nil {
		b.Fatalf("failed to create dataset: %v", err)
	}

	config := DefaultArrowHNSWConfig()
	config.EfConstruction = 100
	index := NewArrowHNSW(dataset, config, nil)

	// Bulk Insert
	for i := 0; i < n; i++ {
		_, err := index.AddByLocation(0, i)
		if err != nil {
			b.Fatalf("insert failed: %v", err)
		}
	}

	query := make([]float32, dim)
	for i := range query {
		query[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := index.Search(context.Background(), query, 10, 50, nil)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
	}
}
