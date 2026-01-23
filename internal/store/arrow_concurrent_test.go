package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestConcurrentSearch validates thread-safe concurrent search operations.
func TestConcurrentSearch(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	// Create a simple index with a few nodes
	// Note: This test validates the locking mechanism works
	// Full concurrent search testing requires vector storage integration

	query := []float32{1.0, 2.0, 3.0}

	// Run concurrent searches
	var wg sync.WaitGroup
	numGoroutines := 10
	numSearches := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numSearches; j++ {
				_, err := index.Search(context.Background(), query, 10, 20, nil)
				if err != nil {
					t.Errorf("concurrent search failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentSearchAndInsert(t *testing.T) {
	// Setup dataset with 1000 vectors
	mem := memory.NewGoAllocator()
	dim := 32
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create vectors
	numVectors := 1000
	bldr := array.NewFixedSizeListBuilder(mem, int32(dim), arrow.PrimitiveTypes.Float32)
	defer bldr.Release()
	vb := bldr.ValueBuilder().(*array.Float32Builder)

	bldr.Reserve(numVectors)
	vb.Reserve(numVectors * dim)

	// Fill with dummy data
	for i := 0; i < numVectors; i++ {
		bldr.Append(true)
		for j := 0; j < dim; j++ {
			vb.Append(float32(i + j))
		}
	}

	rec := bldr.NewArray()
	defer rec.Release()
	batch := array.NewRecordBatch(schema, []arrow.Array{rec}, int64(numVectors))
	defer batch.Release()

	dataset := &Dataset{
		Name:    "test",
		Schema:  schema,
		Records: []arrow.RecordBatch{batch},
	}

	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	query := make([]float32, dim)

	var wg sync.WaitGroup

	// Searchers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = index.Search(context.Background(), query, 10, 20, nil)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Inserters
	for i := 0; i < 2; i++ {
		wg.Add(1)
		// We insert distinct ranges
		startIdx := i * 100
		go func(start int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				idx := start + j
				// AddByLocation automatically populates location store
				// BatchIdx 0, RowIdx = idx
				// Assuming idx < numVectors
				if _, err := index.AddByLocation(context.Background(), 0, idx); err != nil {
					t.Errorf("Insert failed: %v", err)
				}
				time.Sleep(time.Microsecond * 10)
			}
		}(startIdx)
	}

	wg.Wait()
}

// BenchmarkConcurrentSearch benchmarks concurrent search performance.
func BenchmarkConcurrentSearch(b *testing.B) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	query := []float32{1.0, 2.0, 3.0}

	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = index.Search(context.Background(), query, 10, 20, nil)
		}
	})

	b.Run("Parallel-4", func(b *testing.B) {
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = index.Search(context.Background(), query, 10, 20, nil)
			}
		})
	})

	b.Run("Parallel-8", func(b *testing.B) {
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = index.Search(context.Background(), query, 10, 20, nil)
			}
		})
	})
}

// BenchmarkSearchLatency benchmarks search latency distribution.
func BenchmarkSearchLatency(b *testing.B) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	query := make([]float32, 384)
	for i := range query {
		query[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = index.Search(context.Background(), query, 10, 20, nil)
	}
}

// BenchmarkInsertThroughput benchmarks insert throughput.
func BenchmarkInsertThroughput(b *testing.B) {
	dataset := &Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultArrowHNSWConfig(), nil)

	lg := NewLevelGenerator(1.44269504089)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		level := lg.Generate()
		_ = index.Insert(uint32(i), level)
	}
}
