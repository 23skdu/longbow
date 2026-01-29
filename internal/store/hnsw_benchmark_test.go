package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkHNSWComparison(b *testing.B) {
	// Configuration
	numVectors := 1000
	dim := 128
	k := 10

	// Create dataset
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	vectors := generateRandomVectors(numVectors, dim)

	// Prepare Arrow Records
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	valueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(uint32(i))
		vecBuilder.Append(true)
		for _, v := range vectors[i] {
			valueBuilder.Append(v)
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds := store.NewDataset("bench", schema)
	ds.Records = []arrow.RecordBatch{rec}

	// Helper to create and populate coder/hnsw
	createCoderHNSW := func() *store.HNSWIndex {
		idx := store.NewHNSWIndex(ds)
		start := time.Now()
		for i := 0; i < numVectors; i++ {
			// Updated to AddByLocation
			_, _ = idx.AddByLocation(context.Background(), 0, i)
		}
		b.ReportMetric(float64(time.Since(start).Milliseconds()), "coder_build_ms")
		return idx
	}

	// Helper to create and populate hnsw2
	createHNSW2 := func() *store.ArrowHNSW {
		config := store.DefaultArrowHNSWConfig()
		// Updated to remove 3rd arg
		idx := store.NewArrowHNSW(ds, &config)

		start := time.Now()
		for i := 0; i < numVectors; i++ {
			_, _ = idx.AddByLocation(context.Background(), 0, i)
		}
		b.ReportMetric(float64(time.Since(start).Milliseconds()), "hnsw2_build_ms")
		return idx
	}

	// Run Benchmarks
	b.Run("Build/Coder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			createCoderHNSW()
		}
	})

	b.Run("Build/HNSW2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			createHNSW2()
		}
	})

	// Search Benchmarks
	coderIdx := createCoderHNSW()
	hnsw2Idx := createHNSW2()

	query := vectors[0] // Use first vector as query

	b.Run("Search/Coder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// SearchVectors signature updated
			_, err := coderIdx.SearchVectors(context.Background(), query, k, nil, store.SearchOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Search/HNSW2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Search signature updated (removed efSearch)
			_, err := hnsw2Idx.Search(context.Background(), query, k, nil)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Parallel Search Benchmarks
	b.Run("SearchParallel/Coder", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := coderIdx.SearchVectors(context.Background(), query, k, nil, store.SearchOptions{})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("SearchParallel/HNSW2", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := hnsw2Idx.Search(context.Background(), query, k, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
