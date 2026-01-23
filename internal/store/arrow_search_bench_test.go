package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkSearchWithBatchCompute benchmarks search performance with and without batch compute
func BenchmarkSearchWithBatchCompute(b *testing.B) {
	mem := memory.NewGoAllocator()
	dim := 384
	numVectors := 1000

	// Create dataset with schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	ds := &Dataset{
		Name:   "bench_test",
		Schema: schema,
	}

	// Create HNSW index
	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 100
	h := NewArrowHNSW(ds, config, nil)

	// Build record batches with vectors
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(uint32(i))
		vecBuilder.Append(true)
		for j := 0; j < dim; j++ {
			floatBuilder.Append(rand.Float32())
		}
	}

	rec := builder.NewRecordBatch()
	ds.Records = []arrow.RecordBatch{rec}

	// Create level generator
	ml := 1.0 / math.Log(float64(config.M))
	levelGen := NewLevelGenerator(ml)

	// Insert vectors into HNSW
	for i := 0; i < numVectors; i++ {
		level := levelGen.Generate()
		_ = h.Insert(uint32(i), level)
	}

	// Create query vector
	query := make([]float32, dim)
	for i := range query {
		query[i] = rand.Float32()
	}

	b.Run("WithBatchCompute", func(b *testing.B) {
		// Ensure batch computer is enabled
		if h.batchComputer == nil {
			h.batchComputer = NewBatchDistanceComputer(mem, dim)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = h.Search(context.Background(), query, 10, 50, nil)
		}
	})

	b.Run("WithoutBatchCompute", func(b *testing.B) {
		// Disable batch computer
		originalBC := h.batchComputer
		h.batchComputer = nil

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = h.Search(context.Background(), query, 10, 50, nil)
		}

		// Restore
		h.batchComputer = originalBC
	})
}

// BenchmarkSearchScaling benchmarks search performance at different scales
func BenchmarkSearchScaling(b *testing.B) {
	mem := memory.NewGoAllocator()
	dim := 384

	for _, numVectors := range []int{100, 500, 1000, 5000} {
		b.Run(fmt.Sprintf("Vectors_%d", numVectors), func(b *testing.B) {
			// Create dataset
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
				{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
			}, nil)

			ds := &Dataset{
				Name:   "scale_test",
				Schema: schema,
			}

			// Create HNSW index
			config := DefaultArrowHNSWConfig()
			h := NewArrowHNSW(ds, config, nil)

			// Build vectors
			builder := array.NewRecordBuilder(mem, schema)
			defer builder.Release()

			idBuilder := builder.Field(0).(*array.Uint32Builder)
			vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
			floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

			for i := 0; i < numVectors; i++ {
				idBuilder.Append(uint32(i))
				vecBuilder.Append(true)
				for j := 0; j < dim; j++ {
					floatBuilder.Append(rand.Float32())
				}
			}

			rec := builder.NewRecordBatch()
			ds.Records = []arrow.RecordBatch{rec}

			// Create level generator
			ml := 1.0 / math.Log(float64(config.M))
			levelGen := NewLevelGenerator(ml)

			// Insert vectors
			for i := 0; i < numVectors; i++ {
				level := levelGen.Generate()
				_ = h.Insert(uint32(i), level)
			}

			// Query
			query := make([]float32, dim)
			for i := range query {
				query[i] = rand.Float32()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = h.Search(context.Background(), query, 10, 50, nil)
			}
		})
	}
}
