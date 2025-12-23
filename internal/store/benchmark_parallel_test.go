package store

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// BenchmarkParallelSearch benchmarks parallel vs serial search
func BenchmarkParallelSearch(b *testing.B) {
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()
	vs := NewVectorStore(mem, logger, 1024*1024*512, 0, 0)

	// Setup dataset with 10k vectors
	dim := 128
	count := 10000
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = float32(i + j) // non-random to be fast
		}
	}

	// Manually create dataset and index (internal/store package allow access)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	ds := &Dataset{
		Name:    "bench_parallel",
		Schema:  schema,
		Records: make([]arrow.RecordBatch, 0),
		dataMu:  sync.RWMutex{},
	}
	ds.Index = NewHNSWIndex(ds)
	vs.datasets["bench_parallel"] = ds

	// Insert data
	batchSize := 1000
	for i := 0; i < count; i += batchSize {
		// Create record batch
		bld := array.NewRecordBuilder(mem, ds.Schema)

		vecBld := bld.Field(0).(*array.FixedSizeListBuilder)
		valBld := vecBld.ValueBuilder().(*array.Float32Builder)

		for j := 0; j < batchSize && i+j < count; j++ {
			vecBld.Append(true)
			valBld.AppendValues(vectors[i+j], nil)
		}

		rec := bld.NewRecordBatch()
		ds.dataMu.Lock()
		ds.Records = append(ds.Records, rec)
		ds.dataMu.Unlock()

		// Add to index
		for j := 0; j < int(rec.NumRows()); j++ {
			ds.Index.AddByLocation(len(ds.Records)-1, j)
		}
		bld.Release()
	}

	query := vectors[0] // Use first vector as query

	hnswIdx, ok := ds.Index.(*HNSWIndex)
	if !ok {
		b.Fatalf("Index is not HNSWIndex")
	}

	b.ResetTimer()

	b.Run("Serial", func(b *testing.B) {
		hnswIdx.SetParallelSearchConfig(ParallelSearchConfig{
			Enabled: false,
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Use k=1000 to trigger meaningful work
			ds.Index.SearchVectors(query, 1000, nil)
		}
	})

	b.Run("Parallel-2Workers", func(b *testing.B) {
		hnswIdx.SetParallelSearchConfig(ParallelSearchConfig{
			Enabled:   true,
			Workers:   2,
			Threshold: 1, // Force parallel
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds.Index.SearchVectors(query, 1000, nil)
		}
	})

	b.Run("Parallel-4Workers", func(b *testing.B) {
		hnswIdx.SetParallelSearchConfig(ParallelSearchConfig{
			Enabled:   true,
			Workers:   4,
			Threshold: 1, // Force parallel
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds.Index.SearchVectors(query, 1000, nil)
		}
	})

	b.Run("Parallel-8Workers", func(b *testing.B) {
		hnswIdx.SetParallelSearchConfig(ParallelSearchConfig{
			Enabled:   true,
			Workers:   8,
			Threshold: 1, // Force parallel
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ds.Index.SearchVectors(query, 1000, nil)
		}
	})
}
