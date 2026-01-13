package store

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func BenchmarkIndexWorker_Throughput(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create a dummy record with 1 row
	rb := array.NewRecordBuilder(mem, schema)
	rb.Field(0).(*array.Int64Builder).Append(1)
	vb := rb.Field(1).(*array.FixedSizeListBuilder)
	vb.ValueBuilder().(*array.Float32Builder).AppendValues(make([]float32, 128), nil)
	vb.Append(true)
	rec1 := rb.NewRecordBatch()
	defer rec1.Release()

	b.Run("Batch1", func(b *testing.B) {
		runBenchmark(b, mem, schema, rec1, 1)
	})
}

func BenchmarkIndexWorker_BulkInsert(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	// Create a large batch for bulk insert (1000 rows)
	rows := 1000
	rb := array.NewRecordBuilder(mem, schema)

	vectorData := make([]float32, 128*rows)
	for i := 0; i < len(vectorData); i++ {
		vectorData[i] = float32(i)
	}

	for i := 0; i < rows; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
	}

	vb := rb.Field(1).(*array.FixedSizeListBuilder)
	vb.ValueBuilder().(*array.Float32Builder).AppendValues(vectorData, nil)
	for i := 0; i < rows; i++ {
		vb.Append(true)
	}

	rec := rb.NewRecordBatch()
	defer rec.Release()

	b.Run("Batch1000", func(b *testing.B) {
		runBenchmark(b, mem, schema, rec, rows)
	})
}

func runBenchmark(b *testing.B, mem memory.Allocator, schema *arrow.Schema, rec arrow.RecordBatch, rowsPerJob int) {
	s := NewVectorStore(
		mem,
		zerolog.New(io.Discard),
		1*1024*1024*1024,
		0,
		0,
	)
	// Use default queue from NewVectorStore (10k size)
	// This avoids race condition with default worker started by NewVectorStore

	// Create dataset
	dsName := "bench_ds"
	s.getOrCreateDataset(dsName, func() *Dataset {
		ds := NewDataset(dsName, schema)
		// Mock index? Or real?
		// Real AutoShardingIndex initialized with defaults
		ds.Index = NewAutoShardingIndex(ds, DefaultAutoShardingConfig())
		return ds
	})
	ds, _ := s.getDataset(dsName)

	// Start Worker manually
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runIndexWorker(mem)
	}()

	b.ResetTimer()

	// Push b.N jobs
	for i := 0; i < b.N; i++ {
		rec.Retain()
		job := IndexJob{
			DatasetName: dsName,
			Record:      rec,
			BatchIdx:    i,
			CreatedAt:   time.Now(),
		}
		ds.PendingIndexJobs.Add(int64(rowsPerJob))
		s.indexQueue.Send(job)
	}

	// Wait for drain
	// We check pending jobs
	for ds.PendingIndexJobs.Load() > 0 {
		time.Sleep(1 * time.Millisecond)
	}

	b.StopTimer()
	s.indexQueue.Stop()
	close(s.stopChan)
	wg.Wait()
}
