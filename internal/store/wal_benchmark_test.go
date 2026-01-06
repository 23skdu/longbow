package store

import (
	"os"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkWAL(b *testing.B) {
	// Prepare common data
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
		{Name: "bytes", Type: arrow.BinaryTypes.Binary},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Create a record with reasonable size (e.g. 1KB)
	payload := make([]byte, 1000)
	builder.Field(0).(*array.Int32Builder).Append(1)
	builder.Field(1).(*array.BinaryBuilder).Append(payload)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Benchmark StdWAL (Sync)
	b.Run("StdWAL", func(b *testing.B) {
		tmpDir, err := os.MkdirTemp("", "wal_bench_std")
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = os.RemoveAll(tmpDir) }()

		wal := storage.NewStdWAL(tmpDir)
		defer func() { _ = wal.Close() }()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := wal.Write("bench", uint64(i), 0, rec); err != nil {
				b.Fatal(err)
			}
		}
	})

	// Benchmark BufferedWAL (Async)
	b.Run("BufferedWAL", func(b *testing.B) {
		tmpDir, err := os.MkdirTemp("", "wal_bench_buf")
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = os.RemoveAll(tmpDir) }()

		// Use new storage types
		// Requires direct access to FSBackend or similar if exposed
		// But in storage, we usually use NewWALBatcher which uses BufferedWAL underneath or manages batching differently

		// If BufferedWAL is private in storage, we can't benchmark it here directly unless exported
		// wal_buffered.go has BufferedWAL?

		// Assuming exposed as storage.NewBufferedWAL for now
		// If not, we skip this benchmark or use batcher.

		// fsBackend := storage.NewFSBackend(...) ??

		// If BufferedWAL is not exposed, we probably should benchmark WALBatcher
		// WALBatcher wraps everything.

		cfg := &storage.WALBatcherConfig{
			FlushInterval: 50 * time.Millisecond,
			MaxBatchSize:  1024 * 1024,
		}
		batcher := storage.NewWALBatcher(tmpDir, cfg)
		if err := batcher.Start(); err != nil {
			b.Fatal(err)
		}
		defer func() { _ = batcher.Stop() }()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := batcher.Write(rec, "bench", uint64(i), 0); err != nil {
				b.Fatal(err)
			}
		}
	})
}
