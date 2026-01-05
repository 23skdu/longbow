package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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

		wal := NewStdWAL(tmpDir, nil)
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

		fsBackend, _ := NewFSBackend(filepath.Join(tmpDir, "wal.log"))
		// 1MB buffer, 50ms flush
		wal := NewBufferedWAL(fsBackend, 1024*1024, 50*time.Millisecond)
		defer func() { _ = wal.Close() }()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := wal.Write("bench", uint64(i), 0, rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}
