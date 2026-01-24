//go:build linux
// +build linux

package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
)

func createTestBatch() arrow.Record {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "data", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	builder.Field(0).(*array.BinaryBuilder).Append([]byte("test-data"))

	return builder.NewRecord()
}

func writeWALEntries(backend storage.WALBackend, count int) error {
	for i := 0; i < count; i++ {
		batch := createTestBatch()
		defer batch.Release()

		// Create WALEntry directly
		record := batch
		name := fmt.Sprintf("test-dataset-%d", i)
		seq := uint64(i)
		timestamp := time.Now().UnixNano()

		// Serialize the entry manually for benchmarking
		size := 4 + 4 + len(name) + 8 + record.NumRows()*int(record.Schema().Field(0).Type.(arrow.BinaryType).Len)
		buf := make([]byte, size)
		offset := 0

		// Name length and name
		buf[offset] = byte(len(name))
		offset++
		copy(buf[offset:], name)
		offset += len(name)

		// Sequence number
		binary.LittleEndian.PutUint64(buf[offset:], seq)
		offset += 8

		// Timestamp
		binary.LittleEndian.PutUint64(buf[offset:], uint64(timestamp))
		offset += 8

		// Record data (simplified - just write length + data)
		data := []byte("test-data")
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(data)))
		offset += 4
		copy(buf[offset:], data)
		offset += len(data)

		// Write to backend
		if _, err := backend.Write(buf[:offset]); err != nil {
			return fmt.Errorf("failed to write WAL entry: %w", err)
		}
	}
	return nil
}

func BenchmarkWALStandard(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "longbow-benchmark-std-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create standard backend directly
	file, err := os.OpenFile(filepath.Join(tmpDir, "wal.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		b.Fatalf("failed to create WAL file: %v", err)
	}
	defer file.Close()

	backend := &storage.FSBackend{
		F:    file,
		Path: filepath.Join(tmpDir, "wal.log"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(b.N) * 64) // ~64 bytes per entry

	for i := 0; i < b.N; i++ {
		if err := writeWALEntries(backend, 100); err != nil {
			b.Fatalf("failed to write WAL entries: %v", err)
		}

		if err := backend.Sync(); err != nil {
			b.Fatalf("failed to sync WAL: %v", err)
		}
	}
}

func BenchmarkWALIOUring(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "longbow-benchmark-iouring-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := storage.NewUringBackend(filepath.Join(tmpDir, "wal.log"))
	if err != nil {
		b.Fatalf("failed to create io_uring backend: %v", err)
	}
	defer backend.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(b.N) * 64) // ~64 bytes per entry

	for i := 0; i < b.N; i++ {
		if err := writeWALEntries(backend, 100); err != nil {
			b.Fatalf("failed to write WAL entries: %v", err)
		}

		if err := backend.Sync(); err != nil {
			b.Fatalf("failed to sync WAL: %v", err)
		}
	}
}
