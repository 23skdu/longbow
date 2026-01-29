package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// createTestRecords creates multiple record batches for testing
func createTestRecords(count int) []arrow.RecordBatch {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "data", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)

	records := make([]arrow.RecordBatch, 0, count)
	for i := 0; i < count; i++ {
		builder := array.NewRecordBuilder(pool, schema)

		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.BinaryBuilder).Append([]byte(fmt.Sprintf("test-data-%d", i)))

		record := builder.NewRecordBatch()
		records = append(records, record)
		builder.Release()
	}

	return records
}

// writeRecordsToWAL writes records to WAL backend
func writeRecordsToWAL(ctx context.Context, backend storage.WALBackend, records []arrow.RecordBatch) error {
	for i, record := range records {
		if err := writeRecordToWAL(ctx, backend, record, fmt.Sprintf("dataset-%d", i), uint64(i)); err != nil {
			return fmt.Errorf("failed to write record %d: %w", i, err)
		}
	}
	return nil
}

// writeRecordToWAL writes a single record to WAL
func writeRecordToWAL(_ context.Context, backend storage.WALBackend, _ arrow.RecordBatch, dataset string, seq uint64) error {
	// Create a simple WAL entry format
	entry := struct {
		Dataset string
		Seq     uint64
	}{
		Dataset: dataset,
		Seq:     seq,
	}

	// Simple serialization for benchmarking
	data := fmt.Sprintf("%s|%d|%s", entry.Dataset, entry.Seq, "data")
	if _, err := backend.Write([]byte(data)); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	return nil
}

// TestWALPerformance performs end-to-end WAL performance testing
func TestWALPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping WAL performance test in short mode")
	}

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "longbow-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			t.Logf("failed to remove temp dir %s: %v", tmpDir, err)
		}
	}()

	// Test different backends
	testCases := []struct {
		name    string
		backend storage.WALBackend
	}{
		{"Standard WAL", createStandardWALBackend(t, tmpDir)},
		{"IOUring WAL", createIOUringWALBackend(t, tmpDir)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if err := tc.backend.Close(); err != nil {
					t.Logf("failed to close backend: %v", err)
				}
			}()

			ctx := context.Background()
			records := createTestRecords(1000)

			start := time.Now()
			if err := writeRecordsToWAL(ctx, tc.backend, records); err != nil {
				t.Fatalf("failed to write records: %v", err)
			}
			if err := tc.backend.Sync(); err != nil {
				t.Fatalf("failed to sync: %v", err)
			}
			duration := time.Since(start)

			t.Logf("WAL Performance for %s:", tc.name)
			t.Logf("  Records written: %d", len(records))
			t.Logf("  Total duration: %v", duration)
			t.Logf("  Records/second: %.2f", float64(len(records))/duration.Seconds())
			t.Logf("  Avg time/record: %v", duration/time.Duration(len(records)))
		})
	}
}

// createStandardWALBackend creates a standard WAL backend for testing
func createStandardWALBackend(t *testing.T, dir string) storage.WALBackend {
	backend, err := storage.NewFSBackend(filepath.Join(dir, "standard.wal"))
	if err != nil {
		t.Fatalf("failed to create standard WAL backend: %v", err)
	}
	return backend
}

// createIOUringWALBackend creates an io_uring WAL backend for testing
func createIOUringWALBackend(t *testing.T, dir string) storage.WALBackend {
	backend, err := storage.NewUringBackend(filepath.Join(dir, "iouring.wal"))
	if err != nil {
		t.Fatalf("failed to create io_uring WAL backend: %v", err)
	}
	return backend
}
