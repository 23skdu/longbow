package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func BenchmarkWAL_Write(b *testing.B) {
	// Setup temp dir
	tmpDir, err := os.MkdirTemp("", "wal_bench")
	require.NoError(b, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create WAL
	wal := NewWAL(tmpDir)
	require.NotNil(b, wal)
	defer func() { _ = wal.Close() }()

	// Create a record batch
	mem := memory.NewGoAllocator()
	rec := makeTestRecord(mem, 1)
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("dataset_%d", i)
		ts := time.Now().UnixNano()
		err := wal.Write(name, uint64(i+1), ts, rec)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkWAL_Write_Sync(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_sync")
	require.NoError(b, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	wal := NewWAL(tmpDir)
	require.NotNil(b, wal)
	defer func() { _ = wal.Close() }()

	mem := memory.NewGoAllocator()
	rec := makeTestRecord(mem, 1)
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("dataset_%d", i)
		ts := time.Now().UnixNano()
		err := wal.Write(name, uint64(i+1), ts, rec)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := wal.Sync(); err != nil {
			b.Fatalf("Sync failed: %v", err)
		}
	}
}
