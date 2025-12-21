package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func BenchmarkWAL_Write(b *testing.B) {
	// Setup temp dir
	tmpDir, err := os.MkdirTemp("", "wal_bench")
	require.NoError(b, err)
	defer os.RemoveAll(tmpDir)

	// Create dummy VectorStore (nil is fine for NewWAL usually, but StdWAL stores it)
	// StdWAL stores it but doesn't seem to use it in Write.
	vs := &VectorStore{}

	// Create WAL
	// This uses NewWAL which picks implementation based on OS
	wal := NewWAL(tmpDir, vs)
	require.NotNil(b, wal)
	defer wal.Close()

	// Create a record batch
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0, 3.0, 4.0}}
	rec := makeHNSWTestRecord(mem, 4, vectors) // using helper from hnsw_test.go
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("dataset_%d", i)
		err := wal.Write(name, uint64(i+1), 0, rec)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkWAL_Write_Sync(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "wal_bench_sync")
	require.NoError(b, err)
	defer os.RemoveAll(tmpDir)

	vs := &VectorStore{}
	wal := NewWAL(tmpDir, vs)
	require.NotNil(b, wal)
	defer wal.Close()

	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0, 3.0, 4.0}}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("dataset_%d", i)
		err := wal.Write(name, uint64(i+1), 0, rec)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if err := wal.Sync(); err != nil {
			b.Fatalf("Sync failed: %v", err)
		}
	}
}
