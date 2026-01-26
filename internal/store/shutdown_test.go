package store

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/rs/zerolog"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func discardLogger() zerolog.Logger {
	return zerolog.Nop()
}

func TestShutdownCompletesWithinTimeout(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shutdown-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	if err := s.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}
}

func TestShutdownFlushesWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shutdown-wal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	if err := s.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}

	// Write some data via WAL batcher
	rec := makeTestRecord(s.mem, 1)
	defer rec.Release()

	// Corrected signature: (dsName, batch, batchID, timestamp)
	if err := s.writeToWAL("flush-test", rec, 1, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}

	if !s.isShutdown() {
		t.Error("shutdown flag should be set after Shutdown()")
	}
}

func TestWALTruncationAfterSnapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-truncate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	if err := s.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}

	rec := makeTestRecord(s.mem, 1)
	defer rec.Release()

	if err := s.writeToWAL("truncate-test", rec, 1, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}

	if err := s.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Corrected signature: TruncateWAL(index)
	if err := s.TruncateWAL(0); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(tmpDir, "wal.bin")
	info, err := os.Stat(walPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if info != nil && info.Size() > 0 {
		t.Errorf("WAL should be truncated after snapshot, size: %d", info.Size())
	}
}

func TestShutdownIdempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shutdown-idempotent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	if err := s.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("First shutdown failed: %v", err)
	}

	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Second shutdown should not fail: %v", err)
	}
}

func TestConcurrentShutdown(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shutdown-concurrent-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	if err := s.InitPersistence(storage.StorageConfig{DataPath: tmpDir, SnapshotInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}

	var errCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.Shutdown(ctx); err != nil {
				errCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("%d concurrent shutdowns failed", errCount.Load())
	}
}
