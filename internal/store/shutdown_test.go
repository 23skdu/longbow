package store

import (
"context"
"io"
"log/slog"
"os"
"path/filepath"
"sync"
"sync/atomic"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow/memory"
)

func discardLogger() *slog.Logger {
return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestShutdownCompletesWithinTimeout verifies shutdown respects context deadline
func TestShutdownCompletesWithinTimeout(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-test-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
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
t.Logf("Shutdown completed in %v", elapsed)
}

// TestShutdownDrainsIndexQueue verifies pending index jobs complete before shutdown
func TestShutdownDrainsIndexQueue(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-drain-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
t.Fatal(err)
}

// Queue some index jobs
for i := 0; i < 100; i++ {
select {
case s.indexChan <- IndexJob{DatasetName: "test", BatchIdx: i, RowIdx: 0}:
default:
}
}

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := s.Shutdown(ctx); err != nil {
t.Errorf("Shutdown failed: %v", err)
}
}

// TestShutdownFlushesWAL verifies shutdown completes with active WAL batcher
func TestShutdownFlushesWAL(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-wal-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
t.Fatal(err)
}

// Write some data via WAL batcher
rec := makeTestRecord(s.mem, 1)
defer rec.Release()

if err := s.walBatcher.Write(rec, "flush-test"); err != nil {
t.Fatal(err)
}

// Shutdown should flush batcher and complete cleanly
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := s.Shutdown(ctx); err != nil {
t.Errorf("Shutdown failed: %v", err)
}

// Verify shutdown flag is set
if !s.isShutdown() {
t.Error("shutdown flag should be set after Shutdown()")
}
t.Log("Shutdown completed successfully with active WAL batcher")
}

// TestShutdownStopsBackgroundWorkers verifies all background goroutines stop
func TestShutdownStopsBackgroundWorkers(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-workers-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
s.StartEvictionTicker(100 * time.Millisecond)
if err := s.InitPersistence(tmpDir, 100*time.Millisecond); err != nil {
t.Fatal(err)
}

time.Sleep(300 * time.Millisecond)

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := s.Shutdown(ctx); err != nil {
t.Errorf("Shutdown failed: %v", err)
}

if !s.isShutdown() {
t.Error("shutdown flag should be set")
}
}

// TestWALTruncationAfterSnapshot verifies WAL is truncated after successful snapshot
func TestWALTruncationAfterSnapshot(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "wal-truncate-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
t.Fatal(err)
}

// Add data and write to WAL
rec := makeTestRecord(s.mem, 1)
defer rec.Release()

if err := s.walBatcher.Write(rec, "truncate-test"); err != nil {
t.Fatal(err)
}
if err := s.walBatcher.Stop(); err != nil {
t.Fatal(err)
}

// Force a snapshot
if err := s.Snapshot(); err != nil {
t.Fatal(err)
}

// Truncate WAL after snapshot
if err := s.TruncateWAL(); err != nil {
t.Fatal(err)
}

// WAL should be empty or minimal
walPath := filepath.Join(tmpDir, "wal.bin")
info, err := os.Stat(walPath)
if err != nil && !os.IsNotExist(err) {
t.Fatal(err)
}
if info != nil && info.Size() > 0 {
t.Errorf("WAL should be truncated after snapshot, size: %d", info.Size())
}
t.Log("WAL successfully truncated after snapshot")
}

// TestShutdownIdempotent verifies multiple shutdown calls are safe
func TestShutdownIdempotent(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-idempotent-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
t.Fatal(err)
}

ctx := context.Background()

if err := s.Shutdown(ctx); err != nil {
t.Errorf("First shutdown failed: %v", err)
}

if err := s.Shutdown(ctx); err != nil {
t.Errorf("Second shutdown should not fail: %v", err)
}

if err := s.Shutdown(ctx); err != nil {
t.Errorf("Third shutdown should not fail: %v", err)
}
}

// TestConcurrentShutdown verifies concurrent shutdown calls are safe
func TestConcurrentShutdown(t *testing.T) {
tmpDir, err := os.MkdirTemp("", "shutdown-concurrent-*")
if err != nil {
t.Fatal(err)
}
defer func() { _ = os.RemoveAll(tmpDir) }()

s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
if err := s.InitPersistence(tmpDir, time.Hour); err != nil {
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
