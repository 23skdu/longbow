package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedWAL_SyncLatency(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "wal_sync_test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	backend, err := NewFSBackend(filepath.Join(tmpDir, "wal.log"))
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	// Use a long flush delay to make the bug obvious
	// If Sync works correctly, it should override this delay.
	flushDelay := 5 * time.Second
	wal := NewBufferedWAL(backend, 1024*1024, flushDelay)
	defer func() { _ = wal.Close() }()

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(1)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// 1. Induce a "Busy" state
	// We want to trigger a flush, and while it's running (simulated or real), call Sync.
	// Since we can't easily hook into "isFlushing" without using internal APIs or slow I/O,
	// we'll flood the WAL to trigger an auto-flush, then immediately do another write + Sync.

	// Write 1: Should trigger flush if we fill buffer?
	// Or we rely on the race:
	// T1: Write Loop
	// T2: Write + Sync

	// Let's use a mocked backend to inject delay?
	// The buffered WAL takes a WALBackend interface, but NewBufferedWAL uses it directly.
	// We can't swap it easily unless we use a constructor that accepts the interface (which it does).

	// Better: Use a mock WALBackend that sleeps in Write/Sync to simulate slow I/O.
	slowBackend := &SlowBackend{
		Backend: backend,
		Delay:   500 * time.Millisecond,
	}
	walWithSlowBackend := NewBufferedWAL(slowBackend, 10*1024*1024, flushDelay) // Large buffer to avoid auto-flush
	defer func() { _ = walWithSlowBackend.Close() }()

	var wg sync.WaitGroup
	wg.Add(1)

	// 1. Write Batch 1
	err = walWithSlowBackend.Write("batch1", 1, 0, rec)
	require.NoError(t, err)

	// 2. Force flush of Batch 1 in background
	// This will take 500ms
	go func() {
		_ = walWithSlowBackend.Sync()
		wg.Done()
	}()

	// 3. Wait a bit to ensure flush has started (backend is sleeping)
	time.Sleep(100 * time.Millisecond)

	// 4. Write Batch 2 (Target)
	// This goes into the NEW buffer (since SwapBuffer happened in step 2)
	// But the flush loop is currently busy with Batch 1.
	start := time.Now()
	err = walWithSlowBackend.Write("batch2", 2, 0, rec)
	require.NoError(t, err)

	// 5. Sync Batch 2
	// If bug exists: Sync sees isFlushing=true (Batch 1), goes to wait.
	// Batch 1 finishes. Sync wakes. Buffer has Batch 2.
	// BUT Sync loop just checks flushedSeq. It doesn't re-check if it can trigger a flush.
	// So it waits for Ticker (5s).
	err = walWithSlowBackend.Sync()
	require.NoError(t, err)

	duration := time.Since(start)

	wg.Wait()

	// If implementation is naive, Sync might miss the "flush train" and wait for the next Ticker (5s).
	// If optimized, it should force a subsequent flush immediately after the slow one.
	// We assert that it was reasonably fast (< 2s) despite the 5s ticker.
	// The backend delay is 500ms, so we expect ~500ms-1s.
	t.Logf("Sync took %v", duration)
	assert.Less(t, duration, 2*time.Second, "Sync took too long, arguably waiting for ticker")
}

// SlowBackend wraps a WALBackend and adds delay
type SlowBackend struct {
	Backend WALBackend
	Delay   time.Duration
}

func (b *SlowBackend) Write(p []byte) (int, error) {
	time.Sleep(b.Delay)
	return b.Backend.Write(p)
}
func (b *SlowBackend) Sync() error {
	time.Sleep(b.Delay)
	return b.Backend.Sync()
}
func (b *SlowBackend) Close() error   { return b.Backend.Close() }
func (b *SlowBackend) Name() string   { return b.Backend.Name() }
func (b *SlowBackend) File() *os.File { return b.Backend.File() }
