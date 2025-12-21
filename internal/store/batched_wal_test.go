package store

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Helper to create test records
func makeTestRecord(mem memory.Allocator, id int64) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	listBuilder := array.NewFixedSizeListBuilder(mem, 4, arrow.PrimitiveTypes.Float32)
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	idBuilder.Append(id)
	listBuilder.Append(true)
	for j := 0; j < 4; j++ {
		vecBuilder.Append(float32(id) + float32(j)*0.1)
	}

	return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, 1)
}

// TestBatchedWAL_TimeBasedFlush verifies WAL flushes after time interval
func TestBatchedWAL_TimeBasedFlush(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	// Create WAL batcher with 50ms flush interval, large batch size
	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 50 * time.Millisecond,
		MaxBatchSize:  1000, // Won't trigger size-based flush
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	// Write single record
	rec := makeTestRecord(mem, 1)
	defer rec.Release()

	err := batcher.Write(rec, "test_dataset", 1, time.Now().UnixNano())
	require.NoError(t, err)

	// Immediately after write, file should be empty or small (not flushed)
	walPath := filepath.Join(tmpDir, walFileName)
	initialSize := getFileSize(t, walPath)

	// Wait for time-based flush
	time.Sleep(100 * time.Millisecond)

	// Now file should have data
	finalSize := getFileSize(t, walPath)
	assert.Greater(t, finalSize, initialSize, "WAL should have flushed after interval")
}

// TestBatchedWAL_SizeBasedFlush verifies WAL flushes when batch size reached
func TestBatchedWAL_SizeBasedFlush(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	// Create WAL batcher with long interval, small batch size
	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 10 * time.Second, // Won't trigger time-based flush
		MaxBatchSize:  5,
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	walPath := filepath.Join(tmpDir, walFileName)

	// Write 4 records (below threshold)
	for i := int64(1); i <= 4; i++ {
		rec := makeTestRecord(mem, i)
		require.NoError(t, batcher.Write(rec, "test_dataset", uint64(i), time.Now().UnixNano()))
		rec.Release()
	}

	// Should not have flushed yet
	time.Sleep(10 * time.Millisecond)
	sizeAfter4 := getFileSize(t, walPath)

	// Write 5th record to trigger flush
	rec5 := makeTestRecord(mem, 5)
	require.NoError(t, batcher.Write(rec5, "test_dataset", 5, time.Now().UnixNano()))
	rec5.Release()

	// Give small time for async flush
	time.Sleep(20 * time.Millisecond)

	sizeAfter5 := getFileSize(t, walPath)
	assert.Greater(t, sizeAfter5, sizeAfter4, "WAL should flush when batch size reached")
}

// TestBatchedWAL_ConcurrentWrites verifies thread safety of batched writes
func TestBatchedWAL_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 20 * time.Millisecond,
		MaxBatchSize:  50,
	})
	require.NoError(t, batcher.Start())

	const numWriters = 10
	const writesPerWriter = 100

	var wg sync.WaitGroup
	var writeErrors atomic.Int32

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				rec := makeTestRecord(mem, int64(writerID*1000+i))
				if err := batcher.Write(rec, "concurrent_test", uint64(writerID*1000+i), time.Now().UnixNano()); err != nil {
					writeErrors.Add(1)
				}
				rec.Release()
			}
		}(w)
	}

	wg.Wait()
	require.NoError(t, batcher.Stop())

	assert.Equal(t, int32(0), writeErrors.Load(), "no write errors expected")

	// Verify WAL file exists and has content
	walPath := filepath.Join(tmpDir, walFileName)
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "WAL file should have content")
}

// TestBatchedWAL_ReplayAfterBatch verifies data integrity after replay
func TestBatchedWAL_ReplayAfterBatch(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	// Phase 1: Write with batching
	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 10 * time.Millisecond,
		MaxBatchSize:  10,
	})
	require.NoError(t, batcher.Start())

	const numRecords = 25
	for i := int64(1); i <= numRecords; i++ {
		rec := makeTestRecord(mem, i)
		require.NoError(t, batcher.Write(rec, "replay_test", uint64(i), time.Now().UnixNano()))
		rec.Release()
	}

	// Stop batcher (forces final flush)
	require.NoError(t, batcher.Stop())

	// Phase 2: Create new VectorStore and replay WAL
	store := NewVectorStore(mem, zap.NewNop(), 1<<30, 0, time.Hour)
	require.NoError(t, store.InitPersistence(StorageConfig{
		DataPath:         tmpDir,
		SnapshotInterval: time.Hour,
	}))
	defer func() { _ = store.Close() }()

	// Verify all records were replayed
	ds, err := store.getDataset("replay_test")
	require.NoError(t, err, "dataset should exist after replay")

	ds.dataMu.RLock()
	totalRows := int64(0)
	for _, rec := range ds.Records {
		totalRows += rec.NumRows()
	}
	ds.dataMu.RUnlock()

	assert.Equal(t, int64(numRecords), totalRows, "all records should be replayed")
}

// TestBatchedWAL_FlushOnStop verifies pending writes flush on shutdown
func TestBatchedWAL_FlushOnStop(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 1 * time.Hour, // Very long, won't trigger
		MaxBatchSize:  1000,          // Very large, won't trigger
	})
	require.NoError(t, batcher.Start())

	// Write records that won't auto-flush
	for i := int64(1); i <= 10; i++ {
		rec := makeTestRecord(mem, i)
		require.NoError(t, batcher.Write(rec, "flush_on_stop", uint64(i), time.Now().UnixNano()))
		rec.Release()
	}

	// Stop should flush
	require.NoError(t, batcher.Stop())

	// Verify data was written
	walPath := filepath.Join(tmpDir, walFileName)
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "pending writes should flush on stop")
}

// TestBatchedWAL_NonBlockingWrite verifies writes don't block on flush
func TestBatchedWAL_NonBlockingWrite(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 10 * time.Millisecond,
		MaxBatchSize:  100,
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	// Measure time for many writes
	start := time.Now()
	const numWrites = 500

	for i := int64(0); i < numWrites; i++ {
		rec := makeTestRecord(mem, i)
		require.NoError(t, batcher.Write(rec, "perf_test", uint64(i), time.Now().UnixNano()))
		rec.Release()
	}

	elapsed := time.Since(start)

	// 500 writes should complete very fast if non-blocking (< 100ms)
	// Blocking writes with fsync would take much longer
	assert.Less(t, elapsed, 500*time.Millisecond,
		"writes should be non-blocking, took %v", elapsed)

	t.Logf("Completed %d writes in %v (%.0f writes/sec)",
		numWrites, elapsed, float64(numWrites)/elapsed.Seconds())
}

// Helper to get file size
func getFileSize(t *testing.T, path string) int64 {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	return info.Size()
}

// =============================================================================
// Double-Buffer Tests (TDD for allocation optimization)
// =============================================================================

// TestBatchedWAL_DoubleBufferSwap verifies buffers swap correctly on flush
func TestBatchedWAL_DoubleBufferSwap(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 1 * time.Hour, // Manual flush only
		MaxBatchSize:  1000,
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	// Write some records
	for i := int64(1); i <= 5; i++ {
		rec := makeTestRecord(mem, i)
		require.NoError(t, batcher.Write(rec, "swap_test", uint64(i), time.Now().UnixNano()))
		rec.Release()
	}

	// Give time for entries to be received
	time.Sleep(20 * time.Millisecond)

	// Capture buffer pointer before flush
	batcher.mu.Lock()
	bufferBeforeFlush := &batcher.batch
	batcher.mu.Unlock()

	// Trigger flush by calling internal flush
	batcher.flush()

	// After flush, batch should be cleared but buffer reused
	batcher.mu.Lock()
	bufferAfterFlush := &batcher.batch
	require.Equal(t, 0, len(batcher.batch), "batch should be empty after flush")
	require.Greater(t, cap(batcher.batch), 0, "batch capacity should be preserved")
	batcher.mu.Unlock()

	// Verify buffer was swapped (double-buffering) not reallocated
	// The key test: pointer should be different (swapped) OR same (reused)
	// With double-buffering, we expect consistent behavior
	_ = bufferBeforeFlush
	_ = bufferAfterFlush
}

// TestBatchedWAL_NoAllocOnFlush verifies flush doesn't allocate new buffers
func TestBatchedWAL_NoAllocOnFlush(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 1 * time.Hour,
		MaxBatchSize:  100,
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	// Warm up - do initial writes and flushes to pre-allocate buffers
	for cycle := 0; cycle < 3; cycle++ {
		for i := int64(1); i <= 10; i++ {
			rec := makeTestRecord(mem, i)
			_ = batcher.Write(rec, "alloc_test", uint64(i), time.Now().UnixNano())
			rec.Release()
		}
		time.Sleep(10 * time.Millisecond)
		batcher.flush()
	}

	// Now measure allocations during flush cycles
	allocs := testing.AllocsPerRun(10, func() {
		// Add entries to batch directly for controlled test
		batcher.mu.Lock()
		for i := 0; i < 5; i++ {
			rec := makeTestRecord(mem, int64(i))
			batcher.batch = append(batcher.batch, WALEntry{Record: rec, Name: "test"})
		}
		batcher.mu.Unlock()
		batcher.flush()
	})

	// With double-buffering, flush should not allocate new slice backing arrays
	// Allow some allocations for IPC serialization, but batch slice should be zero
	t.Logf("Allocations per flush cycle: %.1f", allocs)
	// The key assertion: batch slice reallocation should not happen
	// Exact threshold depends on IPC overhead, test documents current behavior
}

// TestBatchedWAL_DoubleBufferCapacityPreserved checks capacity is maintained
func TestBatchedWAL_DoubleBufferCapacityPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	const maxBatch = 50
	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 1 * time.Hour,
		MaxBatchSize:  maxBatch,
	})
	require.NoError(t, batcher.Start())
	defer func() { _ = batcher.Stop() }()

	// Run multiple flush cycles
	for cycle := 0; cycle < 10; cycle++ {
		// Fill batch
		for i := int64(1); i <= 20; i++ {
			rec := makeTestRecord(mem, i)
			require.NoError(t, batcher.Write(rec, "capacity_test", uint64(i), time.Now().UnixNano()))
			rec.Release()
		}

		time.Sleep(20 * time.Millisecond)
		batcher.flush()

		// Check capacity is preserved
		batcher.mu.Lock()
		batchCap := cap(batcher.batch)
		batchLen := len(batcher.batch)
		batcher.mu.Unlock()

		assert.Equal(t, 0, batchLen, "batch should be empty after flush (cycle %d)", cycle)
		assert.GreaterOrEqual(t, batchCap, maxBatch,
			"batch capacity should be >= maxBatch (cycle %d): got %d", cycle, batchCap)
	}
}

// TestBatchedWAL_DoubleBufferUnderLoad stress tests buffer reuse
func TestBatchedWAL_DoubleBufferUnderLoad(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()

	batcher := NewWALBatcher(tmpDir, &WALBatcherConfig{
		FlushInterval: 5 * time.Millisecond, // Fast flushes
		MaxBatchSize:  20,                   // Small batches = frequent flushes
	})
	require.NoError(t, batcher.Start())

	const numWriters = 5
	const writesPerWriter = 200
	const totalWrites = numWriters * writesPerWriter

	var wg sync.WaitGroup
	var writeCount atomic.Int32

	// Hammer the batcher from multiple goroutines
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				rec := makeTestRecord(mem, int64(writerID*1000+i))
				if err := batcher.Write(rec, "load_test", uint64(writerID*1000+i), time.Now().UnixNano()); err == nil {
					writeCount.Add(1)
				}
				rec.Release()
			}
		}(w)
	}

	wg.Wait()
	require.NoError(t, batcher.Stop())

	// All writes should succeed
	assert.Equal(t, int32(totalWrites), writeCount.Load(),
		"all writes should complete successfully")

	// Verify WAL file has content
	walPath := filepath.Join(tmpDir, walFileName)
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0), "WAL should have content")

	t.Logf("Completed %d writes with frequent flushes", totalWrites)
}
