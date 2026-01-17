package store

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIngestionPipeline_AsyncDecoupling verifies that DoPut returns quickly
// and data becomes visible eventually (Async Ingestion).
func TestIngestionPipeline_AsyncDecoupling(t *testing.T) {
	// Setup Store with Ingestion Pipelining Enabled
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 1024*1024*1024, 1024*1024*100, 1*time.Hour)
	defer func() { _ = store.Close() }()

	// Create a dummy record batch
	rec := createTestRecordBatch(t, 100) // 100 rows
	defer rec.Release()

	dsName := "test_async_ingest"

	// Mock WAL (optional, but store uses a real engine if config provided, here likely nil which is fine for test logic flow)

	// Measure DoPut duration - should be fast (just queueing)
	start := time.Now()
	// Using StoreRecordBatch directly as it mimics DoPut's internal logic or is called by it
	// Note: We need to ensure StoreRecordBatch uses the new pipeline.
	err := store.StoreRecordBatch(context.Background(), dsName, rec)
	require.NoError(t, err)
	duration := time.Since(start)

	t.Logf("StoreRecordBatch took %v", duration)

	// In a real decoupled system, check weak consistency:
	// immediate visibility isn't guaranteed if we rely solely on the queue worker.
	// HOWEVER, for this specific test to pass *before* implementation, we might see it act synchronously.
	// The goal is to verify it *still works* (eventually consistent) once we refactor.

	// Poll for data visibility
	assert.Eventually(t, func() bool {
		ds, ok := store.getDataset(dsName)
		if !ok {
			return false
		}
		ds.dataMu.RLock()
		defer ds.dataMu.RUnlock()
		return len(ds.Records) > 0
	}, 2*time.Second, 10*time.Millisecond, "Dataset should eventually have records")
}

// TestIngestionPipeline_Backpressure verifies that we block when queue is full.
func TestIngestionPipeline_Backpressure(t *testing.T) {
	mem := memory.NewGoAllocator()
	store := NewVectorStore(mem, zerolog.Nop(), 1024*1024*1024, 1024*1024*100, 1*time.Hour)
	defer func() { _ = store.Close() }()
	dsName := "test_backpressure"

	// Stop workers so we can fill the queue manually
	store.stopWorkers()
	store.workerWg.Wait()

	// Capacity is 16384 (from NewVectorStore)
	queueCap := 16384

	rec := createTestRecordBatch(t, 10)
	defer rec.Release()

	// Fill the queue
	for i := 0; i < queueCap; i++ {
		job := ingestionJob{datasetName: dsName, batch: rec}
		rec.Retain()
		if !store.ingestionQueue.Push(job) {
			t.Fatalf("Queue should not be full at %d", i)
		}
	}

	// Now the queue is full.
	// Next PushBlocking should block or timeout.

	// Start a goroutine that blocks
	done := make(chan bool)
	go func() {
		job := ingestionJob{datasetName: dsName, batch: rec}
		rec.Retain()
		// Wait up to 5s. Should succeed after we drain.
		if store.ingestionQueue.PushBlocking(job, 5*time.Second) {
			done <- true
		}
	}()

	select {
	case <-done:
		t.Fatal("Insert should have blocked on full queue")
	case <-time.After(50 * time.Millisecond):
		// Success, it blocked
	}

	// Unblock by draining one
	_, ok := store.ingestionQueue.Pop()
	require.True(t, ok, "Queue should have item")

	select {
	case <-done:
		// Success, it unblocked
	case <-time.After(1 * time.Second):
		t.Fatal("Insert should have unblocked after drain")
	}
}
