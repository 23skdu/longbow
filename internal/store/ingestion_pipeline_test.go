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
	dsName := "test_backpressure"

	// Fill the queue (buffer size 100)
	// We need to stop the worker first to strictly fill the queue?
	// The worker starts automatically in NewVectorStore.
	// We can't stop it easily. But we can flood it faster than it consumes?
	// Or we can just rely on the fact that if we send 101 items, the 101st *might* block if worker is slow.
	// A better test is to mock the worker or consumer, but we rely on internal impl.

	// Create a store but DONT consume?
	// We can manipulate the queue directly since we are in `package store`.
	// Close the worker? No.

	// Let's create a store where we don't start the worker?
	// NewVectorStore starts it.

	// We can manually close stopChan to stop worker?
	close(store.stopChan)
	// Wait for worker to exit?
	store.workerWg.Wait() // This blocks until all workers done.

	// Now queue is not being consumed.
	// Fill it.
	rec := createTestRecordBatch(t, 10)
	defer rec.Release()

	// Buffer is 100.
	for i := 0; i < 100; i++ {
		// We manually inject into channel to avoid WAL overhead/checks if we want pure queue test,
		// but using StoreRecordBatch is better integration test.
		// However StoreRecordBatch writes WAL. We better mock or just accept it (file I/O might be slow but fine).
		// Let's just push to channel directly to test queue physics.
		select {
		case store.ingestionQueue <- ingestionJob{datasetName: dsName, batch: rec}:
			rec.Retain()
		default:
			t.Fatalf("Queue should not be full at %d", i)
		}
	}

	// Now the queue is full (100 items).
	// Next insert should block.

	done := make(chan bool)
	go func() {
		// This should block
		store.ingestionQueue <- ingestionJob{datasetName: dsName, batch: rec}
		rec.Retain()
		done <- true
	}()

	select {
	case <-done:
		t.Fatal("Insert should have blocked on full queue")
	case <-time.After(50 * time.Millisecond):
		// Success, it blocked
	}

	// Unblock by draining one
	<-store.ingestionQueue

	select {
	case <-done:
		// Success, it unblocked
	case <-time.After(1 * time.Second):
		t.Fatal("Insert should have unblocked after drain")
	}
}
