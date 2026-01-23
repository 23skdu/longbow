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

func TestVectorStore_ShutdownTimeoutEnforcement(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 0, 0)

	// Start a worker that will hang
	hung := make(chan struct{})
	vs.workerWg.Add(1)
	go func() {
		defer vs.workerWg.Done()
		<-hung // Hang forever
	}()

	// Try to close with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- vs.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		// Should timeout and return context error
		require.Error(t, err)
		assert.Contains(t, err.Error(), context.DeadlineExceeded.Error())
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Shutdown should have returned within timeout")
	}

	// Cleanup - unhang the worker
	close(hung)
}

func TestVectorStore_CloseTimeoutEnforcement(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 0, 0)

	// Start a worker that will hang
	hung := make(chan struct{})
	vs.workerWg.Add(1)
	go func() {
		defer vs.workerWg.Done()
		<-hung // Hang forever
	}()

	start := time.Now()
	err := vs.Close() // Uses 30s timeout internally
	elapsed := time.Since(start)

	// Should complete within reasonable time (much less than 30s since no real work)
	assert.Less(t, elapsed, 100*time.Millisecond, "Close should return quickly even with hung workers")
	// The Close method should still succeed (idempotent)
	require.NoError(t, err)

	// Cleanup
	close(hung)
}

func TestVectorStore_ShutdownWithHungWorkers(t *testing.T) {
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 0, 0)

	// Start multiple hung workers
	numHung := 3
	hungChans := make([]chan struct{}, numHung)
	for i := 0; i < numHung; i++ {
		hungChans[i] = make(chan struct{})
		vs.workerWg.Add(1)
		go func(hung chan struct{}) {
			defer vs.workerWg.Done()
			<-hung // Hang
		}(hungChans[i])
	}

	// Shutdown should still complete (with timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := vs.Shutdown(ctx)
	require.Error(t, err) // Should timeout
	assert.Contains(t, err.Error(), context.DeadlineExceeded.Error())

	// Cleanup
	for _, hung := range hungChans {
		close(hung)
	}
}
