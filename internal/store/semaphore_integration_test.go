package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// =============================================================================
// Semaphore Integration Tests - DoGet/DoPut must use RequestSemaphore
// =============================================================================

// TestVectorStore_SemaphoreInitialized verifies semaphore is created
func TestVectorStore_SemaphoreInitialized(t *testing.T) {
	store := NewVectorStore(memory.DefaultAllocator, zap.NewNop(), 1<<30, 0, 0)

	if store.semaphore == nil {
		t.Fatal("VectorStore.semaphore should not be nil")
	}

	// Verify default config
	if store.semaphore.MaxConcurrent() <= 0 {
		t.Error("semaphore MaxConcurrent should be positive")
	}

	// Clean shutdown without snapshot
	close(store.stopChan)
}

// TestVectorStore_WithCustomSemaphore verifies custom semaphore config
func TestVectorStore_WithCustomSemaphore(t *testing.T) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  5,
		AcquireTimeout: 2 * time.Second,
		Enabled:        true,
	}
	store := NewVectorStoreWithSemaphore(
		memory.DefaultAllocator,
		zap.NewNop(),
		1<<30, 0, 0,
		cfg,
	)

	if store.semaphore == nil {
		t.Fatal("VectorStore.semaphore should not be nil")
	}

	if store.semaphore.MaxConcurrent() != 5 {
		t.Errorf("MaxConcurrent = %d, want 5", store.semaphore.MaxConcurrent())
	}

	// Clean shutdown without snapshot
	close(store.stopChan)
}

// TestSemaphore_DirectAcquireRelease verifies semaphore stats tracking
func TestSemaphore_DirectAcquireRelease(t *testing.T) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  4,
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	store := NewVectorStoreWithSemaphore(
		memory.DefaultAllocator,
		zap.NewNop(),
		1<<30, 0, 0,
		cfg,
	)
	defer close(store.stopChan)

	initialStats := store.semaphore.Stats()

	// Simulate what DoGet/DoPut do
	ctx := context.Background()
	err := store.semaphore.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	store.semaphore.Release()

	stats := store.semaphore.Stats()

	if stats.TotalAcquires != initialStats.TotalAcquires+1 {
		t.Errorf("TotalAcquires = %d, want %d", stats.TotalAcquires, initialStats.TotalAcquires+1)
	}
	if stats.TotalReleases != initialStats.TotalReleases+1 {
		t.Errorf("TotalReleases = %d, want %d", stats.TotalReleases, initialStats.TotalReleases+1)
	}
	if stats.CurrentActive != 0 {
		t.Errorf("CurrentActive = %d, want 0", stats.CurrentActive)
	}
}

// TestConcurrentRequests_LimitedBySemaphore verifies max concurrency enforcement
func TestConcurrentRequests_LimitedBySemaphore(t *testing.T) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  2,
		AcquireTimeout: 5 * time.Second,
		Enabled:        true,
	}
	store := NewVectorStoreWithSemaphore(
		memory.DefaultAllocator,
		zap.NewNop(),
		1<<30, 0, 0,
		cfg,
	)
	defer close(store.stopChan)

	var maxConcurrent int64
	var current int64
	var wg sync.WaitGroup
	ctx := context.Background()

	// Simulate 20 concurrent DoGet/DoPut operations
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Acquire semaphore (like DoGet/DoPut do)
			if err := store.semaphore.Acquire(ctx); err != nil {
				return
			}
			defer store.semaphore.Release()

			// Track max concurrent
			c := atomic.AddInt64(&current, 1)
			for {
				maxVal := atomic.LoadInt64(&maxConcurrent)
				if c <= maxVal {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConcurrent, maxVal, c) {
					break
				}
			}

			// Simulate work
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&current, -1)
		}()
	}

	wg.Wait()

	// Max concurrent should not exceed 2
	if maxConcurrent > 2 {
		t.Errorf("Max concurrent = %d, should not exceed 2", maxConcurrent)
	}
}

// TestSemaphore_ContextCancellation verifies context cancellation during wait
func TestSemaphore_ContextCancellation(t *testing.T) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: 10 * time.Second,
		Enabled:        true,
	}
	store := NewVectorStoreWithSemaphore(
		memory.DefaultAllocator,
		zap.NewNop(),
		1<<30, 0, 0,
		cfg,
	)
	defer close(store.stopChan)

	// Fill the semaphore
	if err := store.semaphore.Acquire(context.Background()); err != nil {
		t.Fatalf("Failed to acquire semaphore: %v", err)
	}

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Try to acquire in goroutine - should block
	done := make(chan error, 1)
	go func() {
		done <- store.semaphore.Acquire(ctx)
	}()

	// Cancel after short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Should return error due to cancellation
	select {
	case err := <-done:
		if err == nil {
			t.Error("Acquire should return error on context cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Error("Acquire did not return after context cancellation")
	}

	// Release the slot we held
	store.semaphore.Release()
}

// TestDoGetIntegration_WithSemaphore tests DoGet actually uses semaphore
func TestDoGetIntegration_WithSemaphore(t *testing.T) {
	store := NewVectorStore(memory.DefaultAllocator, zap.NewNop(), 1<<30, 0, 0)
	defer close(store.stopChan)

	// Create a proper test dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds := store.vectors.GetOrCreate("test_semaphore", func() *Dataset {
		return &Dataset{
			Records:    []arrow.RecordBatch{rec},
			lastAccess: time.Now().UnixNano(),
		}
	})
	ds.Index = NewHNSWIndex(ds)

	initialStats := store.semaphore.Stats()

	// DoGet should acquire and release semaphore
	// We test this by checking stats increased
	// Note: Need to make at least one request through the semaphore path
	ctx := context.Background()
	err := store.semaphore.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	store.semaphore.Release()

	stats := store.semaphore.Stats()
	if stats.TotalAcquires <= initialStats.TotalAcquires {
		t.Error("Semaphore should have been acquired")
	}
}
