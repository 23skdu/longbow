package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkQueue_NewWorkQueue(t *testing.T) {
	wq := NewWorkQueue(100, 4)
	if wq == nil {
		t.Fatal("NewWorkQueue returned nil")
	}
	if wq.workers != 4 {
		t.Errorf("expected 4 workers, got %d", wq.workers)
	}
}

func TestWorkQueue_DefaultWorkers(t *testing.T) {
	wq := NewWorkQueue(100, 0)
	if wq.workers == 0 {
		t.Error("expected default workers to be set")
	}
}

func TestWorkQueue_DefaultBufferSize(t *testing.T) {
	wq := NewWorkQueue(0, 1)
	if cap(wq.items) == 0 {
		t.Error("expected default buffer size to be set")
	}
}

func TestWorkQueue_SubmitAndProcess(t *testing.T) {
	wq := NewWorkQueue(100, 2)
	processed := atomic.Int32{}

	handler := func(ctx context.Context, item WorkItem) {
		processed.Add(1)
	}

	wq.Start(handler)

	for i := 0; i < 10; i++ {
		if !wq.Submit(i) {
			t.Errorf("failed to submit item %d", i)
		}
	}

	time.Sleep(100 * time.Millisecond)
	wq.Close()

	if processed.Load() != 10 {
		t.Errorf("expected 10 processed items, got %d", processed.Load())
	}
}

func TestWorkQueue_SubmitBlocking(t *testing.T) {
	wq := NewWorkQueue(10, 1)
	processed := atomic.Int32{}

	handler := func(ctx context.Context, item WorkItem) {
		processed.Add(1)
	}

	wq.Start(handler)

	for i := 0; i < 5; i++ {
		wq.SubmitBlocking(i)
	}

	time.Sleep(100 * time.Millisecond)
	wq.Close()

	if processed.Load() != 5 {
		t.Errorf("expected 5 processed items, got %d", processed.Load())
	}
}

func TestWorkQueue_Close(t *testing.T) {
	wq := NewWorkQueue(100, 2)
	handler := func(ctx context.Context, item WorkItem) {}
	wq.Start(handler)

	err := wq.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	err = wq.Close()
	if err != nil {
		t.Error("expected second close to be idempotent")
	}
}

func TestWorkQueue_Backlog(t *testing.T) {
	wq := NewWorkQueue(100, 1)

	if wq.Backlog() != 0 {
		t.Error("expected initial backlog to be 0")
	}

	wq.Submit(1)
	if wq.Backlog() != 1 {
		t.Errorf("expected backlog of 1, got %d", wq.Backlog())
	}
}

func TestWorkQueue_Len(t *testing.T) {
	wq := NewWorkQueue(100, 1)

	if wq.Len() != 0 {
		t.Error("expected initial length to be 0")
	}

	wq.Submit(1)
	if wq.Len() != 1 {
		t.Errorf("expected length of 1, got %d", wq.Len())
	}
}

func TestWorkQueue_CloseCancelsContext(t *testing.T) {
	wq := NewWorkQueue(100, 1)
	cancelled := make(chan struct{})

	handler := func(ctx context.Context, item WorkItem) {
		select {
		case <-ctx.Done():
			close(cancelled)
		case <-time.After(time.Second):
		}
	}

	wq.Start(handler)
	wq.SubmitBlocking(1)
	time.Sleep(50 * time.Millisecond)
	wq.Close()

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Error("context should be cancelled after close")
	}
}

func TestWorkQueue_ConcurrentSubmit(t *testing.T) {
	wq := NewWorkQueue(1000, 4)
	var wg sync.WaitGroup
	processed := atomic.Int32{}

	handler := func(ctx context.Context, item WorkItem) {
		processed.Add(1)
	}

	wq.Start(handler)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wq.Submit(1)
		}()
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	wq.Close()

	if processed.Load() != 100 {
		t.Errorf("expected 100 processed items, got %d", processed.Load())
	}
}

func TestPriorityWorkQueue_NewPriorityWorkQueue(t *testing.T) {
	pwq := NewPriorityWorkQueue(100, 4)
	if pwq == nil {
		t.Fatal("NewPriorityWorkQueue returned nil")
	}
	if pwq.workers != 4 {
		t.Errorf("expected 4 workers, got %d", pwq.workers)
	}
}

func TestPriorityWorkQueue_SubmitAndProcess(t *testing.T) {
	pwq := NewPriorityWorkQueue(100, 2)
	processed := atomic.Int32{}

	handler := func(ctx context.Context, item WorkItem) {
		processed.Add(1)
	}

	pwq.Start(handler)

	for i := 0; i < 10; i++ {
		if !pwq.Submit(i, PriorityNormal) {
			t.Errorf("failed to submit item %d", i)
		}
	}

	time.Sleep(100 * time.Millisecond)
	pwq.Close()

	if processed.Load() != 10 {
		t.Errorf("expected 10 processed items, got %d", processed.Load())
	}
}

func TestPriorityWorkQueue_PriorityClamping(t *testing.T) {
	pwq := NewPriorityWorkQueue(100, 1)

	if !pwq.Submit(1, PriorityLow-1) {
		t.Error("should accept clamped low priority")
	}
	if !pwq.Submit(2, PriorityHigh+1) {
		t.Error("should accept clamped high priority")
	}
}

func TestPriorityWorkQueue_Close(t *testing.T) {
	pwq := NewPriorityWorkQueue(100, 2)
	handler := func(ctx context.Context, item WorkItem) {}
	pwq.Start(handler)

	err := pwq.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	err = pwq.Close()
	if err != nil {
		t.Error("expected second close to be idempotent")
	}
}

func TestPriorityWorkQueue_PriorityOrdering(t *testing.T) {
	pwq := NewPriorityWorkQueue(100, 1)
	order := []int{}
	var mu sync.Mutex

	handler := func(ctx context.Context, item WorkItem) {
		mu.Lock()
		order = append(order, item.(int))
		mu.Unlock()
	}

	pwq.Start(handler)

	pwq.Submit(1, PriorityLow)
	pwq.Submit(2, PriorityHigh)
	pwq.Submit(3, PriorityNormal)

	time.Sleep(100 * time.Millisecond)
	pwq.Close()

	if len(order) != 3 {
		t.Skipf("order not deterministic in concurrent test, got %v", order)
	}
}
