package scheduler

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// WorkItem represents a work item value.
type WorkItem interface{}

// WorkQueue represents a work queue value.
type WorkQueue struct {
	items   chan WorkItem
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closed  atomic.Bool
	backlog atomic.Int64
}

// NewWorkQueue constructs a new WorkQueue.
func NewWorkQueue(bufferSize int, workers int) *WorkQueue {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WorkQueue{
		items:   make(chan WorkItem, bufferSize),
		workers: workers,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start runs the start operation.
func (wq *WorkQueue) Start(handler func(ctx context.Context, item WorkItem)) {
	for i := 0; i < wq.workers; i++ {
		wq.wg.Add(1)
		go wq.worker(handler)
	}
}

func (wq *WorkQueue) worker(handler func(ctx context.Context, item WorkItem)) {
	defer wq.wg.Done()

	for {
		select {
		case <-wq.ctx.Done():
			return
		case item, ok := <-wq.items:
			if !ok {
				return
			}
			wq.backlog.Add(-1)
			handler(wq.ctx, item)
		}
	}
}

// Submit runs the submit operation.
func (wq *WorkQueue) Submit(item WorkItem) bool {
	select {
	case wq.items <- item:
		wq.backlog.Add(1)
		return true
	default:
		return false
	}
}

// SubmitBlocking runs the submit blocking operation.
func (wq *WorkQueue) SubmitBlocking(item WorkItem) {
	wq.items <- item
	wq.backlog.Add(1)
}

// Close runs the close operation.
func (wq *WorkQueue) Close() error {
	if wq.closed.Swap(true) {
		return nil
	}

	wq.cancel()
	close(wq.items)
	wq.wg.Wait()
	return nil
}

// Backlog runs the backlog operation.
func (wq *WorkQueue) Backlog() int64 {
	return wq.backlog.Load()
}

// Len runs the len operation.
func (wq *WorkQueue) Len() int {
	return len(wq.items)
}

// Priority represents a priority value.
type Priority int

const (
	// PriorityLow is a priority low constant.
	PriorityLow Priority = iota
	// PriorityNormal is a priority normal constant.
	PriorityNormal
	// PriorityHigh is a priority high constant.
	PriorityHigh
)

// PrioritizedWorkItem represents a prioritized work item value.
type PrioritizedWorkItem struct {
	Item     WorkItem
	Priority Priority
}

// PriorityWorkQueue represents a priority work queue value.
type PriorityWorkQueue struct {
	queues  [3]chan PrioritizedWorkItem
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closed  atomic.Bool
}

// NewPriorityWorkQueue constructs a new PriorityWorkQueue.
func NewPriorityWorkQueue(bufferSize int, workers int) *PriorityWorkQueue {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	ctx, cancel := context.WithCancel(context.Background())
	pwq := &PriorityWorkQueue{
		workers: workers,
		ctx:     ctx,
		cancel:  cancel,
	}

	for i := range pwq.queues {
		pwq.queues[i] = make(chan PrioritizedWorkItem, bufferSize)
	}

	return pwq
}

// Start runs the start operation.
func (pwq *PriorityWorkQueue) Start(handler func(ctx context.Context, item WorkItem)) {
	for i := 0; i < pwq.workers; i++ {
		pwq.wg.Add(1)
		go pwq.worker(handler)
	}
}

func (pwq *PriorityWorkQueue) worker(handler func(ctx context.Context, item WorkItem)) {
	defer pwq.wg.Done()

	for {
		select {
		case <-pwq.ctx.Done():
			return
		case item := <-pwq.queues[PriorityHigh]:
			handler(pwq.ctx, item.Item)
		case item := <-pwq.queues[PriorityNormal]:
			handler(pwq.ctx, item.Item)
		case item := <-pwq.queues[PriorityLow]:
			handler(pwq.ctx, item.Item)
		}
	}
}

// Submit runs the submit operation.
func (pwq *PriorityWorkQueue) Submit(item WorkItem, priority Priority) bool {
	if priority < PriorityLow {
		priority = PriorityLow
	}
	if priority > PriorityHigh {
		priority = PriorityHigh
	}

	select {
	case pwq.queues[priority] <- PrioritizedWorkItem{Item: item, Priority: priority}:
		return true
	default:
		return false
	}
}

// Close runs the close operation.
func (pwq *PriorityWorkQueue) Close() error {
	if pwq.closed.Swap(true) {
		return nil
	}

	pwq.cancel()
	for i := range pwq.queues {
		close(pwq.queues[i])
	}
	pwq.wg.Wait()
	return nil
}
