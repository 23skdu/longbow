package scheduler

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

type WorkItem interface{}

type WorkQueue struct {
	items   chan WorkItem
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closed  atomic.Bool
	backlog atomic.Int64
}

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

func (wq *WorkQueue) Submit(item WorkItem) bool {
	select {
	case wq.items <- item:
		wq.backlog.Add(1)
		return true
	default:
		return false
	}
}

func (wq *WorkQueue) SubmitBlocking(item WorkItem) {
	wq.items <- item
	wq.backlog.Add(1)
}

func (wq *WorkQueue) Close() error {
	if wq.closed.Swap(true) {
		return nil
	}

	wq.cancel()
	close(wq.items)
	wq.wg.Wait()
	return nil
}

func (wq *WorkQueue) Backlog() int64 {
	return wq.backlog.Load()
}

func (wq *WorkQueue) Len() int {
	return len(wq.items)
}

type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
)

type PrioritizedWorkItem struct {
	Item     WorkItem
	Priority Priority
}

type PriorityWorkQueue struct {
	queues  [3]chan PrioritizedWorkItem
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closed  atomic.Bool
}

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
