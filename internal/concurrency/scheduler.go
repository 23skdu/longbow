package concurrency

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type WorkStealingScheduler[T any] struct {
	workers    int
	queues     []*WorkQueue[T]
	stealIndex atomic.Uint32
	done       chan struct{}
}

type WorkQueue[T any] struct {
	mu    sync.Mutex
	items []T
}

func NewWorkStealingScheduler[T any](numWorkers int) *WorkStealingScheduler[T] {
	if numWorkers < 1 {
		numWorkers = runtime.NumCPU()
	}

	queues := make([]*WorkQueue[T], numWorkers)
	for i := 0; i < numWorkers; i++ {
		queues[i] = &WorkQueue[T]{}
	}

	return &WorkStealingScheduler[T]{
		workers:    numWorkers,
		queues:     queues,
		stealIndex: atomic.Uint32{},
		done:       make(chan struct{}),
	}
}

func (ws *WorkStealingScheduler[T]) Submit(task T) {
	workerID := runtime.GOMAXPROCS(0) % ws.workers
	q := ws.queues[workerID]
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, task)
}

func (ws *WorkStealingScheduler[T]) GetTask(workerID int) (T, bool) {
	var zero T
	q := ws.queues[workerID]
	q.mu.Lock()
	if len(q.items) > 0 {
		task := q.items[0]
		q.items = q.items[1:]
		q.mu.Unlock()
		return task, true
	}
	q.mu.Unlock()

	startIdx := ws.stealIndex.Load()
	for i := 0; i < ws.workers-1; i++ {
		victimID := (int(startIdx) + i + 1) % ws.workers
		victim := ws.queues[victimID]

		victim.mu.Lock()
		if len(victim.items) > 0 {
			task := victim.items[0]
			victim.items = victim.items[1:]
			victim.mu.Unlock()
			ws.stealIndex.Store(uint32(victimID))
			return task, true
		}
		victim.mu.Unlock()
	}

	return zero, false
}

func (ws *WorkStealingScheduler[T]) Start() {
	for i := 0; i < ws.workers; i++ {
		go ws.worker(i)
	}
}

func (ws *WorkStealingScheduler[T]) Stop() {
	close(ws.done)
}

func (ws *WorkStealingScheduler[T]) worker(workerID int) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for {
		select {
		case <-ws.done:
			return
		default:
			task, ok := ws.GetTask(workerID)
			if !ok {
				runtime.Gosched()
				continue
			}

			if fn, ok := any(task).(func()); ok {
				fn()
			} else {
				runtime.Gosched()
			}
		}
	}
}

func (ws *WorkStealingScheduler[T]) Stats() SchedulerStats {
	stats := SchedulerStats{}

	for _, q := range ws.queues {
		q.mu.Lock()
		stats.QueueSizes = append(stats.QueueSizes, len(q.items))
		q.mu.Unlock()
	}

	stats.NumWorkers = ws.workers
	stats.StealIndex = int(ws.stealIndex.Load())

	return stats
}

type SchedulerStats struct {
	QueueSizes []int
	NumWorkers int
	StealIndex int
}
