package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"sync"
	"sync/atomic"
	"time"
)

// IndexJobQueueLockFree is a lock-free queue for managing asynchronous index jobs.
type IndexJobQueueLockFree struct {
	cfg types.IndexJobQueueConfig

	buffer *LockFreeRingBuffer[types.IndexJob]

	totalSent     uint64
	acceptedCount uint64
	droppedCount  uint64

	stopChan chan struct{}
	stopped  int32
	stopOnce sync.Once

	estimatedBytes int64
	notify         chan struct{}
}

// NewIndexJobQueueLockFree creates a new lock-free index job queue.
func NewIndexJobQueueLockFree(cfg types.IndexJobQueueConfig) *IndexJobQueueLockFree {
	bufferSize := cfg.MainChannelSize + cfg.OverflowBufferSize
	q := &IndexJobQueueLockFree{
		cfg:      cfg,
		buffer:   NewLockFreeRingBuffer[types.IndexJob](uint64(bufferSize)), // #nosec G115
		stopChan: make(chan struct{}),
		notify:   make(chan struct{}, 1),
	}

	return q
}

// Send attempts to add a job to the queue without blocking.
func (q *IndexJobQueueLockFree) Send(job types.IndexJob) bool {
	if atomic.LoadInt32(&q.stopped) == 1 {
		return false
	}

	size := int64(0)
	if job.Record != nil {
		size = int64(job.Record.NumRows() * int64(job.Record.NumCols()) * 8)
	}

	atomic.AddUint64(&q.totalSent, 1)

	if q.buffer.Push(job) {
		atomic.AddUint64(&q.acceptedCount, 1)
		atomic.AddInt64(&q.estimatedBytes, size)
		
		// Non-blocking signal
		select {
		case q.notify <- struct{}{}:
		default:
		}
		return true
	}

	if q.cfg.DropOnOverflow {
		atomic.AddUint64(&q.droppedCount, 1)
	}

	return false
}

// Pop retrieves a job from the queue.
func (q *IndexJobQueueLockFree) Pop() (types.IndexJob, bool) {
	job, ok := q.buffer.Pop()
	if ok {
		size := int64(0)
		if job.Record != nil {
			size = int64(job.Record.NumRows() * int64(job.Record.NumCols()) * 8)
		}
		atomic.AddInt64(&q.estimatedBytes, -size)
	}
	return job, ok
}

// Block attempts to add a job to the queue, blocking until space is available or timeout.
func (q *IndexJobQueueLockFree) Block(job types.IndexJob, timeout time.Duration) bool {
	if atomic.LoadInt32(&q.stopped) == 1 {
		return false
	}

	size := int64(0)
	if job.Record != nil {
		size = int64(job.Record.NumRows() * int64(job.Record.NumCols()) * 8)
	}

	if q.buffer.PushBlocking(job, timeout) {
		atomic.AddUint64(&q.acceptedCount, 1)
		atomic.AddInt64(&q.estimatedBytes, size)

		// Non-blocking signal
		select {
		case q.notify <- struct{}{}:
		default:
		}
		return true
	}

	atomic.AddUint64(&q.droppedCount, 1)
	return false
}

// SendBatch attempts to add multiple jobs to the queue.
func (q *IndexJobQueueLockFree) SendBatch(jobs []types.IndexJob) int {
	accepted := 0
	for i := range jobs {
		if q.Send(jobs[i]) {
			accepted++
		}
	}
	return accepted
}

// Stats returns the current statistics for the queue.
func (q *IndexJobQueueLockFree) Stats() types.IndexJobQueueStats {
	return types.IndexJobQueueStats{
		TotalSent:     atomic.LoadUint64(&q.totalSent),
		DirectSent:    atomic.LoadUint64(&q.acceptedCount),
		OverflowCount: 0,
		DrainedCount:  0,
		DroppedCount:  atomic.LoadUint64(&q.droppedCount),
	}
}

// IsStopped returns true if the queue has been stopped.
func (q *IndexJobQueueLockFree) IsStopped() bool {
	return atomic.LoadInt32(&q.stopped) == 1
}

// Stop shuts down the queue.
func (q *IndexJobQueueLockFree) Stop() {
	q.stopOnce.Do(func() {
		atomic.StoreInt32(&q.stopped, 1)
		close(q.stopChan)
	})
}

// Len returns the current number of jobs in the queue.
func (q *IndexJobQueueLockFree) Len() int {
	return q.buffer.Len()
}

// EstimatedBytes returns the estimated memory usage of the jobs in the queue.
func (q *IndexJobQueueLockFree) EstimatedBytes() int64 {
	return atomic.LoadInt64(&q.estimatedBytes)
}

// DecreaseEstimatedBytes is a no-op for the lock-free implementation.
func (q *IndexJobQueueLockFree) DecreaseEstimatedBytes(amount int64) {
}

// Notify returns a channel that is signaled when a new job is added.
func (q *IndexJobQueueLockFree) Notify() <-chan struct{} {
	return q.notify
}
