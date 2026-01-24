package store

import (
	"sync"
	"sync/atomic"
	"time"
)

type IndexJobQueueLockFree struct {
	cfg IndexJobQueueConfig

	buffer *LockFreeRingBuffer[IndexJob]

	totalSent     uint64
	acceptedCount uint64
	droppedCount  uint64

	stopChan chan struct{}
	stopped  int32
	stopOnce sync.Once
	wg       sync.WaitGroup

	estimatedBytes int64
}

func NewIndexJobQueueLockFree(cfg IndexJobQueueConfig) *IndexJobQueueLockFree {
	bufferSize := cfg.MainChannelSize + cfg.OverflowBufferSize
	q := &IndexJobQueueLockFree{
		cfg:      cfg,
		buffer:   NewLockFreeRingBuffer[IndexJob](uint64(bufferSize)),
		stopChan: make(chan struct{}),
	}

	return q
}

func (q *IndexJobQueueLockFree) Send(job IndexJob) bool {
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
		return true
	}

	if q.cfg.DropOnOverflow {
		atomic.AddUint64(&q.droppedCount, 1)
	}

	return false
}

func (q *IndexJobQueueLockFree) Pop() (IndexJob, bool) {
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

func (q *IndexJobQueueLockFree) Block(job IndexJob, timeout time.Duration) bool {
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
		return true
	}

	atomic.AddUint64(&q.droppedCount, 1)
	return false
}

func (q *IndexJobQueueLockFree) SendBatch(jobs []IndexJob) int {
	accepted := 0
	for i := range jobs {
		if q.Send(jobs[i]) {
			accepted++
		}
	}
	return accepted
}

func (q *IndexJobQueueLockFree) Stats() IndexJobQueueStats {
	return IndexJobQueueStats{
		TotalSent:     atomic.LoadUint64(&q.totalSent),
		DirectSent:    atomic.LoadUint64(&q.acceptedCount),
		OverflowCount: 0,
		DrainedCount:  0,
		DroppedCount:  atomic.LoadUint64(&q.droppedCount),
	}
}

func (q *IndexJobQueueLockFree) IsStopped() bool {
	return atomic.LoadInt32(&q.stopped) == 1
}

func (q *IndexJobQueueLockFree) Stop() {
	q.stopOnce.Do(func() {
		atomic.StoreInt32(&q.stopped, 1)
		close(q.stopChan)
	})
}

func (q *IndexJobQueueLockFree) Len() int {
	return q.buffer.Len()
}

func (q *IndexJobQueueLockFree) EstimatedBytes() int64 {
	return atomic.LoadInt64(&q.estimatedBytes)
}

func (q *IndexJobQueueLockFree) DecreaseEstimatedBytes(amount int64) {
}
