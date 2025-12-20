package store

import (
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// IndexJobQueue - Non-Blocking Index Job Queue with Overflow Strategy
// =============================================================================

// IndexJobQueueConfig configures the non-blocking index job queue.
type IndexJobQueueConfig struct {
	MainChannelSize    int           // Primary channel buffer size
	OverflowBufferSize int           // Secondary overflow buffer size
	DropOnOverflow     bool          // If true, drop jobs when both buffers full
	DrainInterval      time.Duration // How often to drain overflow to main channel
}

// DefaultIndexJobQueueConfig returns sensible defaults for production.
func DefaultIndexJobQueueConfig() IndexJobQueueConfig {
	return IndexJobQueueConfig{
		MainChannelSize:    20000,
		OverflowBufferSize: 200000,
		DropOnOverflow:     true,
		DrainInterval:      1 * time.Millisecond,
	}
}

// IndexJobQueueStats tracks queue statistics.
type IndexJobQueueStats struct {
	TotalSent     uint64 // Total jobs sent
	DirectSent    uint64 // Jobs sent directly to main channel
	OverflowCount uint64 // Jobs sent to overflow buffer
	DrainedCount  uint64 // Jobs drained from overflow to main
	DroppedCount  uint64 // Jobs dropped when both buffers full
}

// IndexJobQueue provides non-blocking job submission with overflow handling.
type IndexJobQueue struct {
	cfg IndexJobQueueConfig

	// Main channel for consumers
	mainChan chan IndexJob

	// Overflow buffer with mutex protection
	overflowMu sync.Mutex
	overflow   []IndexJob

	// Statistics (atomic)
	totalSent     uint64
	directSent    uint64
	overflowCount uint64
	drainedCount  uint64
	droppedCount  uint64

	// Lifecycle
	stopChan chan struct{}
	stopped  int32
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewIndexJobQueue creates a new non-blocking index job queue.
func NewIndexJobQueue(cfg IndexJobQueueConfig) *IndexJobQueue {
	q := &IndexJobQueue{
		cfg:      cfg,
		mainChan: make(chan IndexJob, cfg.MainChannelSize),
		overflow: make([]IndexJob, 0, cfg.OverflowBufferSize),
		stopChan: make(chan struct{}),
	}

	// Start background drainer
	q.wg.Add(1)
	go q.drainLoop()

	return q
}

// Send submits a job without blocking. Returns true if accepted, false if dropped.
func (q *IndexJobQueue) Send(job IndexJob) bool {
	// Check if stopped
	if atomic.LoadInt32(&q.stopped) == 1 {
		return false
	}

	atomic.AddUint64(&q.totalSent, 1)

	// Try non-blocking send to main channel
	select {
	case q.mainChan <- job:
		atomic.AddUint64(&q.directSent, 1)
		return true
	default:
		// Main channel full, try overflow buffer
		return q.sendToOverflow(job)
	}
}

// SendBatch submits multiple jobs efficiently.
func (q *IndexJobQueue) SendBatch(jobs []IndexJob) int {
	accepted := 0
	for i := range jobs {
		if q.Send(jobs[i]) {
			accepted++
		}
	}
	return accepted
}

// sendToOverflow adds job to overflow buffer or drops it.
func (q *IndexJobQueue) sendToOverflow(job IndexJob) bool {
	q.overflowMu.Lock()
	defer q.overflowMu.Unlock()

	// Check if overflow buffer has space
	if len(q.overflow) >= q.cfg.OverflowBufferSize {
		if q.cfg.DropOnOverflow {
			atomic.AddUint64(&q.droppedCount, 1)
			// We can't easily log here without a logger, but we can increment a metric
			return false
		}
		// If not dropping, force append (may exceed capacity)
	}

	q.overflow = append(q.overflow, job)
	atomic.AddUint64(&q.overflowCount, 1)
	return true
}

// drainLoop continuously drains overflow buffer to main channel.
func (q *IndexJobQueue) drainLoop() {
	defer q.wg.Done()

	ticker := time.NewTicker(q.cfg.DrainInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopChan:
			// Final drain before exit - non-blocking to prevent deadlock
			q.drainRemaining()
			return
		case <-ticker.C:
			q.drainBatch()
		}
	}
}

// drainBatch moves jobs from overflow to main channel (non-blocking).
func (q *IndexJobQueue) drainBatch() {
	q.overflowMu.Lock()
	if len(q.overflow) == 0 {
		q.overflowMu.Unlock()
		return
	}

	// Copy and clear overflow
	batch := make([]IndexJob, len(q.overflow))
	copy(batch, q.overflow)
	q.overflow = q.overflow[:0]
	q.overflowMu.Unlock()

	// Send to main channel - non-blocking
	var remaining []IndexJob
	for _, job := range batch {
		select {
		case q.mainChan <- job:
			atomic.AddUint64(&q.drainedCount, 1)
		default:
			// Main still full, accumulate for re-queue
			remaining = append(remaining, job)
		}
	}

	// Put remaining back
	if len(remaining) > 0 {
		q.overflowMu.Lock()
		q.overflow = append(remaining, q.overflow...)
		q.overflowMu.Unlock()
	}
}

// drainRemaining drains overflow jobs with timeout to prevent deadlock.
func (q *IndexJobQueue) drainRemaining() {
	q.overflowMu.Lock()
	batch := make([]IndexJob, len(q.overflow))
	copy(batch, q.overflow)
	q.overflow = q.overflow[:0]
	q.overflowMu.Unlock()

	// Non-blocking drain - try each job once
	for _, job := range batch {
		select {
		case q.mainChan <- job:
			atomic.AddUint64(&q.drainedCount, 1)
		default:
			// Channel full, job will be lost on shutdown
			// This is acceptable for graceful shutdown
		}
	}
}

// Jobs returns the channel for consumers to read from.
func (q *IndexJobQueue) Jobs() <-chan IndexJob {
	return q.mainChan
}

// Stats returns current queue statistics.
func (q *IndexJobQueue) Stats() IndexJobQueueStats {
	return IndexJobQueueStats{
		TotalSent:     atomic.LoadUint64(&q.totalSent),
		DirectSent:    atomic.LoadUint64(&q.directSent),
		OverflowCount: atomic.LoadUint64(&q.overflowCount),
		DrainedCount:  atomic.LoadUint64(&q.drainedCount),
		DroppedCount:  atomic.LoadUint64(&q.droppedCount),
	}
}

// Stop gracefully stops the queue.
func (q *IndexJobQueue) Stop() {
	q.stopOnce.Do(func() {
		atomic.StoreInt32(&q.stopped, 1)
		close(q.stopChan)
		q.wg.Wait()
		close(q.mainChan)
	})
}

// Len returns approximate queue depth (main + overflow).
func (q *IndexJobQueue) Len() int {
	q.overflowMu.Lock()
	overflowLen := len(q.overflow)
	q.overflowMu.Unlock()
	return len(q.mainChan) + overflowLen
}
