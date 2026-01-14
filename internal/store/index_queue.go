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
		MainChannelSize:    10000,
		OverflowBufferSize: 50000,
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
	sendMu   sync.RWMutex // Protects mainChan close vs send

	// Memory Pressure
	estimatedBytes int64 // Atomic
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
	// Protect against send on closed channel
	q.sendMu.RLock()
	defer q.sendMu.RUnlock()

	// Check if stopped (must be checked under lock or atomic is fine,
	// but lock ensures we don't race with close)
	if atomic.LoadInt32(&q.stopped) == 1 {
		return false
	}

	size := int64(0)
	if job.Record != nil {
		// Approximate size: rows * cols * 8 (assuming 64-bit types/pointers mixed)
		size = int64(job.Record.NumRows() * int64(job.Record.NumCols()) * 8)
	}

	atomic.AddUint64(&q.totalSent, 1)

	// Try non-blocking send to main channel
	select {
	case q.mainChan <- job:
		atomic.AddUint64(&q.directSent, 1)
		atomic.AddInt64(&q.estimatedBytes, size)
		return true
	default:
		// Main channel full, try overflow buffer
		if q.sendToOverflow(job) {
			atomic.AddInt64(&q.estimatedBytes, size)
			return true
		}
		return false
	}
}

// Block submits a job, blocking until space is available or queue stopped.
// This is more efficient than spin-waiting for backpressure.
func (q *IndexJobQueue) Block(job IndexJob) bool {
	// 1. Try non-blocking existing path (fills overflow if available)
	if q.Send(job) {
		return true
	}

	// 2. Both main and overflow are full. Block on mainChan.
	// We do not hold lock here to allow Stop() to proceed.
	// Stop() will close stopChan, which unblocks us.

	size := int64(0)
	if job.Record != nil {
		size = int64(job.Record.NumRows() * int64(job.Record.NumCols()) * 8)
	}

	select {
	case q.mainChan <- job:
		// Succeeded after blocking
		atomic.AddUint64(&q.directSent, 1)
		atomic.AddInt64(&q.estimatedBytes, size)
		return true
	case <-q.stopChan:
		// Stopped while waiting
		atomic.AddUint64(&q.droppedCount, 1)
		return false
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
	// All overflowed jobs were moved to main or dropped (wait, drainBatch doesn't drop)
	// Wait, if we moved to main, estimatedBytes doesn't change (still in system)
	// If we dropped, we should decrease. But drainBatch logic here moves chunks.
	// No dropping here.
}

// drainRemaining drains overflow jobs with timeout to prevent deadlock.
func (q *IndexJobQueue) drainRemaining() {
	q.overflowMu.Lock()
	batch := make([]IndexJob, len(q.overflow))
	copy(batch, q.overflow)
	q.overflow = q.overflow[:0]
	q.overflowMu.Unlock()

	// Try to send each job with a short timeout to allow consumers to clear capacity
	// This helps avoid dropping data during graceful shutdown if consumers are just slightly behind.
	// This helps avoid dropping data during graceful shutdown if consumers are just slightly behind.
	timeout := 5 * time.Millisecond

	for _, job := range batch {
		select {
		case q.mainChan <- job:
			atomic.AddUint64(&q.drainedCount, 1)
		case <-time.After(timeout):
			// Timed out, force drop
			// This is acceptable for graceful shutdown to ensure we don't hang forever
			atomic.AddUint64(&q.droppedCount, 1)
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

// IsStopped returns true if the queue is stopped.
func (q *IndexJobQueue) IsStopped() bool {
	return atomic.LoadInt32(&q.stopped) == 1
}

// Stop gracefully stops the queue.
func (q *IndexJobQueue) Stop() {
	q.stopOnce.Do(func() {
		// Take write lock to prohibit further sends
		q.sendMu.Lock()
		atomic.StoreInt32(&q.stopped, 1)
		// Signal drainLoop to stop
		close(q.stopChan)
		q.sendMu.Unlock()

		// Wait for drainLoop to finish (which calls drainRemaining)
		q.wg.Wait()

		// NOW close mainChan, after all sends (including drainRemaining) are done
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

// EstimatedBytes returns approximate memory usage of queued jobs in bytes.
func (q *IndexJobQueue) EstimatedBytes() int64 {
	return atomic.LoadInt64(&q.estimatedBytes)
}

// DecreaseEstimatedBytes decreases the estimated bytes (called by consumer).
func (q *IndexJobQueue) DecreaseEstimatedBytes(amount int64) {
	atomic.AddInt64(&q.estimatedBytes, -amount)
}
