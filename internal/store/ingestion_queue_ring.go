package store

import (
	"runtime"
	"sync/atomic"
	"time"
)

// IngestionRingBuffer is a lock-free Multi-Producer Multi-Consumer (MPMC) queue
// optimized for high-throughput ingestion.
type IngestionRingBuffer struct {
	buffer []ringSlot
	mask   uint64
	head   atomic.Uint64 // Consumer index
	tail   atomic.Uint64 // Producer index

	// notify carries a coalesced "there is work" signal to consumers.
	//
	// The capacity is 1 and sends are non-blocking: the channel is a hint, not
	// a counter. A consumer drains the ring until Pop reports empty, so a
	// single pending token is enough - anything more would only queue up
	// wakeups for work that is already gone.
	notify chan struct{}
}

type ringSlot struct {
	sequence atomic.Uint64
	data     IngestionJob
}

// NewIngestionRingBuffer creates a new ring buffer with the specified size.
// Size must be a power of 2.
func NewIngestionRingBuffer(size uint64) *IngestionRingBuffer {
	if size < 2 {
		size = 2
	}
	// Round up to power of 2
	if size&(size-1) != 0 {
		// Not a power of 2, simplistic fix for now: loop until power of 2
		// Ideally use bits.LeadingZeros64 but keeping it simple/portable
		power := uint64(1)
		for power < size {
			power <<= 1
		}
		size = power
	}

	rb := &IngestionRingBuffer{
		buffer: make([]ringSlot, size),
		mask:   size - 1,
		notify: make(chan struct{}, 1),
	}

	// Initialize sequence numbers
	for i := uint64(0); i < size; i++ {
		rb.buffer[i].sequence.Store(i)
	}

	return rb
}

// Notify returns a channel that receives a token whenever a job is published.
//
// Consumers block on it instead of polling with a timer. An idle worker pool
// used to wake every millisecond per goroutine - at 16 workers that is 16,000
// runtime timer operations per second for a queue with nothing in it, and
// profiling attributed 1,498 s (24.93%) of cumulative block delay in
// runtime.selectgo to exactly that churn.
func (rb *IngestionRingBuffer) Notify() <-chan struct{} {
	return rb.notify
}

// signal publishes a wakeup token without blocking.
func (rb *IngestionRingBuffer) signal() {
	if rb.notify == nil {
		return
	}
	select {
	case rb.notify <- struct{}{}:
	default:
	}
}

// Push adds an item to the queue. Returns false if the queue is full.
func (rb *IngestionRingBuffer) Push(job IngestionJob) bool {
	var head, tail, seq, nextSeq uint64
	var slot *ringSlot

	for {
		tail = rb.tail.Load()
		head = rb.head.Load()

		// Full check
		if tail-head >= uint64(len(rb.buffer)) {
			return false
		}

		slot = &rb.buffer[tail&rb.mask]
		seq = slot.sequence.Load()
		nextSeq = tail + 1

		// Check if slot is free for this turn
		if seq == tail {
			if rb.tail.CompareAndSwap(tail, nextSeq) {
				// We claimed the slot
				slot.data = job
				// Mark as available for consumption (seq = tail + 1)
				slot.sequence.Store(tail + 1)
				rb.signal()
				return true
			}
		} else {
			// seq < tail: Slot lags behind
			// seq > tail: Slot already claimed by another producer
			runtime.Gosched()
		}
	}
}

// Pop removes an item from the queue. Returns false if the queue is empty.
func (rb *IngestionRingBuffer) Pop() (IngestionJob, bool) {
	var head, tail, seq, nextSeq uint64
	var slot *ringSlot
	var job IngestionJob

	for {
		head = rb.head.Load()
		tail = rb.tail.Load()

		// Empty check
		if head == tail {
			return job, false
		}

		slot = &rb.buffer[head&rb.mask]
		seq = slot.sequence.Load()
		nextSeq = head + 1

		// Check if slot has data for this turn
		if seq == nextSeq {
			if rb.head.CompareAndSwap(head, nextSeq) {
				// We claimed the item
				job = slot.data
				// Mark as available for production (one full cycle later)
				slot.sequence.Store(head + uint64(len(rb.buffer)))
				return job, true
			}
		} else {
			// seq < nextSeq: Producer hasn't finished writing yet
			// seq > nextSeq: Should not happen normally unless head moved
			runtime.Gosched()
		}
	}
}

// Len returns approximate length
func (rb *IngestionRingBuffer) Len() int {
	return int(rb.tail.Load() - rb.head.Load()) // #nosec G115
}

// Capacity returns the total size of the ring buffer
func (rb *IngestionRingBuffer) Capacity() int {
	return len(rb.buffer)
}

// PushBlocking adds an item, blocking/yielding if full until space is available or timeout.
func (rb *IngestionRingBuffer) PushBlocking(job IngestionJob, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if rb.Push(job) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
		// Maybe sleep slightly to avoid CPU burn if really full?
		// But high throughput expects quick drain.
	}
}
