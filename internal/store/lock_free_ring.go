package store

import (
	"runtime"
	"sync/atomic"
	"time"
)

// LockFreeRingBuffer is a generic lock-free Multi-Producer Multi-Consumer (MPMC) queue.
type LockFreeRingBuffer[T any] struct {
	buffer []ringSlotGeneric[T]
	mask   uint64
	head   atomic.Uint64 // Consumer index
	tail   atomic.Uint64 // Producer index
}

type ringSlotGeneric[T any] struct {
	sequence atomic.Uint64
	data     T
}

// NewLockFreeRingBuffer creates a new ring buffer with the specified size.
// Size must be a power of 2.
func NewLockFreeRingBuffer[T any](size uint64) *LockFreeRingBuffer[T] {
	if size < 2 {
		size = 2
	}
	// Round up to power of 2
	if size&(size-1) != 0 {
		power := uint64(1)
		for power < size {
			power <<= 1
		}
		size = power
	}

	rb := &LockFreeRingBuffer[T]{
		buffer: make([]ringSlotGeneric[T], size),
		mask:   size - 1,
	}

	// Initialize sequence numbers
	for i := uint64(0); i < size; i++ {
		rb.buffer[i].sequence.Store(i)
	}

	return rb
}

// Push adds an item to the queue. Returns false if the queue is full.
func (rb *LockFreeRingBuffer[T]) Push(item T) bool {
	var head, tail, seq, nextSeq uint64
	var slot *ringSlotGeneric[T]

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
				slot.data = item
				// Mark as available for consumption (seq = tail + 1)
				slot.sequence.Store(tail + 1)
				return true
			}
		} else if seq < tail {
			runtime.Gosched()
		} else {
			runtime.Gosched()
		}
	}
}

// Pop removes an item from the queue. Returns false if the queue is empty.
func (rb *LockFreeRingBuffer[T]) Pop() (T, bool) {
	var head, tail, seq, nextSeq uint64
	var slot *ringSlotGeneric[T]
	var item T

	for {
		head = rb.head.Load()
		tail = rb.tail.Load()

		// Empty check
		if head == tail {
			return item, false
		}

		slot = &rb.buffer[head&rb.mask]
		seq = slot.sequence.Load()
		nextSeq = head + 1

		// Check if slot has data for this turn
		if seq == nextSeq {
			if rb.head.CompareAndSwap(head, nextSeq) {
				// We claimed the item
				item = slot.data
				// Mark as available for production (one full cycle later)
				slot.sequence.Store(head + uint64(len(rb.buffer)))
				return item, true
			}
		} else if seq < nextSeq {
			runtime.Gosched()
		} else {
			runtime.Gosched()
		}
	}
}

// Len returns approximate length
func (rb *LockFreeRingBuffer[T]) Len() int {
	return int(rb.tail.Load() - rb.head.Load())
}

// PushBlocking adds an item, blocking/yielding if full until space is available or timeout.
func (rb *LockFreeRingBuffer[T]) PushBlocking(item T, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if rb.Push(item) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
}
