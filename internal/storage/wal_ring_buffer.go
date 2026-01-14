package storage

import (
	"sync/atomic"
)

// WALRingBuffer is a lock-free single-producer single-consumer ring buffer
// optimized for batching WAL entries with minimal contention.
type WALRingBuffer struct {
	buffer   []WALEntry
	capacity int
	mask     int // capacity - 1, for fast modulo using bitwise AND

	// Atomic counters for lock-free operation
	// writePos is where the next entry will be written
	// readPos is where the next entry will be read from
	writePos atomic.Uint64
	readPos  atomic.Uint64
}

// NewWALRingBuffer creates a new ring buffer with the given capacity.
// Capacity must be a power of 2 for efficient modulo operations.
func NewWALRingBuffer(capacity int) *WALRingBuffer {
	// Round up to next power of 2
	if capacity <= 0 {
		capacity = 128
	}
	actualCap := nextPowerOf2(capacity)

	return &WALRingBuffer{
		buffer:   make([]WALEntry, actualCap),
		capacity: actualCap,
		mask:     actualCap - 1,
	}
}

// Push adds an entry to the ring buffer.
// Returns false if the buffer is full.
func (rb *WALRingBuffer) Push(entry WALEntry) bool {
	writePos := rb.writePos.Load()
	readPos := rb.readPos.Load()

	// Check if buffer is full
	if writePos-readPos >= uint64(rb.capacity) {
		return false
	}

	// Write entry at current position
	idx := int(writePos) & rb.mask
	rb.buffer[idx] = entry

	// Advance write position
	rb.writePos.Store(writePos + 1)
	return true
}

// Drain removes all entries from the ring buffer and appends them to the provided slice.
// Returns the number of entries drained.
func (rb *WALRingBuffer) Drain(dest *[]WALEntry) int {
	readPos := rb.readPos.Load()
	writePos := rb.writePos.Load()

	count := int(writePos - readPos)
	if count == 0 {
		return 0
	}

	// Copy entries to destination
	for i := 0; i < count; i++ {
		idx := int(readPos+uint64(i)) & rb.mask
		*dest = append(*dest, rb.buffer[idx])
	}

	// Advance read position
	rb.readPos.Store(writePos)
	return count
}

// Len returns the current number of entries in the buffer.
func (rb *WALRingBuffer) Len() int {
	writePos := rb.writePos.Load()
	readPos := rb.readPos.Load()
	return int(writePos - readPos)
}

// Cap returns the capacity of the ring buffer.
func (rb *WALRingBuffer) Cap() int {
	return rb.capacity
}

// nextPowerOf2 returns the next power of 2 greater than or equal to n
func nextPowerOf2(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
