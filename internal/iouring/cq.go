//go:build linux

package iouring

import (
	"sync/atomic"
	"unsafe"
)

// Peek retrieves the next completion without removing it
// Returns nil if no completions are available or if completion is invalid
func (r *Ring) Peek() *CQE {
	tail := atomic.LoadUint32(r.cqTail)
	head := atomic.LoadUint32(r.cqHead)

	if head == tail {
		return nil
	}

	// Calculate index in CQ ring
	index := head & r.cqRingMaskCached

	// Get pointer to CQE
	cqes := unsafe.Slice(r.cqes, r.cqEntriesCached)
	cqe := &cqes[index]

	// Validate CQE - ignore spurious/invalid completions
	// UserData=0 with Res=0 is often a spurious completion
	if cqe.UserData == 0 && cqe.Res == 0 {
		// This looks like a spurious completion, advance and try again
		r.Advance(1)
		return r.Peek()
	}

	return cqe
}

// PeekBatch retrieves multiple completions without removing them
// Returns the number of completions available (up to maxCount)
func (r *Ring) PeekBatch(cqes []*CQE, maxCount int) int {
	tail := atomic.LoadUint32(r.cqTail)
	head := atomic.LoadUint32(r.cqHead)

	available := int(tail - head)
	if available == 0 {
		return 0
	}

	if available > maxCount {
		available = maxCount
	}

	// Get pointer to CQ array
	cqArray := unsafe.Slice(r.cqes, r.cqEntriesCached)

	for i := 0; i < available; i++ {
		index := (head + uint32(i)) & r.cqRingMaskCached
		cqes[i] = &cqArray[index]
	}

	return available
}

// Advance marks completions as consumed
// Call this after processing completions from Peek/PeekBatch
func (r *Ring) Advance(count uint32) {
	if count == 0 {
		return
	}

	// Update head atomically (release barrier)
	atomic.AddUint32(r.cqHead, count)
}

// Wait waits for at least one completion
// Blocks until a completion is available or timeout
func (r *Ring) Wait() (*CQE, error) {
	// First, try to get an existing completion
	if cqe := r.Peek(); cqe != nil {
		return cqe, nil
	}

	// Enter the ring to wait for completions
	_, err := r.FlushAndWait(1, 0)
	if err != nil {
		return nil, err
	}

	// Now there should be a completion
	return r.Peek(), nil
}

// CqReady returns the number of unread completions
func (r *Ring) CqReady() uint32 {
	tail := atomic.LoadUint32(r.cqTail)
	head := atomic.LoadUint32(r.cqHead)
	return tail - head
}

// CqEventFdEnabled returns true if eventfd notification is enabled
func (r *Ring) CqEventFdEnabled() bool {
	// Check if IORING_SETUP_SQPOLL is set
	return (r.params.Flags & IORING_SETUP_SQPOLL) != 0
}

// NeedsEnter returns true if we need to call io_uring_enter
// to get more completions
func (r *Ring) NeedsEnter() bool {
	return r.CqReady() == 0
}
