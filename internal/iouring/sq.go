//go:build linux

package iouring

import (
	"sync/atomic"
	"time"
	"unsafe"
)

// Submit prepares and submits an operation to the ring
// This is lock-free and can be called from a single producer goroutine
func (r *Ring) Submit(sqe *SQE) error {
	// Get current tail and calculate next position
	tail := atomic.LoadUint32(r.sqTail)
	head := atomic.LoadUint32(r.sqHead)

	// Check for SQ overflow
	if tail-head >= r.sqEntriesCached {
		return ErrRingFull
	}

	// Calculate index in SQ ring
	index := tail & r.sqRingMaskCached

	// Copy SQE to ring
	r.sqes[index] = *sqe

	// Write barrier: ensure SQE is written before updating tail
	// sqArray is the index array that points to SQE slots
	sqArray := unsafe.Slice(r.sqArray, r.sqEntriesCached)
	atomic.StoreUint32(&sqArray[index], index)

	// Update tail (release barrier) - this makes the entry visible to kernel
	atomic.StoreUint32(r.sqTail, tail+1)

	// Metrics callback
	if r.onSubmit != nil {
		r.onSubmit(sqe.Opcode)
	}

	return nil
}

// SubmitVectored prepares a vectored write operation (writev)
func (r *Ring) SubmitVectored(fd int, iovs []IOVec, offset uint64, userData uint64) error {
	if len(iovs) == 0 {
		return ErrInvalidParam
	}
	if len(iovs) > 1024 {
		return ErrInvalidParam
	}

	sqe := &SQE{
		Opcode:   IORING_OP_WRITEV,
		Fd:       int32(fd),
		Off:      offset,
		Addr:     uint64(uintptr(unsafe.Pointer(&iovs[0]))),
		Len:      uint32(len(iovs)),
		UserData: userData,
	}

	return r.Submit(sqe)
}

// SubmitWrite prepares a write operation
func (r *Ring) SubmitWrite(fd int, buf []byte, offset uint64, userData uint64) error {
	if len(buf) == 0 {
		return ErrInvalidParam
	}

	sqe := &SQE{
		Opcode:   IORING_OP_WRITE,
		Fd:       int32(fd),
		Off:      offset,
		Addr:     uint64(uintptr(unsafe.Pointer(&buf[0]))),
		Len:      uint32(len(buf)),
		UserData: userData,
	}

	return r.Submit(sqe)
}

// SubmitRead prepares a read operation
func (r *Ring) SubmitRead(fd int, buf []byte, offset uint64, userData uint64) error {
	if len(buf) == 0 {
		return ErrInvalidParam
	}

	sqe := &SQE{
		Opcode:   IORING_OP_READ,
		Fd:       int32(fd),
		Off:      offset,
		Addr:     uint64(uintptr(unsafe.Pointer(&buf[0]))),
		Len:      uint32(len(buf)),
		UserData: userData,
	}

	return r.Submit(sqe)
}

// SubmitFsync prepares an fsync operation
func (r *Ring) SubmitFsync(fd int, datasync bool, userData uint64) error {
	var flags uint32
	if datasync {
		flags = IORING_FSYNC_DATASYNC
	}

	sqe := &SQE{
		Opcode:      IORING_OP_FSYNC,
		Fd:          int32(fd),
		OpcodeFlags: flags,
		UserData:    userData,
	}

	return r.Submit(sqe)
}

// Flush submits pending operations to the kernel
// Returns the number of operations submitted
func (r *Ring) Flush() (int, error) {
	return r.FlushAndWait(0, 0)
}

// FlushAndWait submits operations and optionally waits for completions
// minComplete: minimum number of completions to wait for (0 = don't wait)
// timeout: maximum time to wait (0 = no timeout)
func (r *Ring) FlushAndWait(minComplete uint32, timeout time.Duration) (int, error) {
	var flags uint32
	if minComplete > 0 {
		flags |= IORING_ENTER_GETEVENTS
	}

	submitted := uint32(0)

	// Loop until all pending SQEs are submitted
	for {
		tail := atomic.LoadUint32(r.sqTail)
		head := atomic.LoadUint32(r.sqHead)
		toSubmit := tail - head

		if toSubmit == 0 {
			break
		}

		n, err := ioUringEnter(r.fd, toSubmit, minComplete, flags, nil)
		if err != nil {
			return int(submitted), err
		}

		submitted += uint32(n)

		if uint32(n) == toSubmit {
			break
		}

		// Partial submission, retry with remaining
		time.Sleep(time.Microsecond)
	}

	return int(submitted), nil
}

// SqSpaceLeft returns the number of available slots in the submission queue
func (r *Ring) SqSpaceLeft() uint32 {
	tail := atomic.LoadUint32(r.sqTail)
	head := atomic.LoadUint32(r.sqHead)
	return r.sqEntriesCached - (tail - head)
}

// SqReady returns the number of pending entries in the submission queue
func (r *Ring) SqReady() uint32 {
	tail := atomic.LoadUint32(r.sqTail)
	head := atomic.LoadUint32(r.sqHead)
	return tail - head
}
