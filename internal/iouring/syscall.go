//go:build linux

package iouring

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

// IoUring syscall numbers
const (
	SYS_IO_URING_SETUP    = 425
	SYS_IO_URING_ENTER    = 426
	SYS_IO_URING_REGISTER = 427
)

// Setup flags
const (
	IORING_SETUP_IOPOLL     = 1 << 0 // Poll instead of interrupt
	IORING_SETUP_SQPOLL     = 1 << 1 // Kernel polls submission queue
	IORING_SETUP_SQ_AFF     = 1 << 2 // Bind poll thread to CPU
	IORING_SETUP_CQSIZE     = 1 << 3 // Custom CQ size
	IORING_SETUP_CLAMP      = 1 << 4 // Clamp CQ ring size
	IORING_SETUP_ATTACH_WQ  = 1 << 5 // Attach to existing WQ
	IORING_SETUP_R_DISABLED = 1 << 6 // Start disabled
)

// Register opcodes
const (
	IORING_REGISTER_BUFFERS     = 0
	IORING_UNREGISTER_BUFFERS   = 1
	IORING_REGISTER_FILES       = 2
	IORING_UNREGISTER_FILES     = 3
	IORING_REGISTER_EVENTFD     = 4
	IORING_UNREGISTER_EVENTFD   = 5
	IORING_REGISTER_PROBE       = 8
	IORING_REGISTER_PERSONALITY = 9
)

// Enter flags
const (
	IORING_ENTER_GETEVENTS = 1 << 0
	IORING_ENTER_SQ_WAKEUP = 1 << 1
)

// Feature flags
const (
	IORING_FEAT_SINGLE_MMAP   = 1 << 0
	IORING_FEAT_NODROP        = 1 << 1
	IORING_FEAT_SUBMIT_STABLE = 1 << 2
)

// SQE flags
const (
	IOSQE_FIXED_FILE    = 1 << 0 // Use registered file
	IOSQE_IO_DRAIN      = 1 << 1 // Drain previous IO
	IOSQE_IO_LINK       = 1 << 2 // Link with next SQE
	IOSQE_IO_HARDLINK   = 1 << 3 // Hard link (must succeed)
	IOSQE_ASYNC         = 1 << 4 // Force async execution
	IOSQE_BUFFER_SELECT = 1 << 5 // Select buffer from pool
)

// IO operations
const (
	IORING_OP_NOP             = 0
	IORING_OP_READV           = 1
	IORING_OP_WRITEV          = 2
	IORING_OP_FSYNC           = 3
	IORING_OP_READ_FIXED      = 4
	IORING_OP_WRITE_FIXED     = 5
	IORING_OP_POLL_ADD        = 6
	IORING_OP_POLL_REMOVE     = 7
	IORING_OP_SYNC_FILE_RANGE = 8
	IORING_OP_READ            = 22
	IORING_OP_WRITE           = 23
	IORING_OP_FADVISE         = 24
	IORING_OP_MADVISE         = 25
	IORING_OP_FALLOCATE       = 26
	IORING_OP_OPENAT          = 27
	IORING_OP_CLOSE           = 28
	IORING_OP_STATX           = 29
)

// Fsync flags
const (
	IORING_FSYNC_DATASYNC = 1 << 0
)

// Common errors
var (
	ErrRingFull     = errors.New("io_uring submission queue is full")
	ErrRingEmpty    = errors.New("io_uring completion queue is empty")
	ErrInvalidParam = errors.New("invalid io_uring parameter")
)

// ioUringSetup creates a new io_uring instance
func ioUringSetup(entries uint32, params *Params) (int, error) {
	fd, _, errno := unix.Syscall(
		SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(params)),
		0,
	)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

// ioUringEnter submits operations and/or waits for completions
func ioUringEnter(fd int, toSubmit uint32, minComplete uint32, flags uint32, sig unsafe.Pointer) (int, error) {
	n, _, errno := unix.Syscall6(
		SYS_IO_URING_ENTER,
		uintptr(fd),
		uintptr(toSubmit),
		uintptr(minComplete),
		uintptr(flags),
		uintptr(sig),
		0,
	)
	if errno != 0 {
		return -1, errno
	}
	return int(n), nil
}

// ioUringRegister registers files or buffers with the ring
func ioUringRegister(fd int, opcode uint32, arg unsafe.Pointer, nrArgs uint32) (int, error) {
	n, _, errno := unix.Syscall6(
		SYS_IO_URING_REGISTER,
		uintptr(fd),
		uintptr(opcode),
		uintptr(arg),
		uintptr(nrArgs),
		0,
		0,
	)
	if errno != 0 {
		return -1, errno
	}
	return int(n), nil
}

// mmapSize calculates the size needed for mmap based on ring parameters
func mmapSize(params *Params, sqRing bool) (int, error) {
	var size int

	if sqRing {
		// Submission queue ring size
		sqRingSize := params.SqOffsets.Array + params.SqEntries*uint32(unsafe.Sizeof(uint32(0)))
		sqeSize := params.SqEntries * uint32(unsafe.Sizeof(SQE{}))
		size = int(sqRingSize + sqeSize)
	} else {
		// Completion queue ring size
		size = int(params.CqOffsets.Cqes + params.CqEntries*uint32(unsafe.Sizeof(uint32(0))))
	}

	// Round up to page size
	pageSize := unix.Getpagesize()
	size = (size + pageSize - 1) &^ (pageSize - 1)

	return size, nil
}
