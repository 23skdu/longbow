//go:build linux

package iouring

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	// SYS_IO_URING_SETUP is a sys_io_uring_setup constant.
	SYS_IO_URING_SETUP = 425
	// SYS_IO_URING_ENTER is a sys_io_uring_enter constant.
	SYS_IO_URING_ENTER = 426
	// SYS_IO_URING_REGISTER is a sys_io_uring_register constant.
	SYS_IO_URING_REGISTER = 427
)

const (
	// IORING_SETUP_IOPOLL is a ioring_setup_iopoll constant.
	IORING_SETUP_IOPOLL = 1 << 0
	// IORING_SETUP_SQPOLL is a ioring_setup_sqpoll constant.
	IORING_SETUP_SQPOLL = 1 << 1
	// IORING_SETUP_SQ_AFF is a ioring_setup_sq_aff constant.
	IORING_SETUP_SQ_AFF = 1 << 2
	// IORING_SETUP_CQSIZE is a ioring_setup_cqsize constant.
	IORING_SETUP_CQSIZE = 1 << 3
	// IORING_SETUP_CLAMP is a ioring_setup_clamp constant.
	IORING_SETUP_CLAMP = 1 << 4
	// IORING_SETUP_ATTACH_WQ is a ioring_setup_attach_wq constant.
	IORING_SETUP_ATTACH_WQ = 1 << 5
	// IORING_SETUP_R_DISABLED is a ioring_setup_r_disabled constant.
	IORING_SETUP_R_DISABLED = 1 << 6
)

const (
	// IORING_REGISTER_BUFFERS is a ioring_register_buffers constant.
	IORING_REGISTER_BUFFERS = 0
	// IORING_UNREGISTER_BUFFERS is a ioring_unregister_buffers constant.
	IORING_UNREGISTER_BUFFERS = 1
	// IORING_REGISTER_FILES is a ioring_register_files constant.
	IORING_REGISTER_FILES = 2
	// IORING_UNREGISTER_FILES is a ioring_unregister_files constant.
	IORING_UNREGISTER_FILES = 3
	// IORING_REGISTER_EVENTFD is a ioring_register_eventfd constant.
	IORING_REGISTER_EVENTFD = 4
	// IORING_UNREGISTER_EVENTFD is a ioring_unregister_eventfd constant.
	IORING_UNREGISTER_EVENTFD = 5
	// IORING_REGISTER_PROBE is a ioring_register_probe constant.
	IORING_REGISTER_PROBE = 8
	// IORING_REGISTER_PERSONALITY is a ioring_register_personality constant.
	IORING_REGISTER_PERSONALITY = 9
)

const (
	// IORING_ENTER_GETEVENTS is a ioring_enter_getevents constant.
	IORING_ENTER_GETEVENTS = 1 << 0
	// IORING_ENTER_SQ_WAKEUP is a ioring_enter_sq_wakeup constant.
	IORING_ENTER_SQ_WAKEUP = 1 << 1
)

const (
	// IORING_FEAT_SINGLE_MMAP is a ioring_feat_single_mmap constant.
	IORING_FEAT_SINGLE_MMAP = 1 << 0
	// IORING_FEAT_NODROP is a ioring_feat_nodrop constant.
	IORING_FEAT_NODROP = 1 << 1
	// IORING_FEAT_SUBMIT_STABLE is a ioring_feat_submit_stable constant.
	IORING_FEAT_SUBMIT_STABLE = 1 << 2
)

const (
	// IOSQE_FIXED_FILE is a iosqe_fixed_file constant.
	IOSQE_FIXED_FILE = 1 << 0
	// IOSQE_IO_DRAIN is a iosqe_io_drain constant.
	IOSQE_IO_DRAIN = 1 << 1
	// IOSQE_IO_LINK is a iosqe_io_link constant.
	IOSQE_IO_LINK = 1 << 2
	// IOSQE_IO_HARDLINK is a iosqe_io_hardlink constant.
	IOSQE_IO_HARDLINK = 1 << 3
	// IOSQE_ASYNC is a iosqe_async constant.
	IOSQE_ASYNC = 1 << 4
	// IOSQE_BUFFER_SELECT is a iosqe_buffer_select constant.
	IOSQE_BUFFER_SELECT = 1 << 5
)

const (
	// IORING_OP_NOP is a ioring_op_nop constant.
	IORING_OP_NOP = 0
	// IORING_OP_READV is a ioring_op_readv constant.
	IORING_OP_READV = 1
	// IORING_OP_WRITEV is a ioring_op_writev constant.
	IORING_OP_WRITEV = 2
	// IORING_OP_FSYNC is a ioring_op_fsync constant.
	IORING_OP_FSYNC = 3
	// IORING_OP_READ_FIXED is a ioring_op_read_fixed constant.
	IORING_OP_READ_FIXED = 4
	// IORING_OP_WRITE_FIXED is a ioring_op_write_fixed constant.
	IORING_OP_WRITE_FIXED = 5
	// IORING_OP_POLL_ADD is a ioring_op_poll_add constant.
	IORING_OP_POLL_ADD = 6
	// IORING_OP_POLL_REMOVE is a ioring_op_poll_remove constant.
	IORING_OP_POLL_REMOVE = 7
	// IORING_OP_SYNC_FILE_RANGE is a ioring_op_sync_file_range constant.
	IORING_OP_SYNC_FILE_RANGE = 8
	// IORING_OP_READ is a ioring_op_read constant.
	IORING_OP_READ = 22
	// IORING_OP_WRITE is a ioring_op_write constant.
	IORING_OP_WRITE = 23
	// IORING_OP_FADVISE is a ioring_op_fadvise constant.
	IORING_OP_FADVISE = 24
	// IORING_OP_MADVISE is a ioring_op_madvise constant.
	IORING_OP_MADVISE = 25
	// IORING_OP_FALLOCATE is a ioring_op_fallocate constant.
	IORING_OP_FALLOCATE = 26
	// IORING_OP_OPENAT is a ioring_op_openat constant.
	IORING_OP_OPENAT = 27
	// IORING_OP_CLOSE is a ioring_op_close constant.
	IORING_OP_CLOSE = 28
	// IORING_OP_STATX is a ioring_op_statx constant.
	IORING_OP_STATX = 29
)

const (
	// IORING_FSYNC_DATASYNC is a ioring_fsync_datasync constant.
	IORING_FSYNC_DATASYNC = 1 << 0
)

var (
	// ErrRingFull is a err ring full value.
	ErrRingFull = errors.New("io_uring submission queue is full")
	// ErrRingEmpty is a err ring empty value.
	ErrRingEmpty = errors.New("io_uring completion queue is empty")
	// ErrInvalidParam is a err invalid param value.
	ErrInvalidParam = errors.New("invalid io_uring parameter")
)

func ioUringSetup(entries uint32, params *Params) (int, error) {
	fd, _, errno := unix.Syscall(
		SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(params)), // #nosec G103
		0,
	)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

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
