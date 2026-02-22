//go:build linux

package iouring

import (
	"unsafe"
)

// Params represents io_uring_params for setup
type Params struct {
	SqEntries    uint32
	CqEntries    uint32
	Flags        uint32
	SqThreadCpu  uint32
	SqThreadIdle uint32
	Features     uint32
	WqFd         uint32
	Resv         [3]uint32
	SqOffsets    SqRingOffsets
	CqOffsets    CqRingOffsets
}

// SqRingOffsets represents the submission queue ring offsets
type SqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Resv1       uint32
	Resv2       uint32
}

// CqRingOffsets represents the completion queue ring offsets
type CqRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	Cqes        uint32
	Flags       uint32
	Resv1       uint32
	Resv2       uint32
}

// SQE represents a submission queue entry (64 bytes)
type SQE struct {
	Opcode      uint8
	Flags       uint8
	Ioprio      uint16
	Fd          int32
	Off         uint64
	Addr        uint64
	Len         uint32
	OpcodeFlags uint32 // Union: rw_flags, fsync_flags, etc.
	UserData    uint64
	BufIndex    uint16
	Personality uint16
	SpliceFdIn  int32
	SpliceOffIn uint64
	SpliceLen   uint32
	SpliceFlags uint32
	_padding    [4]byte
}

// CQE represents a completion queue entry (16 bytes)
type CQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

// Probe represents io_uring_probe for capability detection
type Probe struct {
	LastOp uint8
	OpsLen uint8
	Resv   uint16
	Resv2  [3]uint32
	// Followed by variable number of ProbeOp entries
}

// ProbeOp represents a single operation probe entry
type ProbeOp struct {
	Op    uint8
	Resv  uint8
	Flags uint16
	Resv2 uint32
}

// FilesUpdate represents file set update for registered files
type FilesUpdate struct {
	Offset uint32
	Resv   uint32
	Fds    unsafe.Pointer
}

// Sigset_t represents a signal set for io_uring_enter
type Sigset_t struct {
	Val [16]uint64
}

// IOVec represents an I/O vector for vectored operations
type IOVec struct {
	Base unsafe.Pointer
	Len  uint64
}

// Size constants
const (
	SQESize = uint32(unsafe.Sizeof(SQE{}))
	CQESize = uint32(unsafe.Sizeof(CQE{}))
)
