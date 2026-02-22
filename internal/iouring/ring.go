//go:build linux

package iouring

import (
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Ring represents an io_uring instance
type Ring struct {
	fd     int
	params Params

	// Memory-mapped regions
	sqRingArea []byte
	cqRingArea []byte
	sqesArea   []byte

	// SQ ring pointers
	sqHead        *uint32
	sqTail        *uint32
	sqRingMask    *uint32
	sqRingEntries *uint32
	sqFlags       *uint32
	sqDropped     *uint32
	sqArray       *uint32

	// CQ ring pointers
	cqHead        *uint32
	cqTail        *uint32
	cqRingMask    *uint32
	cqRingEntries *uint32
	cqOverflow    *uint32
	cqFlags       *uint32
	cqes          *CQE

	// SQEs array
	sqes []SQE

	// Cached values
	sqRingMaskCached uint32
	cqRingMaskCached uint32
	sqEntriesCached  uint32
	cqEntriesCached  uint32

	onSubmit   func(opcode uint8)
	onComplete func(res int32)
}

// NewRing creates a new io_uring instance
func NewRing(entries uint32, flags uint32) (*Ring, error) {
	if entries == 0 || entries > 4096 {
		return nil, fmt.Errorf("invalid entries: %d", entries)
	}

	entries = nextPowerOf2(entries)

	params := Params{
		SqEntries: entries,
		CqEntries: entries * 2,
	}

	fd, err := ioUringSetup(entries, &params)
	if err != nil {
		return nil, fmt.Errorf("io_uring_setup failed: %w", err)
	}

	ring := &Ring{
		fd:     fd,
		params: params,
	}

	if err := ring.mmapRings(); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	ring.setupPointers()

	runtime.SetFinalizer(ring, (*Ring).Close)

	return ring, nil
}

// mmapRings maps the SQ and CQ rings
func (r *Ring) mmapRings() error {
	pageSize := unix.Getpagesize()

	// Calculate SQ ring size
	sqRingSize := int(r.params.SqOffsets.Array + r.params.SqEntries*uint32(unsafe.Sizeof(uint32(0))))
	sqRingSize = (sqRingSize + pageSize - 1) &^ (pageSize - 1)

	// Map SQ ring
	sqRing, err := unix.Mmap(r.fd, int64(IORING_OFF_SQ_RING), sqRingSize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		return fmt.Errorf("mmap sq ring failed: %w", err)
	}
	r.sqRingArea = sqRing

	// Calculate CQ ring size
	cqRingSize := int(r.params.CqOffsets.Cqes + r.params.CqEntries*uint32(unsafe.Sizeof(CQE{})))
	cqRingSize = (cqRingSize + pageSize - 1) &^ (pageSize - 1)

	// Map CQ ring separately
	cqRing, err := unix.Mmap(r.fd, int64(IORING_OFF_CQ_RING), cqRingSize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Munmap(sqRing)
		return fmt.Errorf("mmap cq ring failed: %w", err)
	}
	r.cqRingArea = cqRing

	// Map SQEs array
	sqeSize := int(r.params.SqEntries) * int(unsafe.Sizeof(SQE{}))
	sqeSize = (sqeSize + pageSize - 1) &^ (pageSize - 1)

	sqes, err := unix.Mmap(r.fd, int64(IORING_OFF_SQES), sqeSize,
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Munmap(cqRing)
		unix.Munmap(sqRing)
		return fmt.Errorf("mmap sqes failed: %w", err)
	}
	r.sqesArea = sqes

	return nil
}

// setupPointers initializes ring structure pointers
func (r *Ring) setupPointers() {
	// SQ ring pointers
	sqBase := (*[1 << 30]byte)(unsafe.Pointer(&r.sqRingArea[0]))
	r.sqHead = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.Head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.Tail]))
	r.sqRingMask = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.RingMask]))
	r.sqRingEntries = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.RingEntries]))
	r.sqFlags = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.Flags]))
	r.sqDropped = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.Dropped]))
	r.sqArray = (*uint32)(unsafe.Pointer(&sqBase[r.params.SqOffsets.Array]))

	// CQ ring pointers
	cqBase := (*[1 << 30]byte)(unsafe.Pointer(&r.cqRingArea[0]))
	r.cqHead = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.Head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.Tail]))
	r.cqRingMask = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.RingMask]))
	r.cqRingEntries = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.RingEntries]))
	r.cqOverflow = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.Overflow]))
	r.cqFlags = (*uint32)(unsafe.Pointer(&cqBase[r.params.CqOffsets.Flags]))
	r.cqes = (*CQE)(unsafe.Pointer(&cqBase[r.params.CqOffsets.Cqes]))

	// SQEs array
	r.sqes = unsafe.Slice((*SQE)(unsafe.Pointer(&r.sqesArea[0])), r.params.SqEntries)

	r.sqRingMaskCached = r.params.SqEntries - 1
	r.cqRingMaskCached = r.params.CqEntries - 1
	r.sqEntriesCached = r.params.SqEntries
	r.cqEntriesCached = r.params.CqEntries
}

// Close releases resources
func (r *Ring) Close() error {
	if r == nil || r.fd < 0 {
		return nil
	}

	runtime.SetFinalizer(r, nil)

	var errs []error

	if r.sqesArea != nil {
		if err := unix.Munmap(r.sqesArea); err != nil {
			errs = append(errs, err)
		}
	}

	if r.cqRingArea != nil {
		if err := unix.Munmap(r.cqRingArea); err != nil {
			errs = append(errs, err)
		}
	}

	if r.sqRingArea != nil {
		if err := unix.Munmap(r.sqRingArea); err != nil {
			errs = append(errs, err)
		}
	}

	if err := unix.Close(r.fd); err != nil {
		errs = append(errs, err)
	}
	r.fd = -1

	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}

	return nil
}

// Fd returns the ring file descriptor
func (r *Ring) Fd() int {
	return r.fd
}

// roundUpToPage rounds size up to page boundary
func roundUpToPage(size int) int {
	pageSize := unix.Getpagesize()
	return (size + pageSize - 1) &^ (pageSize - 1)
}

// nextPowerOf2 rounds up to next power of 2
func nextPowerOf2(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

// IORING_OFF offsets
const (
	IORING_OFF_SQ_RING uint64 = 0
	IORING_OFF_CQ_RING uint64 = 0x8000000
	IORING_OFF_SQES    uint64 = 0x10000000
)
