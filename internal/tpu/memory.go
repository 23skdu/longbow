package tpu

import (
	"golang.org/x/sys/unix"
	"unsafe"
)

// PinnedBuffer represents memory locked in RAM to prevent swapping,
// ideal for high-speed Host-to-Device transfers via PCIe/CXL for TPU batching.
type PinnedBuffer struct {
	Data []byte
}

// AllocatePinned allocates memory and locks it into RAM.
func AllocatePinned(size int) (*PinnedBuffer, error) {
	data, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	if err := unix.Mlock(data); err != nil {
		_ = unix.Munmap(data)
		return nil, err
	}

	return &PinnedBuffer{Data: data}, nil
}

// Free unlocks and unmaps the pinned memory.
func (p *PinnedBuffer) Free() error {
	_ = unix.Munlock(p.Data)
	return unix.Munmap(p.Data)
}

// Float32Slice returns the pinned memory as a float32 slice for vector embedding ingestion.
func (p *PinnedBuffer) Float32Slice() []float32 {
	if len(p.Data) == 0 {
		return nil
	}
	ptr := (*float32)(unsafe.Pointer(&p.Data[0]))
	return unsafe.Slice(ptr, len(p.Data)/4)
}
