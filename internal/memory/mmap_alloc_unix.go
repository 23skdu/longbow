//go:build linux || darwin
// +build linux darwin

package memory

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// OffHeapAllocator implements a non-GC scanned allocator using mmap.
// This is used for large buffers to reduce runtime.scanObject overhead.
type OffHeapAllocator struct {
	allocated atomic.Int64
}

// NewOffHeapAllocator creates a new mmap-based allocator.
func NewOffHeapAllocator() *OffHeapAllocator {
	return &OffHeapAllocator{}
}

// Allocate allocates a byte slice of the given size from the OS.
// The memory is NOT managed by the Go GC.
func (a *OffHeapAllocator) Allocate(size int) []byte {
	if size <= 0 {
		return nil
	}

	// Use MAP_ANONYMOUS for zero-initialized memory from the OS
	data, err := unix.Mmap(
		-1,
		0,
		size,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_ANON|unix.MAP_PRIVATE,
	)

	if err != nil {
		// Fallback to heap if mmap fails (though we should probably panic in production)
		return make([]byte, size)
	}

	a.allocated.Add(int64(size))
	return data
}

// Free releases the memory back to the OS.
func (a *OffHeapAllocator) Free(b []byte) {
	if len(b) == 0 {
		return
	}

	size := cap(b)
	err := unix.Munmap(b)
	if err == nil {
		a.allocated.Add(-int64(size))
	}
}

// Reallocate resizes a slice.
func (a *OffHeapAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == size {
		return b
	}
	newBuf := a.Allocate(size)
	if len(b) > 0 {
		copy(newBuf, b)
		a.Free(b)
	}
	return newBuf
}

// Allocated returns total bytes currently allocated via this allocator.
func (a *OffHeapAllocator) Allocated() int64 {
	return a.allocated.Load()
}

// CastToSlice converts a pointer and length to a byte slice without allocation.
func CastToSlice(ptr unsafe.Pointer, length int) []byte {
	var sl []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&sl)) // #nosec G103
	header.Data = uintptr(ptr)
	header.Len = length
	header.Cap = length
	return sl
}
// Mmap maps a file into memory.
func Mmap(fd int, offset int64, length int, writable bool) ([]byte, error) {
	flags := unix.MAP_SHARED
	prot := unix.PROT_READ
	if writable {
		prot |= unix.PROT_WRITE
	}
	return unix.Mmap(fd, offset, length, prot, flags)
}

// Munmap unmaps a previously mapped memory region.
func Munmap(b []byte) error {
	return unix.Munmap(b)
}

// Madvise provides hints to the kernel about the memory usage pattern.
func Madvise(b []byte, advice int) error {
	return unix.Madvise(b, advice)
}

const (
	MadvRandom     = unix.MADV_RANDOM
	MadvSequential = unix.MADV_SEQUENTIAL
	MadvWillNeed   = unix.MADV_WILLNEED
	MadvDontNeed   = unix.MADV_DONTNEED
)
