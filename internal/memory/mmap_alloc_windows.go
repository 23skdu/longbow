//go:build windows

package memory

import (
	"errors"
	"sync/atomic"
)

// OffHeapAllocator represents a off heap allocator value.
type OffHeapAllocator struct {
	allocated atomic.Int64
}

// NewOffHeapAllocator constructs a new OffHeapAllocator.
func NewOffHeapAllocator() *OffHeapAllocator {
	return &OffHeapAllocator{}
}

// Allocate runs the allocate operation.
func (a *OffHeapAllocator) Allocate(size int) []byte {
	// On Windows stub, we just use heap for now to allow compilation.
	// Production Windows support would use VirtualAlloc.
	return make([]byte, size)
}

// Free runs the free operation.
func (a *OffHeapAllocator) Free(b []byte) {
	// No-op for heap-backed stub
}

// Reallocate runs the reallocate operation.
func (a *OffHeapAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == size {
		return b
	}
	newBuf := make([]byte, size)
	copy(newBuf, b)
	return newBuf
}

// Allocated runs the allocated operation.
func (a *OffHeapAllocator) Allocated() int64 {
	return 0
}

// Mmap is a stub for Windows.
func Mmap(fd int, offset int64, length int, writable bool) ([]byte, error) {
	return nil, errors.New("mmap not implemented on windows stub")
}

// Munmap is a stub for Windows.
func Munmap(b []byte) error {
	return nil
}

// Madvise is a stub for Windows.
func Madvise(b []byte, advice int) error {
	return nil
}

const (
	// MadvRandom is a madv random constant.
	MadvRandom = 0
	// MadvSequential is a madv sequential constant.
	MadvSequential = 0
	// MadvWillNeed is a madv will need constant.
	MadvWillNeed = 0
	// MadvDontNeed is a madv dont need constant.
	MadvDontNeed = 0
)
