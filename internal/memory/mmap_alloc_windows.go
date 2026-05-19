//go:build windows

package memory

import (
	"errors"
	"sync/atomic"
)

type OffHeapAllocator struct {
	allocated atomic.Int64
}

func NewOffHeapAllocator() *OffHeapAllocator {
	return &OffHeapAllocator{}
}

func (a *OffHeapAllocator) Allocate(size int) []byte {
	// On Windows stub, we just use heap for now to allow compilation.
	// Production Windows support would use VirtualAlloc.
	return make([]byte, size)
}

func (a *OffHeapAllocator) Free(b []byte) {
	// No-op for heap-backed stub
}

func (a *OffHeapAllocator) Reallocate(size int, b []byte) []byte {
	if len(b) == size {
		return b
	}
	newBuf := make([]byte, size)
	copy(newBuf, b)
	return newBuf
}

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
	MadvRandom     = 0
	MadvSequential = 0
	MadvWillNeed   = 0
	MadvDontNeed   = 0
)
