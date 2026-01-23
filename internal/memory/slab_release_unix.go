//go:build linux || darwin
// +build linux darwin

package memory

import (
	"fmt"
	"syscall"
	"unsafe"
)

// ReleaseSlab hints the OS that the memory backing this slab can be reclaimed.
// On Linux/Darwin, this uses madvise(MADV_DONTNEED) to mark pages as reclaimable.
// The slice remains valid but accessing it may cause a page fault and re-zero.
func ReleaseSlab(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	// Get the underlying memory address
	ptr := unsafe.Pointer(&b[0])
	length := uintptr(cap(b))

	// Call madvise with MADV_DONTNEED
	// This tells the kernel it can reclaim the physical pages
	_, _, errno := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(ptr),
		length,
		syscall.MADV_DONTNEED,
	)

	if errno != 0 {
		return fmt.Errorf("madvise(MADV_DONTNEED) failed: %v", errno)
	}

	return nil
}
