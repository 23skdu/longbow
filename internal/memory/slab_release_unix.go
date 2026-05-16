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
	ptr := unsafe.Pointer(&b[0]) // #nosec G103
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

// AdviseHugePage hints the OS to use hugepages for the memory backing this slab.
// This can significantly reduce TLB misses during random access to large buffers.
func AdviseHugePage(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	// Get the underlying memory address
	ptr := unsafe.Pointer(&b[0]) // #nosec G103
	length := uintptr(cap(b))

	// Call madvise with MADV_HUGEPAGE (Linux only, no-op on Darwin)
	// We use the literal value for MADV_HUGEPAGE (14) if not defined in syscall
	const MADV_HUGEPAGE = 14
	
	_, _, errno := syscall.Syscall(
		syscall.SYS_MADVISE,
		uintptr(ptr),
		length,
		MADV_HUGEPAGE,
	)

	if errno != 0 && errno != syscall.ENOSYS && errno != syscall.EINVAL {
		return fmt.Errorf("madvise(MADV_HUGEPAGE) failed: %v", errno)
	}

	return nil
}
