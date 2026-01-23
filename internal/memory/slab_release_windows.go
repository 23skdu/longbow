//go:build windows
// +build windows

package memory

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procVirtualAlloc = kernel32.NewProc("VirtualAlloc")
	procVirtualFree  = kernel32.NewProc("VirtualFree")
)

const (
	MEM_DECOMMIT = 0x4000
	MEM_RELEASE  = 0x8000
)

// ReleaseSlab hints the OS that the memory backing this slab can be reclaimed.
// On Windows, this uses VirtualFree with MEM_DECOMMIT to release physical pages
// while keeping the virtual address space reserved.
func ReleaseSlab(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	ptr := unsafe.Pointer(&b[0])
	length := uintptr(cap(b))

	// Decommit the pages (release physical memory but keep virtual address)
	ret, _, err := procVirtualFree.Call(
		uintptr(ptr),
		length,
		MEM_DECOMMIT,
	)

	if ret == 0 {
		return fmt.Errorf("VirtualFree(MEM_DECOMMIT) failed: %v", err)
	}

	return nil
}
