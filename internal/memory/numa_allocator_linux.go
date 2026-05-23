//go:build linux

package memory

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// GetCurrentCPU returns the current CPU number or -1 if unavailable.
// This is Linux-specific using getcpu syscall.
func GetCurrentCPU() int {
	var cpu uint
	_, _, errno := unix.Syscall(unix.SYS_GETCPU, uintptr(unsafe.Pointer(&cpu)), 0, 0)
	if errno != 0 {
		return -1
	}
	return int(cpu)
}
