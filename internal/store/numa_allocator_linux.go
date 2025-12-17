//go:build linux

package store

import (
"runtime"
"unsafe"

"golang.org/x/sys/unix"
)

// setCPUAffinity sets CPU affinity for the current thread to the given CPUs.
// This is Linux-specific using sched_setaffinity syscall.
func setCPUAffinity(cpus []int) error {
runtime.LockOSThread()
var cpuSet unix.CPUSet
for _, cpu := range cpus {
cpuSet.Set(cpu)
}
return unix.SchedSetaffinity(0, &cpuSet)
}

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
