//go:build linux

package memory

import (
	"runtime"

	"unsafe"
	"golang.org/x/sys/unix"
)

// MPOL_BIND specifies that the memory must be allocated on the specified nodes.
const mpolBind = 2

// PinToNUMANode pins the current goroutine to CPUs on the specified NUMA node.
// This ensures that the goroutine runs on CPUs local to the NUMA node,
// reducing remote memory access latency.
func PinToNUMANode(topo *NUMATopology, nodeID int) error {
	if nodeID >= topo.NumNodes || nodeID < 0 {
		return nil // Silently ignore invalid node IDs
	}

	// Lock goroutine to OS thread
	runtime.LockOSThread()

	// Create CPU set for this NUMA node
	var cpuSet unix.CPUSet
	cpus := topo.CPUs[nodeID]
	for _, cpu := range cpus {
		cpuSet.Set(cpu)
	}

	// Set CPU affinity for current thread
	return unix.SchedSetaffinity(0, &cpuSet)
}

// MbindMemory binds the specified memory range to a NUMA node.
// This uses the mbind(2) system call with MPOL_BIND policy.
func MbindMemory(ptr unsafe.Pointer, size int, nodeID int) error {
	if nodeID < 0 {
		return nil
	}

	// Create a node bitmask
	mask := uint64(1 << uint(nodeID)) // #nosec G115

	// On amd64, SYS_MBIND is 237. On arm64, it's 235.
	// Since this is linux-only, we can use a small switch or constant.
	var sysMbind uintptr
	switch runtime.GOARCH {
	case "amd64":
		sysMbind = 237
	case "arm64":
		sysMbind = 235
	default:
		return nil // Unsupported architecture for mbind
	}

	// maxnode is the number of bits in the mask
	maxnode := uintptr(nodeID + 1)

	_, _, errno := unix.RawSyscall6(sysMbind,
		uintptr(ptr),
		uintptr(size),
		uintptr(mpolBind),
		uintptr(unsafe.Pointer(&mask)),
		maxnode+1, // bits are 0-indexed, so we need at least nodeID+1 bits
		0)
	if errno != 0 && errno != unix.ENOSYS {
		return errno
	}
	return nil
}
