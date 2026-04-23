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
	// unix.Mbind takes a slice of uint64 for the node mask
	// Each bit represents a node ID
	mask := []uint64{uint64(1 << uint(nodeID))} // #nosec G115

	// 0x7fffffffffffffff is the max size for mbind on 64-bit systems
	// But we use the actual size passed in.
	return unix.Mbind(ptr, uintptr(size), mpolBind, mask, uint32(nodeID+1), 0)
}
