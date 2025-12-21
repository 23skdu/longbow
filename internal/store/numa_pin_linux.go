//go:build linux

package store

import (
	"runtime"

	"golang.org/x/sys/unix"
)

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
