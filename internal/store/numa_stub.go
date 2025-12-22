//go:build !linux

package store

// NUMATopology represents the NUMA topology of the system.
// On non-Linux systems, this always returns a single-node topology.
type NUMATopology struct {
	NumNodes int
	CPUs     [][]int
}

// DetectNUMATopology returns a single-node topology on non-Linux systems.
// macOS (M-series) and most non-server systems don't have NUMA.
func DetectNUMATopology() (*NUMATopology, error) {
	return &NUMATopology{
		NumNodes: 1,
		CPUs:     [][]int{{0}}, // Single node with at least one CPU
	}, nil
}

// String returns a human-readable representation.
func (t *NUMATopology) String() string {
	return "Single NUMA node (no NUMA support on this platform)"
}

// GetNodeForCPU returns 0 for any valid CPU on single-node systems.
func (t *NUMATopology) GetNodeForCPU(cpu int) int {
	if cpu < 0 {
		return -1
	}
	return 0
}
