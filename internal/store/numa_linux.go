//go:build linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// NUMATopology represents the NUMA topology of the system.
type NUMATopology struct {
	NumNodes int     // Number of NUMA nodes
	CPUs     [][]int // CPUs[nodeID] = list of CPU IDs on that node
}

// DetectNUMATopology detects the NUMA topology on Linux systems.
// Returns a single-node topology if NUMA is not available.
func DetectNUMATopology() (*NUMATopology, error) {
	// Check if NUMA is available
	numaPath := "/sys/devices/system/node"
	if _, err := os.Stat(numaPath); os.IsNotExist(err) {
		// No NUMA support - return single node
		return &NUMATopology{
			NumNodes: 1,
			CPUs:     [][]int{{0}}, // Assume at least one CPU
		}, nil
	}

	// Find all NUMA nodes
	nodes, err := filepath.Glob(filepath.Join(numaPath, "node*"))
	if err != nil {
		return nil, fmt.Errorf("failed to glob NUMA nodes: %w", err)
	}

	if len(nodes) == 0 {
		// No nodes found - return single node
		return &NUMATopology{NumNodes: 1, CPUs: [][]int{{0}}}, nil
	}

	topo := &NUMATopology{
		NumNodes: len(nodes),
		CPUs:     make([][]int, len(nodes)),
	}

	// Parse CPU list for each node
	for i, nodePath := range nodes {
		cpuListPath := filepath.Join(nodePath, "cpulist")
		cpuListBytes, err := os.ReadFile(cpuListPath)
		if err != nil {
			// Skip this node if we can't read cpulist
			continue
		}

		cpuList := strings.TrimSpace(string(cpuListBytes))
		cpus, err := parseCPUList(cpuList)
		if err != nil {
			// Skip malformed CPU lists
			continue
		}
		topo.CPUs[i] = cpus
	}

	return topo, nil
}

// parseCPUList parses Linux CPU list format (e.g., "0-3,8-11" or "0,2,4,6").
func parseCPUList(cpulist string) ([]int, error) {
	var cpus []int

	if cpulist == "" {
		return cpus, nil
	}

	parts := strings.Split(cpulist, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.Contains(part, "-") {
			// Range format: "0-3"
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range: %s", part)
			}

			start, err1 := strconv.Atoi(strings.TrimSpace(rangeParts[0]))
			end, err2 := strconv.Atoi(strings.TrimSpace(rangeParts[1]))
			if err1 != nil || err2 != nil {
				return nil, fmt.Errorf("invalid range numbers: %s", part)
			}

			for i := start; i <= end; i++ {
				cpus = append(cpus, i)
			}
		} else {
			// Single CPU
			cpu, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid cpu number: %s", part)
			}
			cpus = append(cpus, cpu)
		}
	}

	return cpus, nil
}

// String returns a human-readable representation of the topology.
func (t *NUMATopology) String() string {
	if t.NumNodes == 1 {
		return "Single NUMA node (no NUMA)"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d NUMA nodes:\n", t.NumNodes))
	for i, cpus := range t.CPUs {
		sb.WriteString(fmt.Sprintf("  Node %d: CPUs %v\n", i, cpus))
	}
	return sb.String()
}
