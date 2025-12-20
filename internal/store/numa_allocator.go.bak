package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
)

// NodeSelectionMode defines how NUMA nodes are selected for allocation
type NodeSelectionMode int

const (
	// AutoDetect selects node based on current CPU
	AutoDetect NodeSelectionMode = iota
	// RoundRobin cycles through available nodes
	RoundRobin
	// PreferNodes uses only specified preferred nodes
	PreferNodes
)

// NUMAConfig configures NUMA-aware memory allocation
type NUMAConfig struct {
	Enabled        bool
	NodeSelection  NodeSelectionMode
	PreferredNodes []int
}

// NewNUMAConfig creates a default NUMA configuration (disabled)
func NewNUMAConfig() *NUMAConfig {
	return &NUMAConfig{
		Enabled:        false,
		NodeSelection:  AutoDetect,
		PreferredNodes: nil,
	}
}

// Validate checks the configuration for errors
func (c *NUMAConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	switch c.NodeSelection {
	case AutoDetect, RoundRobin:
		return nil
	case PreferNodes:
		if len(c.PreferredNodes) == 0 {
			return errors.New("PreferredNodes required when NodeSelection is PreferNodes")
		}
		return nil
	default:
		return fmt.Errorf("invalid NodeSelectionMode: %d", c.NodeSelection)
	}
}

// NUMATopology represents the system NUMA topology
type NUMATopology struct {
	nodes    []int
	nodeCPUs map[int][]int
	cpuNode  map[int]int
	numCPUs  int
}

// NumNodes returns the number of NUMA nodes
func (t *NUMATopology) NumNodes() int { return len(t.nodes) }

// NumCPUs returns the total number of CPUs
func (t *NUMATopology) NumCPUs() int { return t.numCPUs }

// GetNodeCPUs returns CPUs belonging to a node
func (t *NUMATopology) GetNodeCPUs(node int) []int { return t.nodeCPUs[node] }

// GetCPUNode returns the NUMA node for a CPU
func (t *NUMATopology) GetCPUNode(cpu int) int {
	if node, ok := t.cpuNode[cpu]; ok {
		return node
	}
	return 0
}

// DetectNUMATopology reads NUMA topology from sysfs
func DetectNUMATopology() (*NUMATopology, error) {
	numaPath := "/sys/devices/system/node"
	if _, err := os.Stat(numaPath); os.IsNotExist(err) {
		return nil, errors.New("NUMA sysfs not available")
	}
	entries, err := os.ReadDir(numaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read NUMA sysfs: %w", err)
	}
	topo := &NUMATopology{
		nodes:    make([]int, 0),
		nodeCPUs: make(map[int][]int),
		cpuNode:  make(map[int]int),
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "node") {
			continue
		}
		nodeStr := strings.TrimPrefix(entry.Name(), "node")
		nodeID, err := strconv.Atoi(nodeStr)
		if err != nil {
			continue
		}
		topo.nodes = append(topo.nodes, nodeID)
		cpuListPath := filepath.Join(numaPath, entry.Name(), "cpulist")
		cpuData, err := os.ReadFile(cpuListPath)
		if err != nil {
			continue
		}
		cpus := parseCPUList(strings.TrimSpace(string(cpuData)))
		topo.nodeCPUs[nodeID] = cpus
		for _, cpu := range cpus {
			topo.cpuNode[cpu] = nodeID
			topo.numCPUs++
		}
	}
	if len(topo.nodes) == 0 {
		return nil, errors.New("no NUMA nodes found")
	}
	return topo, nil
}

func parseCPUList(cpuList string) []int {
	var cpus []int
	if cpuList == "" {
		return cpus
	}
	for _, part := range strings.Split(cpuList, ",") {
		part = strings.TrimSpace(part)
		if strings.Contains(part, "-") {
			bounds := strings.Split(part, "-")
			if len(bounds) == 2 {
				start, _ := strconv.Atoi(bounds[0])
				end, _ := strconv.Atoi(bounds[1])
				for i := start; i <= end; i++ {
					cpus = append(cpus, i)
				}
			}
		} else {
			cpu, _ := strconv.Atoi(part)
			cpus = append(cpus, cpu)
		}
	}
	return cpus
}

// IsNUMAAvailable checks if NUMA is available on this system
func IsNUMAAvailable() bool {
	topo, err := DetectNUMATopology()
	return err == nil && topo.NumNodes() > 0
}

// NUMAStats tracks allocation statistics
type NUMAStats struct {
	TotalAllocations int64
	TotalBytes       int64
	NodeAllocations  map[int]int64
}

// NUMAAllocator provides NUMA-aware memory allocation
type NUMAAllocator struct {
	config        NUMAConfig
	topology      *NUMATopology
	enabled       bool
	roundRobinIdx uint64
	preferredIdx  uint64
	totalAllocs   int64
	totalBytes    int64
	nodeAllocs    map[int]*int64
}

// NewNUMAAllocator creates a new NUMA-aware allocator
func NewNUMAAllocator(cfg NUMAConfig) (*NUMAAllocator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	alloc := &NUMAAllocator{
		config:     cfg,
		enabled:    false,
		nodeAllocs: make(map[int]*int64),
	}
	if cfg.Enabled {
		topo, err := DetectNUMATopology()
		if err != nil {
			return nil, fmt.Errorf("failed to detect NUMA topology: %w", err)
		}
		alloc.topology = topo
		alloc.enabled = true
		for _, node := range topo.nodes {
			counter := int64(0)
			alloc.nodeAllocs[node] = &counter
		}
	}
	return alloc, nil
}

// IsEnabled returns whether NUMA allocation is active
func (a *NUMAAllocator) IsEnabled() bool { return a.enabled }

// NumNodes returns number of NUMA nodes
func (a *NUMAAllocator) NumNodes() int {
	if a.topology == nil {
		return 0
	}
	return a.topology.NumNodes()
}

// NextNode returns the next node to use based on selection mode
func (a *NUMAAllocator) NextNode() int {
	if !a.enabled || a.topology == nil {
		return 0
	}
	switch a.config.NodeSelection {
	case AutoDetect:
		cpu := GetCurrentCPU()
		if cpu >= 0 {
			return a.topology.GetCPUNode(cpu)
		}
		return 0
	case RoundRobin:
		idx := atomic.AddUint64(&a.roundRobinIdx, 1) - 1
		return a.topology.nodes[int(idx)%len(a.topology.nodes)] //nolint:gosec // G115 - safe modulo
	case PreferNodes:
		if len(a.config.PreferredNodes) == 0 {
			return 0
		}
		idx := atomic.AddUint64(&a.preferredIdx, 1) - 1
		return a.config.PreferredNodes[int(idx)%len(a.config.PreferredNodes)] //nolint:gosec // G115 - safe modulo
	}
	return 0
}

// AllocBytes allocates a byte slice
func (a *NUMAAllocator) AllocBytes(size int) []byte {
	buf := make([]byte, size)
	atomic.AddInt64(&a.totalAllocs, 1)
	atomic.AddInt64(&a.totalBytes, int64(size))
	return buf
}

// AllocFloat32Slice allocates a float32 slice
func (a *NUMAAllocator) AllocFloat32Slice(count int) []float32 {
	slice := make([]float32, count)
	atomic.AddInt64(&a.totalAllocs, 1)
	atomic.AddInt64(&a.totalBytes, int64(count*4))
	return slice
}

// AllocBytesOnNode allocates bytes preferring a specific NUMA node
func (a *NUMAAllocator) AllocBytesOnNode(size, node int) ([]byte, error) {
	if !a.enabled {
		return nil, errors.New("NUMA not enabled")
	}
	if err := a.BindGoroutineToNode(node); err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	atomic.AddInt64(&a.totalAllocs, 1)
	atomic.AddInt64(&a.totalBytes, int64(size))
	if counter, ok := a.nodeAllocs[node]; ok {
		atomic.AddInt64(counter, 1)
	}
	return buf, nil
}

// BindGoroutineToNode binds the current goroutine to CPUs on a NUMA node.
// Uses platform-specific CPU affinity syscalls (Linux only).
func (a *NUMAAllocator) BindGoroutineToNode(node int) error {
	if !a.enabled || a.topology == nil {
		return errors.New("NUMA not enabled")
	}
	cpus := a.topology.GetNodeCPUs(node)
	if len(cpus) == 0 {
		return fmt.Errorf("no CPUs found for node %d", node)
	}
	runtime.LockOSThread()
	if err := setCPUAffinity(cpus); err != nil {
		return fmt.Errorf("failed to set CPU affinity: %w", err)
	}
	return nil
}

// GetCurrentNode returns the NUMA node of the current CPU
func (a *NUMAAllocator) GetCurrentNode() int {
	if !a.enabled || a.topology == nil {
		return -1
	}
	cpu := GetCurrentCPU()
	if cpu < 0 {
		return -1
	}
	return a.topology.GetCPUNode(cpu)
}

// GetStats returns allocation statistics
func (a *NUMAAllocator) GetStats() NUMAStats {
	stats := NUMAStats{
		TotalAllocations: atomic.LoadInt64(&a.totalAllocs),
		TotalBytes:       atomic.LoadInt64(&a.totalBytes),
		NodeAllocations:  make(map[int]int64),
	}
	for node, counter := range a.nodeAllocs {
		stats.NodeAllocations[node] = atomic.LoadInt64(counter)
	}
	return stats
}
