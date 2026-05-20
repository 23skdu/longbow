package store

import (
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/pkg/loadbalancing"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// NodeMonitor tracks local resource usage to provide load balancing hints.
type NodeMonitor struct {
	cpuLoad     atomic.Uint32 // Percentage 0-100
	memLoad     atomic.Uint32 // Percentage 0-100
	queueDepth  atomic.Int64
	activeTasks atomic.Int64
	
	stopChan chan struct{}
}

// NewNodeMonitor creates and starts a new NodeMonitor.
func NewNodeMonitor() *NodeMonitor {
	nm := &NodeMonitor{
		stopChan: make(chan struct{}),
	}
	go nm.run()
	return nm
}

func (nm *NodeMonitor) run() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nm.updateStats()
		case <-nm.stopChan:
			return
		}
	}
}

func (nm *NodeMonitor) updateStats() {
	// CPU Load
	percentages, err := cpu.Percent(0, false)
	if err == nil && len(percentages) > 0 {
		nm.cpuLoad.Store(uint32(percentages[0]))
	}

	// Memory Load
	v, err := mem.VirtualMemory()
	if err == nil {
		nm.memLoad.Store(uint32(v.UsedPercent))
	}
}

// Stop terminates the background monitoring loop.
func (nm *NodeMonitor) Stop() {
	close(nm.stopChan)
}

// GetLoadHints returns current load balancing hints.
func (nm *NodeMonitor) GetLoadHints() loadbalancing.LoadHints {
	return loadbalancing.LoadHints{
		CPULoad:    nm.cpuLoad.Load(),
		MemLoad:    nm.memLoad.Load(),
		QueueDepth: nm.queueDepth.Load(),
		Health:     100,
	}
}

// IncrementQueue increases the queue depth by one.
func (nm *NodeMonitor) IncrementQueue() { nm.queueDepth.Add(1) }

// DecrementQueue decreases the queue depth by one.
func (nm *NodeMonitor) DecrementQueue() { nm.queueDepth.Add(-1) }
