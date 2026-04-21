package core

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/prometheus/client_golang/prometheus"
)

// RepairAgentConfig configures the HNSW connectivity repair agent
type RepairAgentConfig struct {
	Enabled            bool          // Enable repair agent
	ScanInterval       time.Duration // How often to scan for orphans (default: 5m)
	MaxRepairsPerCycle int           // Max repairs per scan cycle (default: 100)
}

// DefaultRepairAgentConfig returns sensible defaults
func DefaultRepairAgentConfig() RepairAgentConfig {
	return RepairAgentConfig{
		Enabled:            false, // Opt-in
		ScanInterval:       5 * time.Minute,
		MaxRepairsPerCycle: 100,
	}
}

// RepairAgent detects and repairs disconnected sub-graphs in HNSW
type RepairAgent struct {
	index  *ArrowHNSW
	config RepairAgentConfig

	// State
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewRepairAgent creates a new HNSW repair agent
func NewRepairAgent(index *ArrowHNSW, config RepairAgentConfig) *RepairAgent {
	// Validate config
	if config.ScanInterval <= 0 {
		config.ScanInterval = 5 * time.Minute
	}
	if config.MaxRepairsPerCycle <= 0 {
		config.MaxRepairsPerCycle = 100
	}

	return &RepairAgent{
		index:  index,
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start begins the repair agent background worker
func (r *RepairAgent) Start() {
	if !r.config.Enabled {
		return
	}

	if r.running.Swap(true) {
		return // Already running
	}

	r.wg.Add(1)
	go r.run()
}

// Stop halts the repair agent
func (r *RepairAgent) Stop() {
	if !r.running.Swap(false) {
		return // Not running
	}

	close(r.stopCh)
	r.wg.Wait()
}

// run is the main repair loop
func (r *RepairAgent) run() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.runRepairCycle()
		}
	}
}

// runRepairCycle performs one repair cycle
func (r *RepairAgent) runRepairCycle() int {
	if r.index == nil {
		return 0
	}

	datasetName := "default"
	if r.index.dataset != nil {
		datasetName = r.index.dataset.GetName()
	}

	timer := prometheus.NewTimer(metrics.HNSWRepairScanDuration.WithLabelValues(datasetName))
	defer timer.ObserveDuration()

	// Detect orphans
	orphans := r.detectOrphans()

	if len(orphans) > 0 {
		metrics.HNSWRepairOrphansDetected.WithLabelValues(datasetName).Add(float64(len(orphans)))
	}

	// Repair orphans (up to max)
	repaired := 0
	for _, orphan := range orphans {
		if repaired >= r.config.MaxRepairsPerCycle {
			break
		}

		r.repairOrphan(orphan, 0)
		repaired++
	}

	if repaired > 0 {
		metrics.HNSWRepairOrphansRepaired.WithLabelValues(datasetName).Add(float64(repaired))
	}

	metrics.HNSWRepairLastScanTime.WithLabelValues(datasetName).SetToCurrentTime()

	return repaired
}

// detectOrphans finds nodes that are unreachable from entry points
func (r *RepairAgent) detectOrphans() []uint32 {
	if r.index == nil {
		return nil
	}

	data := r.index.data.Load()
	dg := r.index.diskGraph.Load()
	if data == nil {
		return nil
	}

	nodeCount := int(r.index.nodeCount.Load())
	if nodeCount == 0 {
		return nil
	}

	// BFS from entry point to mark reachable nodes
	reachable := make(map[uint32]bool)
	queue := []uint32{}

	// Start from entry point at layer 0
	entryPoint := r.index.entryPoint.Load()
	if entryPoint == 0 && nodeCount > 0 {
		// If no explicit entry point, use node 0
		queue = append(queue, 0)
	} else if entryPoint < uint32(nodeCount) { // #nosec G115
		queue = append(queue, entryPoint)
	}

	// BFS traversal
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if reachable[current] {
			continue
		}
		reachable[current] = true

		// Check if node is deleted
		if r.index.deleted.Contains(current) {
			continue
		}

		// Get neighbors at layer 0 using unified accessor with cached DiskGraph
		neighbors := r.index.GetNeighborsCombinedCached(0, current, dg)
		for _, neighbor := range neighbors {
			if !reachable[neighbor] && !r.index.deleted.Contains(neighbor) {
				queue = append(queue, neighbor)
			}
		}
	}

	// Find orphans (nodes not reachable)
	orphans := []uint32{}
	for i := 0; i < nodeCount; i++ {
		nodeID := uint32(i)
		if !reachable[nodeID] && !r.index.deleted.Contains(nodeID) {
			orphans = append(orphans, nodeID)
		}
	}

	return orphans
}

// repairOrphan re-links an orphaned node to the graph
func (r *RepairAgent) repairOrphan(orphan uint32, layer int) {
	if r.index == nil {
		return
	}

	data := r.index.data.Load()
	if data == nil {
		return
	}

	// Get orphan's vector
	orphanVecAny := r.index.mustGetVectorFromData(data, orphan)
	if orphanVecAny == nil {
		return // Can't repair without vector
	}
	orphanVec, okOrphan := orphanVecAny.([]float32)
	if !okOrphan {
		return // Only support float32 for now
	}

	// Find K nearest neighbors in the reachable set
	// We'll do a simple linear scan for now (could be optimized)
	nodeCount := int(r.index.nodeCount.Load())
	k := r.index.m // Use M as target neighbor count

	type candidate struct {
		id   uint32
		dist float32
	}
	candidates := []candidate{}

	for i := 0; i < nodeCount; i++ {
		nodeID := uint32(i)
		if nodeID == orphan {
			continue
		}
		if r.index.deleted.Contains(nodeID) {
			continue
		}

		nodeVecAny := r.index.mustGetVectorFromData(data, nodeID)
		if nodeVecAny == nil {
			continue
		}
		nodeVec, okNode := nodeVecAny.([]float32)
		if !okNode {
			continue
		}

		// Use SIMD distance function
		dist, err := simd.DistFunc(orphanVec, nodeVec)
		if err != nil {
			dist = math.MaxFloat32
		}
		candidates = append(candidates, candidate{id: nodeID, dist: dist})
	}

	// Sort by distance and take top K
	// Simple selection for K nearest
	if len(candidates) > k {
		// Partial sort to get K nearest
		for i := 0; i < k && i < len(candidates); i++ {
			minIdx := i
			for j := i + 1; j < len(candidates); j++ {
				if candidates[j].dist < candidates[minIdx].dist {
					minIdx = j
				}
			}
			if minIdx != i {
				candidates[i], candidates[minIdx] = candidates[minIdx], candidates[i]
			}
		}
		candidates = candidates[:k]
	}

	// Add bidirectional edges
	searchCtx := r.index.searchPool.Get()
	defer r.index.searchPool.Put(searchCtx)

	maxConn := r.index.mMax
	if layer == 0 {
		maxConn = r.index.mMax0
	}

	for _, c := range candidates {
		// Add edge from orphan to candidate
		data = r.index.AddConnection(searchCtx, data, orphan, c.id, layer, maxConn, c.dist)
		// Add edge from candidate to orphan (bidirectional)
		data = r.index.AddConnection(searchCtx, data, c.id, orphan, layer, maxConn, c.dist)
	}

}

// getNeighbors is now deprecated in favor of r.index.GetNeighborsCombined.

// addEdge is now deprecated in favor of using r.index.AddConnection directly.
// Removing it to prevent accidental bypassing of COW/Locking logic.
