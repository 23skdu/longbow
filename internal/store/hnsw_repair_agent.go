package store

import (
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
		datasetName = r.index.dataset.Name
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

		if r.repairOrphan(orphan, 0) == nil {
			repaired++
		}
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
	} else if entryPoint < uint32(nodeCount) {
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
		if r.index.deleted.Contains(int(current)) {
			continue
		}

		// Get neighbors at layer 0
		neighbors := r.getNeighbors(current, 0, data)
		for _, neighbor := range neighbors {
			if !reachable[neighbor] && !r.index.deleted.Contains(int(neighbor)) {
				queue = append(queue, neighbor)
			}
		}
	}

	// Find orphans (nodes not reachable)
	orphans := []uint32{}
	for i := 0; i < nodeCount; i++ {
		nodeID := uint32(i)
		if !reachable[nodeID] && !r.index.deleted.Contains(int(nodeID)) {
			orphans = append(orphans, nodeID)
		}
	}

	return orphans
}

// repairOrphan re-links an orphaned node to the graph
func (r *RepairAgent) repairOrphan(orphan uint32, layer int) error {
	if r.index == nil {
		return nil
	}

	data := r.index.data.Load()
	if data == nil {
		return nil
	}

	// Get orphan's vector
	orphanVec := r.index.mustGetVectorFromData(data, orphan)
	if orphanVec == nil {
		return nil // Can't repair without vector
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
		if r.index.deleted.Contains(int(nodeID)) {
			continue
		}

		nodeVec := r.index.mustGetVectorFromData(data, nodeID)
		if nodeVec == nil {
			continue
		}

		// Use SIMD distance function
		dist := simd.DistFunc(orphanVec, nodeVec)
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
	for _, c := range candidates {
		// Add edge from orphan to candidate
		r.addEdge(orphan, c.id, layer, data)
		// Add edge from candidate to orphan (bidirectional)
		r.addEdge(c.id, orphan, layer, data)
	}

	return nil
}

// getNeighbors retrieves neighbors of a node at a given layer
func (r *RepairAgent) getNeighbors(nodeID uint32, layer int, data *GraphData) []uint32 {
	cID := chunkID(nodeID)
	cOff := chunkOffset(nodeID)

	nc := data.GetNeighborsChunk(layer, cID)
	cc := data.GetCountsChunk(layer, cID)

	if nc == nil || cc == nil {
		return nil
	}

	count := int(atomic.LoadInt32(&cc[cOff]))
	if count == 0 {
		return nil
	}

	base := int(cOff) * MaxNeighbors
	neighbors := make([]uint32, count)
	for i := 0; i < count; i++ {
		neighbors[i] = nc[base+i]
	}

	return neighbors
}

// addEdge adds an edge from 'from' to 'to' at the given layer
func (r *RepairAgent) addEdge(from, to uint32, layer int, data *GraphData) {
	cID := chunkID(from)
	cOff := chunkOffset(from)

	nc := data.GetNeighborsChunk(layer, cID)
	cc := data.GetCountsChunk(layer, cID)

	if nc == nil || cc == nil {
		return
	}

	// Check if edge already exists
	count := int(atomic.LoadInt32(&cc[cOff]))
	base := int(cOff) * MaxNeighbors

	for i := 0; i < count; i++ {
		if nc[base+i] == to {
			return // Edge already exists
		}
	}

	// Add edge if there's space
	if count < MaxNeighbors {
		nc[base+count] = to
		atomic.StoreInt32(&cc[cOff], int32(count+1))
	}
}
