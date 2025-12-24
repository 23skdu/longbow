package hnsw2

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Grow ensures the graph has enough capacity for the requested ID.
// It uses a Copy-On-Resize strategy: a new GraphData is allocated, data copied, and atomically swapped.
func (h *ArrowHNSW) Grow(minCap int) {
	// Fast path: check current capacity
	oldData := h.data.Load()
	if oldData.Capacity > minCap {
		return
	}

	// Calculate new capacity
	newCap := oldData.Capacity * 2
	if newCap < minCap {
		newCap = minCap
	}
	if newCap < 1000 {
		newCap = 1000
	}

	// Allocate new data
	newData := NewGraphData(newCap)

	// Copy existing data
	// 1. Levels
	copy(newData.Levels, oldData.Levels)
	
	// 2. VectorPtrs
	copy(newData.VectorPtrs, oldData.VectorPtrs)
	
	// 3. Neighbors and Counts for each layer
	for i := 0; i < MaxLayers; i++ {
		copy(newData.Neighbors[i], oldData.Neighbors[i])
		copy(newData.Counts[i], oldData.Counts[i])
	}
	
	// Atomically switch to new data
	// Note: Since this is called under writeMu, we are the only writer.
	// Readers might be using oldData, which is fine (snapshot).
	h.data.Store(newData)
}

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	
	// Ensure capacity
	h.Grow(int(id) + 1)
	
	// Get current graph data
	data := h.data.Load()
	
	// Initialize the new node
	data.Levels[id] = uint8(level)
	
	// Check if this is the first node
	if h.nodeCount.Load() == 0 {
		h.entryPoint.Store(id)
		h.maxLevel.Store(int32(level))
		h.nodeCount.Add(1)
		return nil
	}
	
	// Get vector for distance calculations
	vec, err := h.getVector(id)
	if err != nil {
		return err
	}
	
	// Cache dimensions on first insert
	if h.dims == 0 && len(vec) > 0 {
		h.dims = len(vec)
	}
	
	// Cache unsafe pointer
	if len(vec) > 0 {
		data.VectorPtrs[id] = unsafe.Pointer(&vec[0])
	}
	
	// Search for nearest neighbors at each layer
	ep := h.entryPoint.Load()
	maxL := int(h.maxLevel.Load())
	
	// Search from top layer down to level+1
	for lc := maxL; lc > level; lc-- {
		// Find closest point at this layer
		neighbors := h.searchLayerForInsert(vec, ep, 1, lc, data)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Insert at layers 0 to level
	for lc := level; lc >= 0; lc-- {
		// Find M nearest neighbors at this layer
		candidates := h.searchLayerForInsert(vec, ep, h.efConstruction, lc, data)
		
		// Select M neighbors
		m := h.mMax
		if lc > 0 {
			m = h.mMax0
		}
		neighbors := h.selectNeighbors(candidates, m, data)
		
		// Add connections
		for _, neighbor := range neighbors {
			h.addConnection(data, id, neighbor.ID, lc)
			h.addConnection(data, neighbor.ID, id, lc)
			
			// Prune neighbor if needed
			maxConn := h.mMax
			if lc > 0 {
				maxConn = h.mMax0
			}
			
			// Read count atomically
			count := atomic.LoadInt32(&data.Counts[lc][neighbor.ID])
			if int(count) > maxConn {
				h.pruneConnections(data, neighbor.ID, maxConn, lc)
			}
		}
		
		// Update entry point
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Update max level if needed
	if level > maxL {
		h.maxLevel.Store(int32(level))
		h.entryPoint.Store(id)
	}
	
	h.nodeCount.Add(1)
	return nil
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(query []float32, entryPoint uint32, ef int, layer int, data *GraphData) []Candidate {
	visited := NewBitset(int(h.nodeCount.Load()))
	candidates := NewFixedHeap(ef * 2)
	
	// Initialize with entry point
	entryDist := distanceSIMD(query, h.mustGetVectorFromData(data, entryPoint))
	candidates.Push(Candidate{ID: entryPoint, Dist: entryDist})
	visited.Set(entryPoint)
	
	results := make([]Candidate, 0, ef)
	results = append(results, Candidate{ID: entryPoint, Dist: entryDist})
	
	for candidates.Len() > 0 {
		curr, ok := candidates.Pop()
		if !ok {
			break
		}
		
		// Stop if current is farther than furthest result
		if len(results) >= ef && curr.Dist > results[len(results)-1].Dist {
			break
		}
		
		// Explore neighbors
		neighborCount := int(atomic.LoadInt32(&data.Counts[layer][curr.ID]))
		baseIdx := int(curr.ID) * MaxNeighbors
		
		for i := 0; i < neighborCount; i++ {
			neighborID := data.Neighbors[layer][baseIdx+i]
			
			if visited.IsSet(neighborID) {
				continue
			}
			visited.Set(neighborID)
			
			dist := distanceSIMD(query, h.mustGetVectorFromData(data, neighborID))
			
			if len(results) < ef || dist < results[len(results)-1].Dist {
				candidates.Push(Candidate{ID: neighborID, Dist: dist})
				results = append(results, Candidate{ID: neighborID, Dist: dist})
				
				// Keep results sorted and trimmed
				if len(results) > ef {
					// Simple bubble sort for small ef
					for i := len(results) - 1; i > 0; i-- {
						if results[i].Dist < results[i-1].Dist {
							results[i], results[i-1] = results[i-1], results[i]
						} else {
							break
						}
					}
					results = results[:ef]
				}
			}
		}
	}
	
	return results
}

// selectNeighbors selects the best M neighbors using the RobustPrune heuristic.
func (h *ArrowHNSW) selectNeighbors(candidates []Candidate, m int, data *GraphData) []Candidate {
	if len(candidates) <= m {
		return candidates
	}
	
	// RobustPrune heuristic: select diverse neighbors
	selected := make([]Candidate, 0, m)
	remaining := make([]Candidate, len(candidates))
	copy(remaining, candidates)
	
	for len(selected) < m && len(remaining) > 0 {
		// Find the closest remaining candidate
		bestIdx := 0
		bestDist := remaining[0].Dist
		
		for i := 1; i < len(remaining); i++ {
			if remaining[i].Dist < bestDist {
				bestDist = remaining[i].Dist
				bestIdx = i
			}
		}
		
		// Add to selected
		selected = append(selected, remaining[bestIdx])
		
		// Remove from remaining
		remaining[bestIdx] = remaining[len(remaining)-1]
		remaining = remaining[:len(remaining)-1]
		
		// Prune remaining candidates that are too close to the selected one
		if len(selected) < m && len(remaining) > 0 {
			selectedVec := h.mustGetVectorFromData(data, selected[len(selected)-1].ID)
			
			// Filter remaining to remove candidates too close to selected
			filtered := remaining[:0]
			for _, cand := range remaining {
				candVec := h.mustGetVectorFromData(data, cand.ID)
				distToSelected := distanceSIMD(candVec, selectedVec)
				
				// Keep candidate if it's not too close to the selected neighbor
				if distToSelected > cand.Dist {
					filtered = append(filtered, cand)
				}
			}
			remaining = filtered
		}
	}
	
	return selected
}

// addConnection adds a directed edge from source to target at the given layer.
func (h *ArrowHNSW) addConnection(data *GraphData, source, target uint32, layer int) {
	countAddr := &data.Counts[layer][source]
	count := int(atomic.LoadInt32(countAddr))
	baseIdx := int(source) * MaxNeighbors
	
	// Check if already connected
	for i := 0; i < count; i++ {
		if data.Neighbors[layer][baseIdx+i] == target {
			return // Already connected
		}
	}
	
	// Add connection if space available
	if count < MaxNeighbors {
		data.Neighbors[layer][baseIdx+count] = target
		atomic.StoreInt32(countAddr, int32(count+1))
	}
}

// pruneConnections reduces the number of connections to maxConn by removing the furthest neighbor.
func (h *ArrowHNSW) pruneConnections(data *GraphData, nodeID uint32, maxConn int, layer int) {
	h.pruneConnectionsInternal(data, nodeID, maxConn, layer, false)
}

// pruneConnectionsInternal is the internal implementation with skipReplenish flag
func (h *ArrowHNSW) pruneConnectionsInternal(data *GraphData, nodeID uint32, maxConn int, layer int, skipReplenish bool) {
	countAddr := &data.Counts[layer][nodeID]
	count := int(atomic.LoadInt32(countAddr))
	
	if count <= maxConn {
		return
	}
	
	// Get node's vector for distance calculations
	nodeVec := h.mustGetVectorFromData(data, nodeID)
	
	// Find the furthest neighbor
	var worstDist float32 = -1
	var worstIdx int = -1
	baseIdx := int(nodeID) * MaxNeighbors
	
	for i := 0; i < count; i++ {
		neighborID := data.Neighbors[layer][baseIdx+i]
		neighborVec := h.mustGetVectorFromData(data, neighborID)
		dist := distanceSIMD(nodeVec, neighborVec)
		
		if dist > worstDist {
			worstDist = dist
			worstIdx = i
		}
	}
	
	if worstIdx == -1 {
		// Fallback: remove last neighbor
		worstIdx = count - 1
	}
	
	// Get the worst neighbor ID before removing it
	worstNeighborID := data.Neighbors[layer][baseIdx+worstIdx]
	
	// Remove worst neighbor by shifting array
	for i := worstIdx; i < count-1; i++ {
		data.Neighbors[layer][baseIdx+i] = data.Neighbors[layer][baseIdx+i+1]
	}
	atomic.StoreInt32(countAddr, int32(count-1))
	
	// Delete backlink from the worst neighbor
	h.removeConnectionInternal(data, worstNeighborID, nodeID, layer, skipReplenish)
	
	// If we still have too many neighbors, prune again
	if int(atomic.LoadInt32(countAddr)) > maxConn {
		h.pruneConnectionsInternal(data, nodeID, maxConn, layer, skipReplenish)
	}
}

// removeConnection removes a directed edge from source to target at the given layer.
func (h *ArrowHNSW) removeConnection(data *GraphData, source, target uint32, layer int) {
	h.removeConnectionInternal(data, source, target, layer, false)
}

// removeConnectionInternal is the internal implementation with skipReplenish flag
func (h *ArrowHNSW) removeConnectionInternal(data *GraphData, source, target uint32, layer int, skipReplenish bool) {
	countAddr := &data.Counts[layer][source]
	count := int(atomic.LoadInt32(countAddr))
	baseIdx := int(source) * MaxNeighbors
	
	// Find and remove the connection
	for i := 0; i < count; i++ {
		if data.Neighbors[layer][baseIdx+i] == target {
			// Shift remaining neighbors
			for j := i; j < count-1; j++ {
				data.Neighbors[layer][baseIdx+j] = data.Neighbors[layer][baseIdx+j+1]
			}
			atomic.StoreInt32(countAddr, int32(count-1))
			
			// Replenish connections if we're below the target (unless we're in a pruning operation)
			if !skipReplenish {
				maxConn := h.mMax
				if layer > 0 {
					maxConn = h.mMax0
				}
				h.replenishConnections(data, source, maxConn, layer)
			}
			return
		}
	}
}

// replenishConnections restores connectivity by finding new neighbors through neighbors-of-neighbors.
func (h *ArrowHNSW) replenishConnections(data *GraphData, nodeID uint32, maxConn int, layer int) {
	count := int(atomic.LoadInt32(&data.Counts[layer][nodeID]))
	
	if count >= maxConn {
		return // Already at capacity
	}
	
	baseIdx := int(nodeID) * MaxNeighbors
	visited := make(map[uint32]bool)
	visited[nodeID] = true
	
	// Mark existing neighbors as visited
	for i := 0; i < count; i++ {
		visited[data.Neighbors[layer][baseIdx+i]] = true
	}
	
	// Explore neighbors of neighbors
	for i := 0; i < count && int(atomic.LoadInt32(&data.Counts[layer][nodeID])) < maxConn; i++ {
		neighborID := data.Neighbors[layer][baseIdx+i]
		neighborCount := int(atomic.LoadInt32(&data.Counts[layer][neighborID]))
		neighborBaseIdx := int(neighborID) * MaxNeighbors
		
		for j := 0; j < neighborCount && int(atomic.LoadInt32(&data.Counts[layer][nodeID])) < maxConn; j++ {
			candidateID := data.Neighbors[layer][neighborBaseIdx+j]
			
			// Skip if already visited or is the node itself
			if visited[candidateID] {
				continue
			}
			visited[candidateID] = true
			
			// Add this candidate as a new neighbor
			h.addConnection(data, nodeID, candidateID, layer)
			
			// Also add reverse connection
			h.addConnection(data, candidateID, nodeID, layer)
			
			// Prune if candidate now has too many connections
			candCount := int(atomic.LoadInt32(&data.Counts[layer][candidateID]))
			if candCount > maxConn {
				h.pruneConnectionsInternal(data, candidateID, maxConn, layer, true)
			}
		}
	}
}

// mustGetVectorFromData is a helper that uses the cached pointer from GraphData.
func (h *ArrowHNSW) mustGetVectorFromData(data *GraphData, id uint32) []float32 {
	// Optimization: Check for cached vector pointer
	if int(id) < len(data.VectorPtrs) {
		ptr := data.VectorPtrs[id]
		if ptr != nil && h.dims > 0 {
			return unsafe.Slice((*float32)(ptr), h.dims)
		}
	}
	
	// Fallback
	vec, err := h.getVector(id)
	if err != nil {
		return []float32{}
	}
	return vec
}

// mustGetVector is a helper that panics on error (for internal use).
// TODO: Deprecate or update to use current Load()
func (h *ArrowHNSW) mustGetVector(id uint32) []float32 {
	return h.mustGetVectorFromData(h.data.Load(), id)
}

// LevelGenerator generates random levels for new nodes.
type LevelGenerator struct {
	ml  float64
	rng *rand.Rand
	mu  sync.Mutex
}

// NewLevelGenerator creates a new level generator.
func NewLevelGenerator(ml float64) *LevelGenerator {
	return &LevelGenerator{
		ml:  ml,
		rng: rand.New(rand.NewSource(42)), // Fixed seed for reproducibility
	}
}

// Generate returns a random level using exponential decay.
func (lg *LevelGenerator) Generate() int {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	
	// Use exponential distribution: -ln(uniform(0,1)) * ml
	uniform := lg.rng.Float64()
	level := int(-math.Log(uniform) * lg.ml)
	
	// Cap at MaxLayers - 1
	if level >= MaxLayers {
		level = MaxLayers - 1
	}
	
	return level
}
