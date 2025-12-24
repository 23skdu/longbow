package hnsw2

import (
	"math"
	"math/rand"
	"sync"
)

// Insert adds a new vector to the HNSW graph.
// The vector is identified by its VectorID and assigned a random level.
func (h *ArrowHNSW) Insert(id uint32, level int) error {
	h.nodeMu.Lock()
	defer h.nodeMu.Unlock()
	
	// Ensure we have enough capacity
	if int(id) >= len(h.nodes) {
		// Expand nodes slice
		newNodes := make([]GraphNode, id+1)
		copy(newNodes, h.nodes)
		h.nodes = newNodes
	}
	
	// Initialize the new node
	node := &h.nodes[id]
	node.ID = id
	node.Level = uint8(level)
	
	// If this is the first node, make it the entry point
	if h.nodeCount == 0 {
		h.entryPoint = id
		h.maxLevel = level
		h.nodeCount++
		return nil
	}
	
	// Get vector for distance calculations
	vec, err := h.getVector(id)
	if err != nil {
		return err
	}
	
	// Search for nearest neighbors at each layer
	ep := h.entryPoint
	
	// Search from top layer down to level+1
	for lc := h.maxLevel; lc > level; lc-- {
		// Find closest point at this layer
		neighbors := h.searchLayerForInsert(vec, ep, 1, lc)
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Insert at layers 0 to level
	for lc := level; lc >= 0; lc-- {
		// Find M nearest neighbors at this layer
		candidates := h.searchLayerForInsert(vec, ep, h.efConstruction, lc)
		
		// Select M neighbors using heuristic
		m := h.mMax
		if lc > 0 {
			m = h.mMax0
		}
		neighbors := h.selectNeighbors(candidates, m)
		
		// Add bidirectional links
		for _, neighbor := range neighbors {
			// Add neighbor to new node
			h.addConnection(id, neighbor.ID, lc)
			
			// Add new node to neighbor
			h.addConnection(neighbor.ID, id, lc)
			
			// Prune neighbor's connections if needed
			neighborNode := &h.nodes[neighbor.ID]
			maxConn := h.mMax
			if lc > 0 {
				maxConn = h.mMax0
			}
			if int(neighborNode.NeighborCounts[lc]) > maxConn {
				h.pruneConnections(neighbor.ID, maxConn, lc)
			}
		}
		
		// Update entry point for next layer
		if len(neighbors) > 0 {
			ep = neighbors[0].ID
		}
	}
	
	// Update entry point if new node has higher level
	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPoint = id
	}
	
	h.nodeCount++
	return nil
}

// searchLayerForInsert performs search during insertion.
// Returns candidates sorted by distance.
func (h *ArrowHNSW) searchLayerForInsert(query []float32, entryPoint uint32, ef int, layer int) []Candidate {
	visited := NewBitset(len(h.nodes))
	candidates := NewFixedHeap(ef * 2)
	
	// Initialize with entry point
	entryDist := distanceSIMD(query, h.mustGetVector(entryPoint))
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
		node := &h.nodes[curr.ID]
		neighborCount := int(node.NeighborCounts[layer])
		
		for i := 0; i < neighborCount; i++ {
			neighborID := node.Neighbors[layer][i]
			
			if visited.IsSet(neighborID) {
				continue
			}
			visited.Set(neighborID)
			
			dist := distanceSIMD(query, h.mustGetVector(neighborID))
			
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
// This maintains diversity in the graph by preferring neighbors that are not already
// well-connected to other selected neighbors.
func (h *ArrowHNSW) selectNeighbors(candidates []Candidate, m int) []Candidate {
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
		// This maintains diversity
		if len(selected) < m && len(remaining) > 0 {
			selectedVec := h.mustGetVector(selected[len(selected)-1].ID)
			
			// Filter remaining to remove candidates too close to selected
			filtered := remaining[:0]
			for _, cand := range remaining {
				candVec := h.mustGetVector(cand.ID)
				distToSelected := distanceSIMD(candVec, selectedVec)
				
				// Keep candidate if it's not too close to the selected neighbor
				// (i.e., distance to selected > distance to query)
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
func (h *ArrowHNSW) addConnection(source, target uint32, layer int) {
	node := &h.nodes[source]
	count := int(node.NeighborCounts[layer])
	
	// Check if already connected
	for i := 0; i < count; i++ {
		if node.Neighbors[layer][i] == target {
			return // Already connected
		}
	}
	
	// Add connection if space available
	if count < MaxNeighbors {
		node.Neighbors[layer][count] = target
		node.NeighborCounts[layer]++
	}
}

// pruneConnections reduces the number of connections to maxConn by removing the furthest neighbor.
// This maintains better graph quality by keeping closer neighbors.
func (h *ArrowHNSW) pruneConnections(nodeID uint32, maxConn int, layer int) {
	h.pruneConnectionsInternal(nodeID, maxConn, layer, false)
}

// pruneConnectionsInternal is the internal implementation with skipReplenish flag
func (h *ArrowHNSW) pruneConnectionsInternal(nodeID uint32, maxConn int, layer int, skipReplenish bool) {
	node := &h.nodes[nodeID]
	count := int(node.NeighborCounts[layer])
	
	if count <= maxConn {
		return
	}
	
	// Get node's vector for distance calculations
	nodeVec := h.mustGetVector(nodeID)
	
	// Find the furthest neighbor
	var worstDist float32 = -1
	var worstIdx int = -1
	
	for i := 0; i < count; i++ {
		neighborID := node.Neighbors[layer][i]
		neighborVec := h.mustGetVector(neighborID)
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
	worstNeighborID := node.Neighbors[layer][worstIdx]
	
	// Remove worst neighbor by shifting array
	for i := worstIdx; i < count-1; i++ {
		node.Neighbors[layer][i] = node.Neighbors[layer][i+1]
	}
	node.NeighborCounts[layer]--
	
	// Delete backlink from the worst neighbor
	h.removeConnectionInternal(worstNeighborID, nodeID, layer, skipReplenish)
	
	// If we still have too many neighbors, prune again
	if int(node.NeighborCounts[layer]) > maxConn {
		h.pruneConnectionsInternal(nodeID, maxConn, layer, skipReplenish)
	}
}

// removeConnection removes a directed edge from source to target at the given layer.
func (h *ArrowHNSW) removeConnection(source, target uint32, layer int) {
	h.removeConnectionInternal(source, target, layer, false)
}

// removeConnectionInternal is the internal implementation with skipReplenish flag
func (h *ArrowHNSW) removeConnectionInternal(source, target uint32, layer int, skipReplenish bool) {
	node := &h.nodes[source]
	count := int(node.NeighborCounts[layer])
	
	// Find and remove the connection
	for i := 0; i < count; i++ {
		if node.Neighbors[layer][i] == target {
			// Shift remaining neighbors
			for j := i; j < count-1; j++ {
				node.Neighbors[layer][j] = node.Neighbors[layer][j+1]
			}
			node.NeighborCounts[layer]--
			
			// Replenish connections if we're below the target (unless we're in a pruning operation)
			if !skipReplenish {
				maxConn := h.mMax
				if layer > 0 {
					maxConn = h.mMax0
				}
				h.replenishConnections(source, maxConn, layer)
			}
			return
		}
	}
}

// replenishConnections restores connectivity by finding new neighbors through neighbors-of-neighbors.
// This is called when a node loses a connection during pruning to maintain graph connectivity.
func (h *ArrowHNSW) replenishConnections(nodeID uint32, maxConn int, layer int) {
	node := &h.nodes[nodeID]
	count := int(node.NeighborCounts[layer])
	
	if count >= maxConn {
		return // Already at capacity
	}
	
	// Try to find new neighbors through existing neighbors
	// This is a simplified version of coder/hnsw's replenish
	visited := make(map[uint32]bool)
	visited[nodeID] = true
	
	// Mark existing neighbors as visited
	for i := 0; i < count; i++ {
		visited[node.Neighbors[layer][i]] = true
	}
	
	// Explore neighbors of neighbors
	for i := 0; i < count && int(node.NeighborCounts[layer]) < maxConn; i++ {
		neighborID := node.Neighbors[layer][i]
		neighborNode := &h.nodes[neighborID]
		neighborCount := int(neighborNode.NeighborCounts[layer])
		
		for j := 0; j < neighborCount && int(node.NeighborCounts[layer]) < maxConn; j++ {
			candidateID := neighborNode.Neighbors[layer][j]
			
			// Skip if already visited or is the node itself
			if visited[candidateID] {
				continue
			}
			visited[candidateID] = true
			
			// Add this candidate as a new neighbor
			h.addConnection(nodeID, candidateID, layer)
			
			// Also add reverse connection
			h.addConnection(candidateID, nodeID, layer)
			
			// Prune if candidate now has too many connections
			candidateNode := &h.nodes[candidateID]
			if int(candidateNode.NeighborCounts[layer]) > maxConn {
				h.pruneConnectionsInternal(candidateID, maxConn, layer, true)
			}
		}
	}
}

// mustGetVector is a helper that panics on error (for internal use).
func (h *ArrowHNSW) mustGetVector(id uint32) []float32 {
	vec, err := h.getVector(id)
	if err != nil {
		// For now, return empty vector
		// TODO: Better error handling
		return []float32{}
	}
	return vec
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
