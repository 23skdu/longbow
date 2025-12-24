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
	entryDist := l2Distance(query, h.mustGetVector(entryPoint))
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
			
			dist := l2Distance(query, h.mustGetVector(neighborID))
			
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

// selectNeighbors selects the best M neighbors using a heuristic.
// For now, uses simple nearest neighbor selection.
func (h *ArrowHNSW) selectNeighbors(candidates []Candidate, m int) []Candidate {
	if len(candidates) <= m {
		return candidates
	}
	return candidates[:m]
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

// pruneConnections reduces the number of connections to maxConn.
func (h *ArrowHNSW) pruneConnections(nodeID uint32, maxConn int, layer int) {
	node := &h.nodes[nodeID]
	count := int(node.NeighborCounts[layer])
	
	if count <= maxConn {
		return
	}
	
	// Simple pruning: keep first maxConn neighbors
	// TODO: Implement heuristic pruning (keep diverse neighbors)
	node.NeighborCounts[layer] = uint8(maxConn)
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
