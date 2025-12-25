package hnsw2

import (
	"fmt"
	"sync/atomic"
)

// GraphMetrics holds statistics about the HNSW graph quality.
type GraphMetrics struct {
	TotalNodes          int
	TotalEdges          int
	AverageDegree       float64
	ZeroDegreeNodes     int
	ConnectedComponents int
	MaxComponentSize    int
	EstimatedDiameter   int // BFS depth from entry point
}

// AnalyzeGraph computes quality metrics for the graph.
// Note: This is a heavy operation and should only be used for debugging/validation.
func (h *ArrowHNSW) AnalyzeGraph() GraphMetrics {
	data := h.data.Load()
	nodeCount := int(h.nodeCount.Load())
	
	metrics := GraphMetrics{
		TotalNodes: nodeCount,
	}

	// 1. Degree Distribution (Layer 0)
	// We focus on Layer 0 as it contains all nodes and determines global connectivity.
	layer := 0
	visited := NewBitset(nodeCount)
	componentSizes := make(map[int]int)
	
	for i := 0; i < nodeCount; i++ {
		count := int(atomic.LoadInt32(&data.Counts[layer][i]))
		metrics.TotalEdges += count
		if count == 0 {
			metrics.ZeroDegreeNodes++
		}
		
		// Connected Components (BFS)
		if !visited.IsSet(uint32(i)) {
			metrics.ConnectedComponents++
			size := h.bfsComponentSize(data, layer, uint32(i), visited)
			componentSizes[metrics.ConnectedComponents] = size
			if size > metrics.MaxComponentSize {
				metrics.MaxComponentSize = size
			}
		}
	}
	
	if nodeCount > 0 {
		metrics.AverageDegree = float64(metrics.TotalEdges) / float64(nodeCount)
	}

	// 2. Estimate Diameter (BFS from Entry Point)
	// Only if entry point is set
	ep := h.entryPoint.Load()
	if nodeCount > 0 {
		metrics.EstimatedDiameter = h.bfsDiameter(data, layer, ep)
	}

	return metrics
}

// bfsComponentSize counts nodes in the component reachable from startNode.
func (h *ArrowHNSW) bfsComponentSize(data *GraphData, layer int, startNode uint32, visited *Bitset) int {
	queue := []uint32{startNode}
	visited.Set(startNode)
	count := 0
	
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		count++
		
		// Iterate neighbors
		neighborCount := int(atomic.LoadInt32(&data.Counts[layer][curr]))
		baseIdx := int(curr) * MaxNeighbors
		for i := 0; i < neighborCount; i++ {
			neighbor := data.Neighbors[layer][baseIdx+i]
			if !visited.IsSet(neighbor) {
				visited.Set(neighbor)
				queue = append(queue, neighbor)
			}
		}
	}
	return count
}

// bfsDiameter estimates diameter by running BFS from startNode and tracking max depth.
func (h *ArrowHNSW) bfsDiameter(data *GraphData, layer int, startNode uint32) int {
	nodeCount := int(h.nodeCount.Load())
	visited := NewBitset(nodeCount)
	visited.Set(startNode)
	
	queue := []uint32{startNode}
	maxDepth := 0
	
	// Level-order traversal
	for len(queue) > 0 {
		levelSize := len(queue)
		maxDepth++
		
		for j := 0; j < levelSize; j++ {
			curr := queue[j]
			
			// Neighbors
			neighborCount := int(atomic.LoadInt32(&data.Counts[layer][curr]))
			baseIdx := int(curr) * MaxNeighbors
			for k := 0; k < neighborCount; k++ {
				neighbor := data.Neighbors[layer][baseIdx+k]
				if !visited.IsSet(neighbor) {
					visited.Set(neighbor)
					queue = append(queue, neighbor)
				}
			}
		}
		queue = queue[levelSize:]
	}
	
	return maxDepth - 1 // Depth 1 is just the start node
}

func (m GraphMetrics) String() string {
	return fmt.Sprintf(
		"Nodes: %d, Edges: %d, AvgDegree: %.2f, ZeroDeg: %d, Components: %d, MaxComp: %d (%.2f%%), EstDiameter: %d",
		m.TotalNodes, m.TotalEdges, m.AverageDegree, m.ZeroDegreeNodes,
		m.ConnectedComponents, m.MaxComponentSize,
		float64(m.MaxComponentSize)/float64(m.TotalNodes)*100.0,
		m.EstimatedDiameter,
	)
}
