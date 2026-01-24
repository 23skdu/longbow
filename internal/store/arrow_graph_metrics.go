package store

import (
	"fmt"
	"strconv"
	"sync/atomic"

	prommetrics "github.com/23skdu/longbow/internal/metrics"
)

const MaxNeighbors = 64

// GraphMetrics holds statistics about the HNSW graph quality.
type GraphMetrics struct {
	TotalNodes          int
	TotalEdges          int
	AverageDegree       float64
	ZeroDegreeNodes     int
	ConnectedComponents int
	MaxComponentSize    int
	EstimatedDiameter   int         // BFS depth from entry point
	LevelDistribution   map[int]int // Nodes per level
}

// AnalyzeGraph computes quality metrics for the graph.
// Note: This is a heavy operation and should only be used for debugging/validation.
func (h *ArrowHNSW) AnalyzeGraph() GraphMetrics {
	data := h.data.Load()
	nodeCount := int(h.nodeCount.Load())

	metrics := GraphMetrics{
		TotalNodes:        nodeCount,
		LevelDistribution: make(map[int]int),
	}

	// 1. Degree Distribution (Layer 0)
	// We focus on Layer 0 as it contains all nodes and determines global connectivity.
	layer := 0
	visited := NewArrowBitset(nodeCount)
	componentSizes := make(map[int]int)

	for i := 0; i < nodeCount; i++ {
		cID := chunkID(uint32(i))
		cOff := chunkOffset(uint32(i))

		// Level Distribution
		levelsChunk := data.GetLevelsChunk(cID)
		if levelsChunk != nil {
			lvl := int(levelsChunk[cOff])
			metrics.LevelDistribution[lvl]++
		}

		// Safety check for nil chunks (lazy allocation or incomplete graph)
		countsChunk := data.GetCountsChunk(layer, cID)
		if countsChunk == nil {
			metrics.ZeroDegreeNodes++
			continue
		}

		count := int(atomic.LoadInt32(&countsChunk[cOff]))
		metrics.TotalEdges += count
		if count == 0 {
			metrics.ZeroDegreeNodes++
		}

		// Connected Components (BFS)
		if !visited.IsSet(int(i)) {
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

	// 3. Report Metrics to Prometheus
	dsName := "default"
	if h.dataset != nil {
		dsName = h.dataset.Name
	}
	prommetrics.HNSWDisconnectedComponents.WithLabelValues(dsName).Set(float64(metrics.ConnectedComponents))
	prommetrics.HNSWOrphanNodes.WithLabelValues(dsName).Set(float64(metrics.ZeroDegreeNodes))
	prommetrics.HNSWAverageDegree.WithLabelValues(dsName).Set(metrics.AverageDegree)
	prommetrics.HNSWMaxComponentSize.WithLabelValues(dsName).Set(float64(metrics.MaxComponentSize))
	prommetrics.HNSWEstimatedDiameter.WithLabelValues(dsName).Set(float64(metrics.EstimatedDiameter))

	// 4. Memory Attribution (Step 9)
	totalBytes := h.EstimateMemory()
	prommetrics.HNSWMemoryUsageBytes.WithLabelValues(dsName, "total").Set(float64(totalBytes))

	for lvl, count := range metrics.LevelDistribution {
		prommetrics.HNSWAvgLevelDistribution.WithLabelValues(dsName, strconv.Itoa(lvl)).Set(float64(count))
	}

	return metrics
}

// bfsComponentSize counts nodes in the component reachable from startNode.
func (h *ArrowHNSW) bfsComponentSize(data *GraphData, layer int, startNode uint32, visited *ArrowBitset) int {
	queue := []uint32{startNode}
	visited.Set(int(startNode))
	count := 0

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		count++

		// Iterate neighbors
		cID := chunkID(curr)
		cOff := chunkOffset(curr)

		countsChunk := data.GetCountsChunk(layer, cID)
		if countsChunk == nil {
			continue
		}
		neighborCount := atomic.LoadInt32(&countsChunk[cOff])

		baseIdx := int(cOff) * MaxNeighbors
		neighborsChunk := data.GetNeighborsChunk(layer, cID)
		if neighborsChunk == nil {
			continue
		}

		for i := 0; i < int(neighborCount); i++ {
			neighbor := neighborsChunk[baseIdx+i]
			if !visited.IsSet(int(neighbor)) {
				visited.Set(int(neighbor))
				queue = append(queue, neighbor)
			}
		}
	}
	return count
}

// bfsDiameter estimates diameter by running BFS from startNode and tracking max depth.
func (h *ArrowHNSW) bfsDiameter(data *GraphData, layer int, startNode uint32) int {
	nodeCount := int(h.nodeCount.Load())
	visited := NewArrowBitset(nodeCount)
	visited.Set(int(startNode))

	queue := []uint32{startNode}
	maxDepth := 0

	// Level-order traversal
	for len(queue) > 0 {
		levelSize := len(queue)
		maxDepth++

		for j := 0; j < levelSize; j++ {
			curr := queue[j]

			// Neighbors
			cID := chunkID(curr)
			cOff := chunkOffset(curr)

			countsChunk := data.GetCountsChunk(layer, cID)
			if countsChunk == nil {
				continue
			}
			neighborCount := int(atomic.LoadInt32(&countsChunk[cOff]))

			neighborsChunk := data.GetNeighborsChunk(layer, cID)
			if neighborsChunk == nil {
				continue
			}

			baseIdx := int(cOff) * MaxNeighbors
			for k := 0; k < neighborCount; k++ {
				neighbor := neighborsChunk[baseIdx+k]
				if !visited.IsSet(int(neighbor)) {
					visited.Set(int(neighbor))
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
