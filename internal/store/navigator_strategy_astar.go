package store

import (
	"container/heap"
	"context"

	"github.com/23skdu/longbow/internal/metrics"
)

// AStarStrategy implements A* search algorithm.
type AStarStrategy struct{}

func (s *AStarStrategy) Name() string {
	return "AStar"
}

func (s *AStarStrategy) FindPath(ctx context.Context, gn *GraphNavigator, query NavigatorQuery) (*NavigatorPath, error) {
	// A* Implementation
	// Priority Queue Item
	startDist := gn.calculateDistance(query.StartID, query.TargetID)

	pq := &aStarPriorityQueue{}
	heap.Init(pq)

	startItem := &aStarItem{
		id:        query.StartID,
		gScore:    0,
		fScore:    startDist,
		hops:      0,
		path:      []uint32{query.StartID},
		distances: []float32{},
	}
	heap.Push(pq, startItem)

	visited := make(map[uint32]float32) // id -> gScore (hops)
	visited[query.StartID] = 0
	maxFrontierSize := 0

	for pq.Len() > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if pq.Len() > maxFrontierSize {
			maxFrontierSize = pq.Len()
		}
		current := heap.Pop(pq).(*aStarItem)

		if current.id == query.TargetID {
			metrics.GraphNavigationNodesVisitedTotal.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(len(visited)))
			metrics.GraphNavigationFrontierMaxSize.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(maxFrontierSize))
			return &NavigatorPath{
				StartID:   query.StartID,
				EndID:     query.TargetID,
				Path:      current.path,
				Distances: current.distances,
				Hops:      current.hops,
				Found:     true,
			}, nil
		}

		if current.hops >= query.MaxHops {
			continue
		}

		if gn.searchConfig.MaxNodesVisited > 0 && len(visited) >= gn.searchConfig.MaxNodesVisited {
			continue
		}

		// Early termination check
		if gn.searchConfig.EarlyTerminate && gn.shouldTerminateEarly(current.id, query.TargetID, current.hops) {
			gn.metrics.EarlyTerminations.Inc()
			continue
		}

		neighbors, ok := gn.getNeighbors(current.id)
		if !ok {
			continue
		}

		for _, neighbor := range neighbors {
			newHops := current.hops + 1

			// Check if we found a better path to neighbor
			if existingG, seen := visited[neighbor]; seen && float32(newHops) >= existingG {
				continue
			}
			visited[neighbor] = float32(newHops)

			// Calculate stats
			distToNeighbor := gn.calculateDistance(current.id, neighbor)

			newPath := make([]uint32, len(current.path)+1)
			copy(newPath, current.path)
			newPath[len(current.path)] = neighbor

			newDistances := make([]float32, len(current.distances)+1)
			copy(newDistances, current.distances)
			newDistances[len(current.distances)] = distToNeighbor

			// Heuristic: Distance to target
			hScore := gn.calculateDistance(neighbor, query.TargetID)
			gScore := float32(newHops) // Cost is hops

			// Optimization: scale hScore to match gScore magnitude?
			// Distances are float32 (e.g. 0.0-2.0 for Cosine/L2 normalized). Hops are integers.
			// Currently simple addition: f = hops + distance.
			// This biases towards fewer hops heavily if distance is small.
			fScore := gScore + hScore

			item := &aStarItem{
				id:        neighbor,
				gScore:    gScore,
				fScore:    fScore,
				hops:      newHops,
				path:      newPath,
				distances: newDistances,
			}
			heap.Push(pq, item)
		}
	}

	metrics.GraphNavigationNodesVisitedTotal.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(len(visited)))
	metrics.GraphNavigationFrontierMaxSize.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(maxFrontierSize))

	return &NavigatorPath{
		StartID: query.StartID,
		EndID:   query.TargetID,
		Path:    []uint32{},
		Hops:    0,
		Found:   false,
	}, nil
}

// Priority Queue Implementation

type aStarItem struct {
	id        uint32
	gScore    float32 // Cost from start (hops)
	fScore    float32 // gScore + hScore
	hops      int
	path      []uint32
	distances []float32
	index     int
}

type aStarPriorityQueue []*aStarItem

func (pq aStarPriorityQueue) Len() int { return len(pq) }

func (pq aStarPriorityQueue) Less(i, j int) bool {
	// Lower fScore has higher priority (min-heap)
	return pq[i].fScore < pq[j].fScore
}

func (pq aStarPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *aStarPriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*aStarItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *aStarPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
