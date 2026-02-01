package store

import (
	"context"

	"github.com/23skdu/longbow/internal/metrics"
)

// NavigationStrategy defines the interface for different pathfinding algorithms.
type NavigationStrategy interface {
	// FindPath executes the search strategy.
	FindPath(ctx context.Context, gn *GraphNavigator, query NavigatorQuery) (*NavigatorPath, error)
	// Name returns the identifier of the strategy.
	Name() string
}

// BFSStrategy implements Breadth-First Search.
type BFSStrategy struct{}

func (s *BFSStrategy) Name() string {
	return "BFS"
}

func (s *BFSStrategy) FindPath(ctx context.Context, gn *GraphNavigator, query NavigatorQuery) (*NavigatorPath, error) {
	type queueItem struct {
		id   uint32
		hops int
		path []uint32
	}

	queue := []queueItem{{id: query.StartID, hops: 0, path: []uint32{query.StartID}}}
	visited := make(map[uint32]bool)
	maxFrontierSize := 0

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if len(queue) > maxFrontierSize {
			maxFrontierSize = len(queue)
		}
		current := queue[0]
		queue = queue[1:]

		if current.id == query.TargetID {
			metrics.GraphNavigationNodesVisitedTotal.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(len(visited)))
			metrics.GraphNavigationFrontierMaxSize.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(maxFrontierSize))
			return &NavigatorPath{
				StartID: query.StartID,
				EndID:   query.TargetID,
				Path:    current.path,
				Hops:    current.hops,
				Found:   true,
			}, nil
		}

		if current.hops >= query.MaxHops {
			continue
		}

		if gn.searchConfig.MaxNodesVisited > 0 && len(visited) >= gn.searchConfig.MaxNodesVisited {
			continue
		}

		if gn.searchConfig.EarlyTerminate && gn.shouldTerminateEarly(current.id, query.TargetID, current.hops) {
			gn.metrics.EarlyTerminations.Inc()
			continue
		}

		if visited[current.id] {
			continue
		}
		visited[current.id] = true

		neighbors, ok := gn.getNeighbors(current.id)
		if !ok {
			continue
		}

		for _, neighbor := range neighbors {
			if !visited[neighbor] {
				newPath := make([]uint32, len(current.path)+1)
				copy(newPath, current.path)
				newPath[len(current.path)] = neighbor

				queue = append(queue, queueItem{
					id:   neighbor,
					hops: current.hops + 1,
					path: newPath,
				})
			}
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
