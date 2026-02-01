package store

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"golang.org/x/sync/errgroup"
)

// ParallelBFSStrategy implements concurrent Breadth-First Search.
// It is optimized for large-scale graph traversals using parallel worker pools.
type ParallelBFSStrategy struct {
	concurrency int
}

func NewParallelBFSStrategy(concurrency int) *ParallelBFSStrategy {
	if concurrency <= 0 {
		concurrency = 4
	}
	return &ParallelBFSStrategy{concurrency: concurrency}
}

func (s *ParallelBFSStrategy) Name() string {
	return "ParallelBFS"
}

func (s *ParallelBFSStrategy) FindPath(ctx context.Context, gn *GraphNavigator, query NavigatorQuery) (*NavigatorPath, error) {
	// Concurrent visited set.
	// For high performance, we might want a bitset, but sync.Map is safer for generic IDs.
	// Optimizing: Use a sharded map or atomic bitset if IDs are dense.
	// For now, sync.Map.
	visited := &sync.Map{}
	visited.Store(query.StartID, true)

	// Frontier Queue
	frontier := []uint32{query.StartID}

	// Paths reconstruction map: Node -> Parent
	parents := &sync.Map{}

	currentHops := 0
	visitedCount := atomic.Int64{}
	visitedCount.Add(1)
	maxFrontierSize := 0

	for len(frontier) > 0 && currentHops < query.MaxHops {
		// Prepare next frontier
		var nextFrontier []uint32
		var frontierMu sync.Mutex

		if len(frontier) > maxFrontierSize {
			maxFrontierSize = len(frontier)
		}

		// Use error group for parallel processing
		g, _ := errgroup.WithContext(ctx)
		g.SetLimit(s.concurrency)

		// Found flag
		var found atomic.Bool

		// Process current level
		for _, nodeID := range frontier {
			if found.Load() {
				break
			}

			if gn.searchConfig.MaxNodesVisited > 0 && visitedCount.Load() >= int64(gn.searchConfig.MaxNodesVisited) {
				break
			}

			// Check context before scheduling
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			id := nodeID // Capture

			g.Go(func() error {
				if found.Load() {
					return nil
				}

				// Check goal
				if id == query.TargetID {
					found.Store(true)
					return nil
				}

				// Early termination check
				if gn.searchConfig.EarlyTerminate && gn.shouldTerminateEarly(id, query.TargetID, currentHops) {
					gn.metrics.EarlyTerminations.Inc()
					return nil
				}

				// Get Neighbors
				// Note: getNeighbors does a read-lock on gn.mu but GraphData internal locks are fine.
				// For truly lock-free, we rely on GraphData's seqlock.
				neighbors, ok := gn.getNeighbors(id)
				if !ok {
					return nil
				}

				var localNext []uint32
				for _, neighbor := range neighbors {
					if _, seen := visited.LoadOrStore(neighbor, true); !seen {
						parents.Store(neighbor, id)
						localNext = append(localNext, neighbor)
						visitedCount.Add(1)

						if neighbor == query.TargetID {
							found.Store(true)
						}
					}
				}

				if len(localNext) > 0 {
					frontierMu.Lock()
					nextFrontier = append(nextFrontier, localNext...)
					frontierMu.Unlock()
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		// Check found
		if found.Load() {
			// Reconstruct path
			path := []uint32{query.TargetID}
			curr := query.TargetID
			for curr != query.StartID {
				p, ok := parents.Load(curr)
				if !ok {
					// Should not happen if logic is correct
					break
				}
				curr = p.(uint32)
				path = append([]uint32{curr}, path...)
			}

			return &NavigatorPath{
				StartID: query.StartID,
				EndID:   query.TargetID,
				Path:    path,
				Hops:    len(path) - 1,
				Found:   true,
			}, nil
		}

		frontier = nextFrontier
		currentHops++
	}

	// Finalize metrics
	metrics.GraphNavigationNodesVisitedTotal.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(visitedCount.Load()))
	metrics.GraphNavigationFrontierMaxSize.WithLabelValues(gn.datasetName, s.Name()).Observe(float64(maxFrontierSize))

	return &NavigatorPath{
		StartID: query.StartID,
		EndID:   query.TargetID,
		Path:    []uint32{},
		Hops:    0,
		Found:   false,
	}, nil
}
