package store

// QueryPlanner determines the best navigation strategy for a given query.
type QueryPlanner struct {
	bfs         NavigationStrategy
	astar       NavigationStrategy
	parallelBfs NavigationStrategy
}

func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{
		bfs:         &BFSStrategy{},
		astar:       &AStarStrategy{},
		parallelBfs: NewParallelBFSStrategy(8), // Default to 8 workers
	}
}

// Plan selects the appropriate strategy based on the query.
func (p *QueryPlanner) Plan(query NavigatorQuery) NavigationStrategy {
	// Heuristic:
	// 1. Directed search -> A*
	// 2. Large exploration (High hop count or no target) -> ParallelBFS
	// 3. Small exploration -> BFS

	if query.TargetID != query.StartID {
		return p.astar
	}

	// If MaxHops is large or unspecified (exploration), use parallel.
	if query.MaxHops > 4 {
		return p.parallelBfs
	}

	return p.bfs
}
