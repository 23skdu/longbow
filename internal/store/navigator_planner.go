package store

// QueryPlanner determines the best navigation strategy for a given query.
type QueryPlanner struct {
	bfs   NavigationStrategy
	astar NavigationStrategy
}

func NewQueryPlanner() *QueryPlanner {
	return &QueryPlanner{
		bfs:   &BFSStrategy{},
		astar: &AStarStrategy{},
	}
}

// Plan selects the appropriate strategy based on the query.
func (p *QueryPlanner) Plan(query NavigatorQuery) NavigationStrategy {
	// If searching for a specific target, use A* for directed heuristic search.
	// BFS is used if we want purely shortest path by hops without geometric awareness roughly,
	// but A* with proper heuristic is better.
	if query.TargetID != query.StartID {
		return p.astar
	}

	return p.bfs
}
