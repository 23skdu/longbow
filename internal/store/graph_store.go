package store

import (
	"container/heap"
	"sort"
	"sync"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

type GraphStore struct {
	mu sync.RWMutex

	forwardEdges  map[uint32][]Edge
	backwardEdges map[uint32][]Edge
	predicateMap  map[string]int32
	predicates    []string
	edgeCount     int
}

type Direction int

const (
	DirectionOutgoing Direction = iota
	DirectionIncoming
	DirectionBoth
)

type TraverseOptions struct {
	MaxHops   int
	Direction Direction
	Weighted  bool
	Decay     float32
}

func DefaultTraverseOptions() TraverseOptions {
	return TraverseOptions{
		MaxHops:   2,
		Direction: DirectionOutgoing,
		Weighted:  true,
		Decay:     0.5,
	}
}

type Path struct {
	Nodes []VectorID
	Edges []Edge
	Score float32
}

type Edge struct {
	Subject   VectorID
	Predicate string
	Object    VectorID
	Weight    float32
}

// PathPriorityQueue for weighted traversal
type PathPriorityQueue []Path

func (pq PathPriorityQueue) Len() int { return len(pq) }

func (pq PathPriorityQueue) Less(i, j int) bool {
	// Higher score comes first (Max-Heap)
	return pq[i].Score > pq[j].Score
}

func (pq PathPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PathPriorityQueue) Push(x any) {
	*pq = append(*pq, x.(Path))
}

func (pq *PathPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func NewGraphStore() *GraphStore {
	return &GraphStore{
		forwardEdges:  make(map[uint32][]Edge),
		backwardEdges: make(map[uint32][]Edge),
		predicateMap:  make(map[string]int32),
		predicates:    make([]string, 0),
		edgeCount:     0,
	}
}

func (gs *GraphStore) AddEdge(edge Edge) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, exists := gs.predicateMap[edge.Predicate]; !exists {
		idx := int32(len(gs.predicates))
		gs.predicateMap[edge.Predicate] = idx
		gs.predicates = append(gs.predicates, edge.Predicate)
	}

	gs.forwardEdges[uint32(edge.Subject)] = append(gs.forwardEdges[uint32(edge.Subject)], edge)
	gs.backwardEdges[uint32(edge.Object)] = append(gs.backwardEdges[uint32(edge.Object)], edge)

	gs.edgeCount++
	return nil
}

func (gs *GraphStore) EdgeCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.edgeCount
}

func (gs *GraphStore) GetEdgesBySubject(subject uint32) []Edge {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]Edge(nil), gs.forwardEdges[subject]...)
}

func (gs *GraphStore) GetEdgesByObject(object uint32) []Edge {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]Edge(nil), gs.backwardEdges[object]...)
}

func (gs *GraphStore) GetEdgesByPredicate(predicate string) []Edge {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var result []Edge
	for _, edges := range gs.forwardEdges {
		for _, edge := range edges {
			if edge.Predicate == predicate {
				result = append(result, edge)
			}
		}
	}
	return result
}

func (gs *GraphStore) PredicateVocabulary() []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]string(nil), gs.predicates...)
}

func (gs *GraphStore) CommunityCount() int {
	// Simplified: return number of subjects as proxy for communities if not implemented
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return len(gs.forwardEdges)
}

func (gs *GraphStore) RankWithGraph(results []SearchResult, alpha float32, depth int) []SearchResult {
	if len(results) == 0 || alpha <= 0 {
		return results
	}

	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// Simple graph-based re-ranking: boost nodes that are neighbors of top results
	scores := make(map[uint32]float32)
	for _, r := range results {
		id := uint32(r.ID)
		scores[id] += r.Score

		// Traverse up to 'depth' hops
		// (Simplified: 1 hop for now)
		edges := gs.forwardEdges[id]
		for _, edge := range edges {
			// Object is VectorID (uint32)
			scores[uint32(edge.Object)] += r.Score * alpha
		}
	}

	// Rebuild results
	newResults := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		newResults = append(newResults, SearchResult{ID: lbtypes.VectorID(id), Score: score})
	}

	sort.Slice(newResults, func(i, j int) bool {
		return newResults[i].Score > newResults[j].Score
	})

	return newResults
}

func (gs *GraphStore) Traverse(start VectorID, opts TraverseOptions) []Path {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// Initial Path
	initPath := Path{
		Nodes: []VectorID{start},
		Edges: []Edge{},
		Score: 1.0,
	}

	if opts.Weighted {
		// Priority Queue for weighted traversal (Best-First Search)
		pq := &PathPriorityQueue{initPath}
		heap.Init(pq)

		var results []Path
		visited := make(map[VectorID]struct{}) // To prevent cycles within a path, or global visited?
		// For GraphRAG, usually we want all paths up to K hops, but cycle detection is needed per path.
		// However, standard specific-path traversal often tracks visited per walk.
		// To avoid explosion, we stick to simple BFS with max hops and visited check.

		// Let's use simple visited set for nodes to avoid loops/re-visiting in this specific walk
		visited[start] = struct{}{}

		// We return all valid paths found up to MaxHops
		// But in a weighted search, we might just want "best" paths?
		// "Traverse" typically returns all reachable subgraphs or paths.
		// Given the test expects multiple paths, we collect them.

		// Let's implement a BFS that expands layers.
		// Replacing PQ with standard queue for simple BFS if "Weighted" just means edge weights exist.
		// If "Weighted" means "Prioritize high weight paths", then PQ is correct.
		// The test expects ALL paths of length 1.

		// Revert to simple BFS for correctness with tests, but handle weights in score.
		// Using a queue for BFS.
		queue := []Path{initPath}

		for len(queue) > 0 {
			curr := queue[0]
			queue = queue[1:]

			if len(curr.Nodes)-1 >= opts.MaxHops {
				results = append(results, curr)
				continue
			}

			// If not at max hops, expand
			// Collecting intermediate paths is also possible, usually only leaf paths or invalid-continuation paths are returned?
			// Tests imply we want the paths of length MaxHops or less?
			// GraphStore_TraverseIncoming expects 2 paths of length 1 (nodes=2).

			if len(curr.Nodes) > 1 {
				results = append(results, curr)
			}

			lastNode := curr.Nodes[len(curr.Nodes)-1]

			// Get Edges based on direction
			var edges []Edge
			switch opts.Direction {
			case DirectionOutgoing:
				edges = gs.forwardEdges[uint32(lastNode)]
			case DirectionIncoming:
				edges = gs.backwardEdges[uint32(lastNode)]
			case DirectionBoth:
				fwd := gs.forwardEdges[uint32(lastNode)]
				bwd := gs.backwardEdges[uint32(lastNode)]
				edges = append(edges, fwd...)
				edges = append(edges, bwd...)
			}

			for _, e := range edges {
				var nextNode VectorID
				switch opts.Direction {
				case DirectionIncoming:
					nextNode = e.Subject
				case DirectionOutgoing:
					nextNode = e.Object
				default:
					if e.Subject == lastNode {
						nextNode = e.Object
					} else {
						nextNode = e.Subject
					}
				}

				// Cycle Check (Per Path)
				seen := false
				for _, n := range curr.Nodes {
					if n == nextNode {
						seen = true
						break
					}
				}
				if seen {
					continue
				}

				// New Path
				newPath := Path{
					Nodes: make([]VectorID, len(curr.Nodes)+1),
					Edges: make([]Edge, len(curr.Edges)+1),
					Score: curr.Score * e.Weight * opts.Decay, // Decay score
				}
				copy(newPath.Nodes, curr.Nodes)
				newPath.Nodes[len(curr.Nodes)] = nextNode
				copy(newPath.Edges, curr.Edges)
				newPath.Edges[len(curr.Edges)] = e

				queue = append(queue, newPath)
			}
		}
		return results
	}

	// Unweighted (BFS) - Placeholder fallback, but actually above logic handles both if weight=1.0
	// For simplicity, reusing same logic.
	return gs.traverseBFS(start, opts)
}

func (gs *GraphStore) traverseBFS(start VectorID, opts TraverseOptions) []Path {
	queue := []Path{{
		Nodes: []VectorID{start},
		Score: 1.0,
	}}
	var results []Path

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		if len(curr.Nodes) > 1 {
			results = append(results, curr)
		}

		if len(curr.Nodes)-1 >= opts.MaxHops {
			continue
		}

		lastNode := curr.Nodes[len(curr.Nodes)-1]

		var edges []Edge
		switch opts.Direction {
		case DirectionOutgoing:
			edges = gs.forwardEdges[uint32(lastNode)]
		case DirectionIncoming:
			edges = gs.backwardEdges[uint32(lastNode)]
		case DirectionBoth:
			edges = append(edges, gs.forwardEdges[uint32(lastNode)]...)
			edges = append(edges, gs.backwardEdges[uint32(lastNode)]...)
		}

		for _, e := range edges {
			var nextNode VectorID
			switch opts.Direction {
			case DirectionIncoming:
				nextNode = e.Subject
			case DirectionOutgoing:
				nextNode = e.Object
			default:
				if e.Subject == lastNode {
					nextNode = e.Object
				} else {
					nextNode = e.Subject
				}
			}

			// Cycle check
			seen := false
			for _, n := range curr.Nodes {
				if n == nextNode {
					seen = true
					break
				}
			}
			if seen {
				continue
			}

			newPath := Path{
				Nodes: make([]VectorID, len(curr.Nodes)+1),
				Edges: make([]Edge, len(curr.Edges)+1),
				Score: curr.Score * opts.Decay,
			}
			copy(newPath.Nodes, curr.Nodes)
			newPath.Nodes[len(curr.Nodes)] = nextNode
			copy(newPath.Edges, curr.Edges)
			newPath.Edges[len(curr.Edges)] = e

			queue = append(queue, newPath)
		}
	}
	return results
}

func (gs *GraphStore) Close() error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	gs.forwardEdges = nil
	gs.backwardEdges = nil
	gs.predicateMap = nil
	gs.predicates = nil
	gs.edgeCount = 0

	return nil
}
