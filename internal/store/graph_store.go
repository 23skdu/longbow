package store

import (
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
}

type Edge struct {
	Subject   VectorID
	Predicate string
	Object    VectorID
	Weight    float32
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
	// Placeholder implementation for graph traversal
	return nil
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
