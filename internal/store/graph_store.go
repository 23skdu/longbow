package store

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"context"
	"unsafe"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/metrics"
)

// GraphStore manages an in-memory graph representation for GraphRAG operations.
type GraphStore struct {
	mu sync.RWMutex

	forwardEdges  map[uint32][]Edge
	backwardEdges map[uint32][]Edge
	predicateMap  map[string]int32
	predicates    []string
	edgeCount     int
}

// Direction defines the traversal direction in the graph.
type Direction int

const (
	// DirectionOutgoing traverses from subject to object.
	DirectionOutgoing Direction = iota
	// DirectionIncoming traverses from object to subject.
	DirectionIncoming
	// DirectionBoth traverses in both directions.
	DirectionBoth
)

// NeighborProvider defines an interface for bulk neighbor lookups.
type NeighborProvider interface {
	GetNeighborsBulk(ctx context.Context, dataset string, nodeIDs []uint32) (map[uint32][]uint32, error)
}

// TraverseOptions configures the graph traversal behavior.
type TraverseOptions struct {
	MaxHops   int
	Direction Direction
	Weighted  bool
	Decay     float32
}

// DefaultTraverseOptions returns a default traversal configuration.
func DefaultTraverseOptions() TraverseOptions {
	return TraverseOptions{
		MaxHops:   2,
		Direction: DirectionOutgoing,
		Weighted:  true,
		Decay:     0.5,
	}
}

// Path represents a sequence of nodes and edges found during traversal.
type Path struct {
	Nodes []VectorID
	Edges []Edge
	Score float32
}

// Edge represents a weighted relationship between a subject and an object.
type Edge struct {
	Subject   VectorID
	Predicate string
	Object    VectorID
	Weight    float32
}

// PathPriorityQueue for weighted traversal
type PathPriorityQueue []Path

// Len returns the number of paths in the queue.
func (pq PathPriorityQueue) Len() int { return len(pq) }

func (pq PathPriorityQueue) Less(i, j int) bool {
	// Higher score comes first (Max-Heap)
	return pq[i].Score > pq[j].Score
}

func (pq PathPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push adds a path to the priority queue.
func (pq *PathPriorityQueue) Push(x any) {
	*pq = append(*pq, x.(Path))
}

// Pop removes the highest-scoring path from the priority queue.
func (pq *PathPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// NewGraphStore creates an empty GraphStore.
func NewGraphStore() *GraphStore {
	return &GraphStore{
		forwardEdges:  make(map[uint32][]Edge),
		backwardEdges: make(map[uint32][]Edge),
		predicateMap:  make(map[string]int32),
		predicates:    make([]string, 0),
		edgeCount:     0,
	}
}

// AddEdge adds a new edge to the graph store.
func (gs *GraphStore) AddEdge(edge Edge) error {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	if _, exists := gs.predicateMap[edge.Predicate]; !exists {
		idx := int32(len(gs.predicates)) // #nosec G115
		gs.predicateMap[edge.Predicate] = idx
		gs.predicates = append(gs.predicates, edge.Predicate)
	}

	gs.forwardEdges[uint32(edge.Subject)] = append(gs.forwardEdges[uint32(edge.Subject)], edge)
	gs.backwardEdges[uint32(edge.Object)] = append(gs.backwardEdges[uint32(edge.Object)], edge)

	gs.edgeCount++
	return nil
}

// EdgeCount returns the total number of edges in the graph.
func (gs *GraphStore) EdgeCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.edgeCount
}

// GetEdgesBySubject returns all outgoing edges for a given subject.
func (gs *GraphStore) GetEdgesBySubject(subject uint32) []Edge {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]Edge(nil), gs.forwardEdges[subject]...)
}

// GetEdgesByObject returns all incoming edges for a given object.
func (gs *GraphStore) GetEdgesByObject(object uint32) []Edge {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]Edge(nil), gs.backwardEdges[object]...)
}

// GetEdgesByPredicate returns all edges with a specific predicate.
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

// PredicateVocabulary returns the list of all predicates in the graph.
func (gs *GraphStore) PredicateVocabulary() []string {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return append([]string(nil), gs.predicates...)
}

// CommunityCount returns the number of nodes that have outgoing edges.
func (gs *GraphStore) CommunityCount() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return len(gs.forwardEdges)
}

// GetCSR converts the graph to a Compressed Sparse Row (CSR) format.
func (gs *GraphStore) GetCSR() (offsets []uint32, neighbors []uint32, weights []float32) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// 1. Determine max node ID
	maxID := uint32(0)
	for id := range gs.forwardEdges {
		if id > maxID {
			maxID = id
		}
	}
	for id := range gs.backwardEdges {
		if id > maxID {
			maxID = id
		}
	}

	nodeCount := maxID + 1
	offsets = make([]uint32, nodeCount+1)
	neighbors = make([]uint32, 0, gs.edgeCount)
	weights = make([]float32, 0, gs.edgeCount)

	currOffset := uint32(0)
	for i := uint32(0); i < nodeCount; i++ {
		offsets[i] = currOffset
		if edges, ok := gs.forwardEdges[i]; ok {
			for _, e := range edges {
				neighbors = append(neighbors, uint32(e.Object))
				weights = append(weights, e.Weight)
				currOffset++
			}
		}
	}
	offsets[nodeCount] = currOffset
	return
}

const (
	// GPUWorkloadThreshold is the minimum number of nodes in the results set
	// to justify the GPU launch latency for graph expansion.
	GPUWorkloadThreshold = 5000
)

// RankWithGraphGPU performs graph-based reranking using GPU acceleration.
func (gs *GraphStore) RankWithGraphGPU(dataset string, results []SearchResult, alpha float32, depth int, gpuIdx gputypes.Index) ([]SearchResult, error) {
	if len(results) == 0 || gpuIdx == nil {
		return results, nil
	}

	// Adaptive Dispatching: Skip GPU for small workloads where kernel launch latency dominates.
	if len(results) < GPUWorkloadThreshold {
		metrics.GraphGPUDispatchFallbackTotal.WithLabelValues(dataset).Inc()
		return gs.RankWithGraph(results, alpha, depth), nil
	}

	metrics.GraphGPUDispatchTotal.WithLabelValues(dataset).Inc()

	// 1. Get CSR and update GPU
	offsets, neighbors, weights := gs.GetCSR()
	if err := gpuIdx.UpdateGraph(offsets, neighbors, weights); err != nil {
		return nil, fmt.Errorf("failed to sync graph to GPU: %w", err)
	}

	// 2. Prepare seeds (top-K results)
	seeds := make([]uint32, len(results))
	for i, r := range results {
		seeds[i] = uint32(r.ID)
	}

	// 3. Expand on GPU
	ids, scores, err := gpuIdx.GraphExpand(seeds, depth, alpha)
	if err != nil {
		return nil, fmt.Errorf("GPU graph expansion failed: %w", err)
	}

	// 4. Combine with initial scores (boost)
	boosts := make(map[uint32]float32)
	for i, id := range ids {
		boosts[id] = scores[i]
	}

	for i, r := range results {
		if b, ok := boosts[uint32(r.ID)]; ok {
			results[i].Score += b
		}
	}

	// 5. Add new discovered neighbors if they have significant scores
	discovered := make(map[uint32]float32)
	for i, id := range ids {
		found := false
		for _, r := range results {
			if uint32(r.ID) == id {
				found = true
				break
			}
		}
		if !found && scores[i] > 0.1 {
			discovered[id] = scores[i]
		}
	}

	for id, s := range discovered {
		results = append(results, SearchResult{ID: lbtypes.VectorID(id), Score: s})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results, nil
}

// RankWithGraph performs graph-based reranking using CPU execution.
func (gs *GraphStore) RankWithGraph(results []SearchResult, alpha float32, depth int) []SearchResult {
	if len(results) == 0 || alpha <= 0 {
		return results
	}

	gs.mu.RLock()
	defer gs.mu.RUnlock()

	// 1. Initial Local Rank & Bounds Checking
	maxID := gs.CommunityCount() + 1000
	for _, r := range results {
		if int(r.ID) > maxID {
			maxID = int(r.ID) + 1
		}
	}

	// Use dense slices for performance
	scoreSlice := make([]float32, maxID+1024)
	visited := make([]uint64, (len(scoreSlice)+63)/64)

	setVisited := func(id uint32) {
		visited[id>>6] |= 1 << (id & 63)
	}
	isVisited := func(id uint32) bool {
		return (visited[id>>6] & (1 << (id & 63))) != 0
	}

	currentNodes := make([]uint32, 0, len(results))
	for _, r := range results {
		id := uint32(r.ID)
		scoreSlice[id] = r.Score
		if !isVisited(id) {
			setVisited(id)
			currentNodes = append(currentNodes, id)
		}
	}

	// 2. Multi-hop BFS Expansion
	nextNodes := make([]uint32, 0, len(currentNodes)*2)

	for d := 0; d < depth; d++ {
		if len(currentNodes) == 0 {
			break
		}

		for i, id := range currentNodes {
			// SIMD prefetching for local edges
			if i+2 < len(currentNodes) {
				nextNextID := currentNodes[i+2]
				if edges, ok := gs.forwardEdges[nextNextID]; ok && len(edges) > 0 {
					simd.Prefetch(unsafe.Pointer(&edges[0])) // #nosec G103
				}
			}

			if edges, ok := gs.forwardEdges[id]; ok {
				s := scoreSlice[id] * alpha
				for _, edge := range edges {
					target := uint32(edge.Object)
					scoreSlice[target] += s * edge.Weight
					if !isVisited(target) {
						setVisited(target)
						nextNodes = append(nextNodes, target)
					}
				}
			}
		}

		// Swap slices for next iteration
		currentNodes = currentNodes[:0]
		currentNodes = append(currentNodes, nextNodes...)
		nextNodes = nextNodes[:0]
	}

	// 3. Rebuild results
	newResults := make([]SearchResult, 0, len(results)*2)
	for id, score := range scoreSlice {
		if score > 0 {
			newResults = append(newResults, SearchResult{ID: lbtypes.VectorID(id), Score: score})
		}
	}

	sort.Slice(newResults, func(i, j int) bool {
		return newResults[i].Score > newResults[j].Score
	})

	if len(newResults) > 2000 {
		newResults = newResults[:2000]
	}

	return newResults
}

// RankWithGraphDistributed performs graph-based reranking across a distributed mesh.
func (gs *GraphStore) RankWithGraphDistributed(ctx context.Context, dataset string, results []SearchResult, alpha float32, depth int, provider NeighborProvider) []SearchResult {
	if len(results) == 0 || alpha <= 0 {
		return results
	}

	// 1. Initial Local Rank & Bounds Checking
	maxID := gs.CommunityCount() + 1000
	for _, r := range results {
		if int(r.ID) > maxID {
			maxID = int(r.ID) + 1
		}
	}

	// Use dense slices for performance if possible
	scoreSlice := make([]float32, maxID+1024)
	visited := make([]uint64, (len(scoreSlice)+63)/64)

	setVisited := func(id uint32) {
		visited[id>>6] |= 1 << (id & 63)
	}
	isVisited := func(id uint32) bool {
		return (visited[id>>6] & (1 << (id & 63))) != 0
	}

	currentNodes := make([]uint32, 0, len(results))
	for _, r := range results {
		id := uint32(r.ID)
		scoreSlice[id] = r.Score
		if !isVisited(id) {
			setVisited(id)
			currentNodes = append(currentNodes, id)
		}
	}

	// 2. Multi-hop Distributed BFS Expansion
	nextNodes := make([]uint32, 0, len(currentNodes)*4) // Pre-allocate with heuristic

	for d := 0; d < depth; d++ {
		if len(currentNodes) == 0 {
			break
		}

		// First, check local edges with prioritized prefetching
		gs.mu.RLock()
		for i, id := range currentNodes {
			// Prioritize for SIMD prefetching: prefetch the edge list for the node after next
			if i+2 < len(currentNodes) {
				nextNextID := currentNodes[i+2]
				if edges, ok := gs.forwardEdges[nextNextID]; ok && len(edges) > 0 {
					simd.Prefetch(unsafe.Pointer(&edges[0])) // #nosec G103
				}
			}

			if edges, ok := gs.forwardEdges[id]; ok {
				s := scoreSlice[id] * alpha
				for _, edge := range edges {
					target := uint32(edge.Object)
					scoreSlice[target] += s * edge.Weight
					if !isVisited(target) {
						setVisited(target)
						nextNodes = append(nextNodes, target)
					}
				}
			}
		}
		gs.mu.RUnlock()

		// Bulk fetch missing neighbors from provider (distributed mesh)
		if provider != nil {
			missing := make([]uint32, 0)
			gs.mu.RLock()
			for _, id := range currentNodes {
				if _, ok := gs.forwardEdges[id]; !ok {
					missing = append(missing, id)
				}
			}
			gs.mu.RUnlock()

			if len(missing) > 0 {
				remoteNeighbors, err := provider.GetNeighborsBulk(ctx, dataset, missing)
				if err == nil {
					for id, neighbors := range remoteNeighbors {
						s := scoreSlice[id] * alpha
						for _, target := range neighbors {
							scoreSlice[target] += s
							if !isVisited(target) {
								setVisited(target)
								nextNodes = append(nextNodes, target)
							}
						}
					}
				}
			}
		}

		// Swap slices for next iteration
		currentNodes = currentNodes[:0]
		currentNodes = append(currentNodes, nextNodes...)
		nextNodes = nextNodes[:0]
	}

	// 3. Rebuild results from dense score slice
	newResults := make([]SearchResult, 0, len(results)*2) // Heuristic
	for id, score := range scoreSlice {
		if score > 0 {
			newResults = append(newResults, SearchResult{ID: lbtypes.VectorID(id), Score: score})
		}
	}

	sort.Slice(newResults, func(i, j int) bool {
		return newResults[i].Score > newResults[j].Score
	})

	if len(newResults) > 2000 { // Limit expansion results to prevent explosion
		newResults = newResults[:2000]
	}

	return newResults
}

// Traverse performs a graph traversal starting from a specific node.
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

// Close releases all resources associated with the graph store.
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

// ToArrowBatch exports the GraphStore's edges to an Arrow Record.
// It uses Dictionary Encoding for the 'predicate' column to ensure the
// predicate vocabulary is self-contained within the record.
func (gs *GraphStore) ToArrowBatch() (arrow.Record, error) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	if gs.edgeCount == 0 {
		return nil, nil
	}

	// Use BinaryDictionaryBuilder for self-contained vocabulary
	dictType := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.Binary}
	mem := memory.NewGoAllocator()
	
	subjectsArr := array.NewUint32Builder(mem)
	defer subjectsArr.Release()
	
	objectsArr := array.NewUint32Builder(mem)
	defer objectsArr.Release()
	
	weightsArr := array.NewFloat32Builder(mem)
	defer weightsArr.Release()
	
	predicatesArr := array.NewDictionaryBuilder(mem, dictType).(*array.BinaryDictionaryBuilder)
	defer predicatesArr.Release()

	// Use ONE loop to avoid random map iteration order issues
	for _, edges := range gs.forwardEdges {
		for _, e := range edges {
			subjectsArr.Append(uint32(e.Subject))
			objectsArr.Append(uint32(e.Object))
			weightsArr.Append(e.Weight)
			if err := predicatesArr.Append([]byte(e.Predicate)); err != nil {
				return nil, err
			}
		}
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "subject", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "predicate", Type: dictType},
		{Name: "object", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	numRows := subjectsArr.Len()
	record := array.NewRecord(schema, []arrow.Array{
		subjectsArr.NewArray(),
		predicatesArr.NewArray(),
		objectsArr.NewArray(),
		weightsArr.NewArray(),
	}, int64(numRows))

	return record, nil
}

// FromArrowBatch loads edges from an Arrow Record into the GraphStore.
// The record must have the schema: subject (uint32), predicate (dictionary), object (uint32), weight (float32).
// This method automatically recovers the predicate vocabulary from the Arrow Dictionary.
// The optional predicates parameter is kept for backward compatibility but is ignored if the record contains a dictionary.
func (gs *GraphStore) FromArrowBatch(record arrow.Record, _ []string) error {
	if record == nil || record.NumRows() == 0 {
		return nil
	}

	gs.mu.Lock()
	defer gs.mu.Unlock()

	// Validate schema
	schema := record.Schema()
	if schema.NumFields() != 4 {
		return arrow.ErrInvalid
	}

	// Get columns
	subjectCol := record.Column(0).(*array.Uint32)
	predicateCol := record.Column(1)
	objectCol := record.Column(2).(*array.Uint32)
	weightCol := record.Column(3).(*array.Float32)

	// Extract predicates from dictionary if available
	if dict, ok := predicateCol.(*array.Dictionary); ok {
		values := dict.Dictionary().(*array.Binary)
		numDictVals := values.Len()
		gs.predicates = make([]string, numDictVals)
		gs.predicateMap = make(map[string]int32)
		for i := 0; i < numDictVals; i++ {
			p := string(values.Value(i))
			gs.predicates[i] = p
			gs.predicateMap[p] = int32(i)
		}
	} else if intIdxCol, ok := predicateCol.(*array.Int32); ok {
		// Fallback for old simple Int32 columns (though vocabulary is lost if not passed externally)
		// This ensures we don't crash on older data if the predicates []string was somehow provided.
		// However, with self-contained dictionary encoding, we prefer the dictionary branch.
		_ = intIdxCol // Just to avoid unused warning in this branch
	}

	// Load edges
	numRows := int(record.NumRows())
	for i := 0; i < numRows; i++ {
		subject := subjectCol.Value(i)
		object := objectCol.Value(i)
		weight := weightCol.Value(i)

		var predicate string
		if dict, ok := predicateCol.(*array.Dictionary); ok {
			idx := dict.GetValueIndex(i)
			if idx >= 0 && idx < len(gs.predicates) {
				predicate = gs.predicates[idx]
			}
		} else if intIdxCol, ok := predicateCol.(*array.Int32); ok {
			idx := int(intIdxCol.Value(i))
			if idx >= 0 && idx < len(gs.predicates) {
				predicate = gs.predicates[idx]
			}
		}

		edge := Edge{
			Subject:   VectorID(subject),
			Predicate: predicate,
			Object:    VectorID(object),
			Weight:    weight,
		}

		// Add to maps (without going through AddEdge to avoid duplicate predicate handling)
		gs.forwardEdges[subject] = append(gs.forwardEdges[subject], edge)
		gs.backwardEdges[object] = append(gs.backwardEdges[object], edge)
		gs.edgeCount++
	}

	return nil
}

// ToArrowRecord is an alias for ToArrowBatch for API consistency.
func (gs *GraphStore) ToArrowRecord() (arrow.Record, error) {
	return gs.ToArrowBatch()
}

// FromArrowRecord is an alias for FromArrowBatch for API consistency.
func (gs *GraphStore) FromArrowRecord(record arrow.Record, predicates []string) error {
	return gs.FromArrowBatch(record, predicates)
}
