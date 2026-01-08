package store

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Edge represents a knowledge graph edge (subject -> predicate -> object)
type Edge struct {
	Subject   VectorID // Source entity
	Predicate string   // Relationship type (e.g., "owns", "likes")
	Object    VectorID // Target entity
	Weight    float32  // Edge weight for scoring
}

// GraphStore manages knowledge graph edges for GraphRAG workflows
// Uses columnar storage for improved memory locality and zero-copy Arrow compatibility.
type GraphStore struct {
	// Global lock for columnar data arrays (subjects, objects, predicates, weights)
	// AddEdge takes Lock; Traverse takes RLock for data access.
	// dataMu protects the dataset reference and migration state.
	// It is held for Reading during searches and Writing during compaction/migration.
	dataMu MeasuredRWMutex

	// Sharded locks for adjacency indices to reduce contention during updates/traversals
	// Maps are protected by indexShards[hash(key) % 256]
	indexShards [256]sync.RWMutex

	// Columnar storage
	subjects   []VectorID
	objects    []VectorID
	predicates []uint16 // Index into predicateDict
	weights    []float32

	// Predicate Dictionary (protected by dataMu)
	predicateDict  []string
	predicateToIdx map[string]uint16

	// Indices (Adjacency Lists) maps value -> list of edge indices
	// Sharded by [hash(key) % 256] matching indexShards
	subjectIndex        [256]map[VectorID][]int
	objectIndex         [256]map[VectorID][]int
	predicateValueIndex [256]map[uint16][]int

	// Community detection results (protected by dataMu for now)
	nodeCommunity map[VectorID]int
	communities   []Community
}

// NewGraphStore creates a new empty graph store
func NewGraphStore() *GraphStore {
	gs := &GraphStore{
		subjects:       make([]VectorID, 0),
		objects:        make([]VectorID, 0),
		predicates:     make([]uint16, 0),
		weights:        make([]float32, 0),
		predicateDict:  make([]string, 0),
		predicateToIdx: make(map[string]uint16),
		dataMu:         NewMeasuredRWMutex("graph_store_data"),
	}

	for i := 0; i < 256; i++ {
		gs.subjectIndex[i] = make(map[VectorID][]int)
		gs.objectIndex[i] = make(map[VectorID][]int)
		gs.predicateValueIndex[i] = make(map[uint16][]int)
	}

	return gs
}

// shardForVectorID returns the lock shard index for a VectorID
func (gs *GraphStore) shardForVectorID(id VectorID) int {
	// Simple mixing
	h := uint64(id)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	return int(h % 256)
}

// shardForPredicate returns the lock shard index for a predicate ID
func (gs *GraphStore) shardForPredicate(id uint16) int {
	return int(id % 256)
}

// AddEdge adds an edge to the graph store
// Uses Hybrid Locking: Global Data Lock -> Append Data -> Unlock -> Sharded Index Locks -> Update Indices
func (gs *GraphStore) AddEdge(e Edge) error {
	// Phase 1: Append Data (Global Lock)
	gs.dataMu.Lock()

	// Dictionary encoding for predicate
	predIdx, ok := gs.predicateToIdx[e.Predicate]
	if !ok {
		if len(gs.predicateDict) >= 65535 {
			gs.dataMu.Unlock()
			return fmt.Errorf("too many unique predicates for Dictionary encoding")
		}
		predIdx = uint16(len(gs.predicateDict))
		gs.predicateDict = append(gs.predicateDict, e.Predicate)
		gs.predicateToIdx[e.Predicate] = predIdx
	}

	idx := len(gs.subjects)
	gs.subjects = append(gs.subjects, e.Subject)
	gs.objects = append(gs.objects, e.Object)
	gs.predicates = append(gs.predicates, predIdx)
	gs.weights = append(gs.weights, e.Weight)

	gs.dataMu.Unlock()

	// Phase 2: Update Indices (Sharded Locks)
	// Note: It's possible for concurrent reads to see data but not find it in index yet.
	// This is acceptable eventual consistency for typical GraphRAG patterns.

	// Subject Index
	sShard := gs.shardForVectorID(e.Subject)
	gs.indexShards[sShard].Lock()
	gs.subjectIndex[sShard][e.Subject] = append(gs.subjectIndex[sShard][e.Subject], idx)
	gs.indexShards[sShard].Unlock()

	// Object Index
	oShard := gs.shardForVectorID(e.Object)
	gs.indexShards[oShard].Lock()
	gs.objectIndex[oShard][e.Object] = append(gs.objectIndex[oShard][e.Object], idx)
	gs.indexShards[oShard].Unlock()

	// Predicate Index
	pShard := gs.shardForPredicate(predIdx)
	gs.indexShards[pShard].Lock()
	gs.predicateValueIndex[pShard][predIdx] = append(gs.predicateValueIndex[pShard][predIdx], idx)
	gs.indexShards[pShard].Unlock()

	return nil
}

// EdgeCount returns the total number of edges
func (gs *GraphStore) EdgeCount() int {
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()
	return len(gs.subjects)
}

// getEdgeAt reconstructs an Edge from columnar data at index i
// Unsafe: caller must hold dataMu RLock
func (gs *GraphStore) getEdgeAt(i int) Edge {
	predIdx := gs.predicates[i]
	return Edge{
		Subject:   gs.subjects[i],
		Predicate: gs.predicateDict[predIdx],
		Object:    gs.objects[i],
		Weight:    gs.weights[i],
	}
}

// GetEdgesBySubject returns all edges with the given subject (outgoing edges)
func (gs *GraphStore) GetEdgesBySubject(subject VectorID) []Edge {
	// 1. Get Indices (Shard Lock)
	shard := gs.shardForVectorID(subject)
	gs.indexShards[shard].RLock()
	indices := make([]int, len(gs.subjectIndex[shard][subject]))
	copy(indices, gs.subjectIndex[shard][subject])
	gs.indexShards[shard].RUnlock()

	// 2. Get Data (Global Data Lock)
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	result := make([]Edge, len(indices))
	for i, idx := range indices {
		result[i] = gs.getEdgeAt(idx)
	}
	return result
}

// GetEdgesByObject returns all edges with the given object (incoming edges)
func (gs *GraphStore) GetEdgesByObject(object VectorID) []Edge {
	// 1. Get Indices (Shard Lock)
	shard := gs.shardForVectorID(object)
	gs.indexShards[shard].RLock()
	indices := make([]int, len(gs.objectIndex[shard][object]))
	copy(indices, gs.objectIndex[shard][object])
	gs.indexShards[shard].RUnlock()

	// 2. Get Data (Global Data Lock)
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	result := make([]Edge, len(indices))
	for i, idx := range indices {
		result[i] = gs.getEdgeAt(idx)
	}
	return result
}

// GetEdgesByPredicate returns all edges with the given predicate
func (gs *GraphStore) GetEdgesByPredicate(predicate string) []Edge {
	gs.dataMu.RLock()
	predIdx, ok := gs.predicateToIdx[predicate]
	gs.dataMu.RUnlock()

	if !ok {
		return nil
	}

	// 1. Get Indices (Shard Lock)
	shard := gs.shardForPredicate(predIdx)
	gs.indexShards[shard].RLock()
	indices := make([]int, len(gs.predicateValueIndex[shard][predIdx]))
	copy(indices, gs.predicateValueIndex[shard][predIdx])
	gs.indexShards[shard].RUnlock()

	// 2. Get Data (Global Data Lock)
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	result := make([]Edge, len(indices))
	for i, idx := range indices {
		result[i] = gs.getEdgeAt(idx)
	}
	return result
}

// PredicateVocabulary returns all unique predicate types
func (gs *GraphStore) PredicateVocabulary() []string {
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	// Return a copy to be safe
	result := make([]string, len(gs.predicateDict))
	copy(result, gs.predicateDict)
	return result
}

// ToArrowBatch converts all edges to an Arrow RecordBatch with Dictionary-encoded predicates
func (gs *GraphStore) ToArrowBatch(mem memory.Allocator) (arrow.Record, error) { //nolint:staticcheck
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	n := len(gs.subjects)
	if n == 0 {
		return nil, fmt.Errorf("no edges to convert")
	}

	// Build schema with Dictionary-encoded predicate column
	md := arrow.NewMetadata(
		[]string{"longbow.entry_type"},
		[]string{"graph"},
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "subject", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "predicate", Type: &arrow.DictionaryType{
			IndexType: arrow.PrimitiveTypes.Uint16,
			ValueType: arrow.BinaryTypes.String,
		}},
		{Name: "object", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float32},
	}, &md)

	// Build subject column
	subjectBuilder := array.NewUint32Builder(mem)
	defer subjectBuilder.Release()
	subjectBuilder.AppendValues(func() []uint32 {
		// Zero-copy-ish cast if possible, but safe copy for now
		res := make([]uint32, len(gs.subjects))
		for i, v := range gs.subjects {
			res[i] = uint32(v)
		}
		return res
	}(), nil)
	subjectArr := subjectBuilder.NewArray()
	defer subjectArr.Release()

	// Build dictionary for predicates
	dictBuilder := array.NewStringBuilder(mem)
	defer dictBuilder.Release()
	dictBuilder.AppendValues(gs.predicateDict, nil)
	dictArr := dictBuilder.NewArray()
	defer dictArr.Release()

	// Build predicate indices
	indexBuilder := array.NewUint16Builder(mem)
	defer indexBuilder.Release()
	indexBuilder.AppendValues(gs.predicates, nil)
	indexArr := indexBuilder.NewArray()
	defer indexArr.Release()

	// Create Dictionary array
	predicateArr := array.NewDictionaryArray(&arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Uint16,
		ValueType: arrow.BinaryTypes.String,
	}, indexArr, dictArr)
	defer predicateArr.Release()

	// Build object column
	objectBuilder := array.NewUint32Builder(mem)
	defer objectBuilder.Release()
	objectBuilder.AppendValues(func() []uint32 {
		res := make([]uint32, len(gs.objects))
		for i, v := range gs.objects {
			res[i] = uint32(v)
		}
		return res
	}(), nil)
	objectArr := objectBuilder.NewArray()
	defer objectArr.Release()

	// Build weight column
	weightBuilder := array.NewFloat32Builder(mem)
	defer weightBuilder.Release()
	weightBuilder.AppendValues(gs.weights, nil)
	weightArr := weightBuilder.NewArray()
	defer weightArr.Release()

	// Retain arrays for record batch
	subjectArr.Retain()
	predicateArr.Retain()
	objectArr.Retain()
	weightArr.Retain()

	return array.NewRecord(schema, []arrow.Array{subjectArr, predicateArr, objectArr, weightArr}, int64(n)), nil //nolint:staticcheck
}

// FromArrowBatch loads edges from an Arrow RecordBatch
func (gs *GraphStore) FromArrowBatch(batch arrow.Record) error { //nolint:staticcheck
	// Phase 1: Batch Load Data (Global Lock)
	gs.dataMu.Lock()

	n := int(batch.NumRows())
	if n == 0 {
		gs.dataMu.Unlock()
		return nil
	}

	// Extract columns
	subjectCol := batch.Column(0).(*array.Uint32)
	predicateCol := batch.Column(1).(*array.Dictionary)
	objectCol := batch.Column(2).(*array.Uint32)
	weightCol := batch.Column(3).(*array.Float32)

	// Sync predicate dictionary
	predicateDictArr := predicateCol.Dictionary().(*array.String)
	dictMapping := make(map[int]uint16) // map arrows-dict-idx -> our-dict-idx

	for i := 0; i < predicateDictArr.Len(); i++ {
		p := predicateDictArr.Value(i)
		if idx, ok := gs.predicateToIdx[p]; ok {
			dictMapping[i] = idx
		} else {
			newIdx := uint16(len(gs.predicateDict))
			gs.predicateDict = append(gs.predicateDict, p)
			gs.predicateToIdx[p] = newIdx
			dictMapping[i] = newIdx
		}
	}

	// Pre-allocate to avoid repeated appends
	startLen := len(gs.subjects)

	// Extend slices
	for i := 0; i < n; i++ {
		subj := VectorID(subjectCol.Value(i))
		obj := VectorID(objectCol.Value(i))
		weight := weightCol.Value(i)
		arrowDictIdx := predicateCol.GetValueIndex(i)
		predIdx := dictMapping[arrowDictIdx]

		gs.subjects = append(gs.subjects, subj)
		gs.objects = append(gs.objects, obj)
		gs.predicates = append(gs.predicates, predIdx)
		gs.weights = append(gs.weights, weight)
	}

	gs.dataMu.Unlock()

	// Phase 2: Update Indices (Sharded Locks)
	for i := 0; i < n; i++ {
		subj := VectorID(subjectCol.Value(i))
		obj := VectorID(objectCol.Value(i))
		arrowDictIdx := predicateCol.GetValueIndex(i)
		predIdx := dictMapping[arrowDictIdx]

		idx := startLen + i

		// Indices
		sShard := gs.shardForVectorID(subj)
		gs.indexShards[sShard].Lock()
		gs.subjectIndex[sShard][subj] = append(gs.subjectIndex[sShard][subj], idx)
		gs.indexShards[sShard].Unlock()

		oShard := gs.shardForVectorID(obj)
		gs.indexShards[oShard].Lock()
		gs.objectIndex[oShard][obj] = append(gs.objectIndex[oShard][obj], idx)
		gs.indexShards[oShard].Unlock()

		pShard := gs.shardForPredicate(predIdx)
		gs.indexShards[pShard].Lock()
		gs.predicateValueIndex[pShard][predIdx] = append(gs.predicateValueIndex[pShard][predIdx], idx)
		gs.indexShards[pShard].Unlock()
	}

	return nil
}

// Path represents a traversal path through the graph
type Path struct {
	Nodes  []VectorID // Sequence of nodes visited
	Edges  []Edge     // Edges traversed
	Weight float32    // Cumulative path weight
}

// Direction of graph traversal
type Direction int

const (
	DirectionOutgoing Direction = iota
	DirectionIncoming
	DirectionBoth
)

// TraverseOptions configures the graph traversal
type TraverseOptions struct {
	Direction     Direction // Which edges to follow
	MaxHops       int       // Maximum depth
	Decay         float32   // Weight decay per hop (e.g., 0.9)
	TopKNeighbors int       // Max neighbors to explore per node (0 = unlimited)
	Weighted      bool      // Use edge weights in path calculation
}

// DefaultTraverseOptions returns sensible defaults
func DefaultTraverseOptions() TraverseOptions {
	return TraverseOptions{
		Direction:     DirectionOutgoing,
		MaxHops:       3,
		Decay:         0.8,
		TopKNeighbors: 0,
		Weighted:      true,
	}
}

// Traverse performs graph traversal with specified options
// Holds Global Data Read Lock for duration to ensure consistent snapshot.
func (gs *GraphStore) Traverse(start VectorID, opts TraverseOptions) []Path {
	startTime := time.Now()
	defer func() {
		metrics.GraphTraversalDurationSeconds.Observe(time.Since(startTime).Seconds())
	}()

	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	var paths []Path
	visited := make(map[VectorID]float32) // Visit score (higher is better/earlier)

	// Frontier management abstraction
	var push func(queueItem)
	var pop func() queueItem
	var notEmpty func() bool

	if opts.Weighted {
		// Priority Queue for weighted traversal (Dijkstra/Best-First)
		pq := &PathPriorityQueue{}
		heap.Init(pq)
		startItem := queueItem{
			path:  Path{Nodes: []VectorID{start}, Weight: 1.0},
			score: 1.0,
		}
		heap.Push(pq, startItem)

		push = func(item queueItem) {
			heap.Push(pq, item)
		}
		pop = func() queueItem {
			return heap.Pop(pq).(queueItem)
		}
		notEmpty = func() bool {
			return pq.Len() > 0
		}
	} else {
		// Standard Queue for BFS
		queue := []queueItem{{
			path:  Path{Nodes: []VectorID{start}, Weight: 1.0},
			score: 1.0,
		}}
		push = func(item queueItem) {
			queue = append(queue, item)
		}
		pop = func() queueItem {
			item := queue[0]
			queue = queue[1:]
			return item
		}
		notEmpty = func() bool {
			return len(queue) > 0
		}
	}

	visited[start] = 1.0

	for notEmpty() {
		item := pop()

		if len(item.path.Nodes) > opts.MaxHops+1 { // +1 because start node counts
			continue
		}

		// Add valid paths (skip single start node path if not desired, but usually we return all sub-paths)
		if len(item.path.Nodes) > 1 {
			paths = append(paths, item.path)
		}

		if len(item.path.Nodes) >= opts.MaxHops+1 {
			continue // Stop expanding
		}

		current := item.path.Nodes[len(item.path.Nodes)-1]

		// Collect neighbor candidates based on direction
		var indices []int

		// Outgoing: subjectIndex[current]
		if opts.Direction == DirectionOutgoing || opts.Direction == DirectionBoth {
			shard := gs.shardForVectorID(current)
			gs.indexShards[shard].RLock()
			indices = append(indices, gs.subjectIndex[shard][current]...)
			gs.indexShards[shard].RUnlock()
		}

		// Incoming: objectIndex[current]
		// For incoming, 'current' is the Object. We look for Subjects.
		// NOTE: indices here point to the edge. The "next" node depends on edge direction.
		var incomingIndices []int
		if opts.Direction == DirectionIncoming || opts.Direction == DirectionBoth {
			shard := gs.shardForVectorID(current)
			gs.indexShards[shard].RLock()
			incomingIndices = append(incomingIndices, gs.objectIndex[shard][current]...)
			gs.indexShards[shard].RUnlock()
		}

		// Process neighbors
		for _, idx := range indices {
			// OUTGOING: current -> next
			nextNode := gs.objects[idx] // The Object is the target
			edgeWeight := gs.weights[idx]
			if !opts.Weighted {
				edgeWeight = 1.0
			}

			gs.processNeighbor(nextNode, idx, edgeWeight, item, opts, visited, push)
		}

		for _, idx := range incomingIndices {
			// INCOMING: next -> current
			nextNode := gs.subjects[idx] // The Subject is the source (next step in reverse traversal)
			edgeWeight := gs.weights[idx]
			if !opts.Weighted {
				edgeWeight = 1.0
			}

			gs.processNeighbor(nextNode, idx, edgeWeight, item, opts, visited, push)
		}
	}

	return paths
}

type queueItem struct {
	path  Path
	score float32
}

// PathPriorityQueue implements heap.Interface for weighted traversal
type PathPriorityQueue []queueItem

func (pq PathPriorityQueue) Len() int { return len(pq) }

// Less implements Max-Heap logic (higher score is better)
func (pq PathPriorityQueue) Less(i, j int) bool {
	return pq[i].score > pq[j].score
}

func (pq PathPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PathPriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(queueItem))
}

func (pq *PathPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// processNeighbor is a helper to extend paths
func (gs *GraphStore) processNeighbor(nextNode VectorID, edgeIdx int, edgeWeight float32, item queueItem, opts TraverseOptions, visited map[VectorID]float32, push func(queueItem)) {
	// Decay score
	decay := opts.Decay
	if decay == 0 {
		decay = 1.0
	}

	newScore := item.score * decay
	if opts.Weighted {
		newScore *= edgeWeight
	}

	// Cycle detection / Visited check with score
	if prevScore, seen := visited[nextNode]; seen {
		// If we found a significantly better path, revisit?
		// For basic DAG/Tree traversal we don't revisit.
		// If we want Dijkstra-like behavior, we update if newScore > prevScore.
		if newScore <= prevScore {
			return
		}
	}
	visited[nextNode] = newScore

	// Create new path
	newNodes := make([]VectorID, len(item.path.Nodes)+1)
	copy(newNodes, item.path.Nodes)
	newNodes[len(item.path.Nodes)] = nextNode

	edgeStr := gs.getEdgeAt(edgeIdx)
	newEdges := make([]Edge, len(item.path.Edges)+1)
	copy(newEdges, item.path.Edges)
	newEdges[len(item.path.Edges)] = edgeStr

	newPath := Path{
		Nodes:  newNodes,
		Edges:  newEdges,
		Weight: item.path.Weight + edgeWeight,
	}

	push(queueItem{
		path:  newPath,
		score: newScore,
	})
}

// TraverseParallel performs concurrent traversal from multiple starting points
func (gs *GraphStore) TraverseParallel(starts []VectorID, opts TraverseOptions) map[VectorID][]Path {
	results := make(map[VectorID][]Path)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, start := range starts {
		wg.Add(1)
		go func(s VectorID) {
			defer wg.Done()
			paths := gs.Traverse(s, opts)
			mu.Lock()
			results[s] = paths
			mu.Unlock()
		}(start)
	}

	wg.Wait()
	return results
}

// Community represents a detected graph community
type Community struct {
	ID      int
	Members []VectorID
}

// DetectCommunities runs Louvain algorithm to find communities
func (gs *GraphStore) DetectCommunities() []Community {
	startTime := time.Now()
	defer func() {
		metrics.GraphClusteringDurationSeconds.Observe(time.Since(startTime).Seconds())
	}()

	gs.dataMu.Lock()
	defer gs.dataMu.Unlock()

	// Get all unique nodes
	nodes := make(map[VectorID]bool)
	for _, s := range gs.subjects {
		nodes[s] = true
	}
	for _, o := range gs.objects {
		nodes[o] = true
	}

	if len(nodes) == 0 {
		return nil
	}

	// Initialize: each node in its own community
	nodeList := make([]VectorID, 0, len(nodes))
	for n := range nodes {
		nodeList = append(nodeList, n)
	}

	// Community assignment: node -> community ID
	community := make(map[VectorID]int)
	for i, n := range nodeList {
		community[n] = i
	}

	// Build adjacency with weights
	adj := make(map[VectorID]map[VectorID]float32)
	for i := 0; i < len(gs.subjects); i++ {
		subj := gs.subjects[i]
		obj := gs.objects[i]
		w := gs.weights[i]

		if adj[subj] == nil {
			adj[subj] = make(map[VectorID]float32)
		}
		adj[subj][obj] += w
	}

	// Louvain Phase 1: Local moving
	changed := true
	for iter := 0; iter < 10 && changed; iter++ {
		changed = false
		for _, node := range nodeList {
			bestComm := community[node]
			bestGain := float32(0.0)

			// Check neighbor communities
			neighbors := adj[node]
			commWeights := make(map[int]float32)
			for neighbor, weight := range neighbors {
				neighborComm := community[neighbor]
				commWeights[neighborComm] += weight
			}

			// Find best community to move to
			for comm, weight := range commWeights {
				if weight > bestGain {
					bestGain = weight
					bestComm = comm
				}
			}

			// Move node if beneficial
			if bestComm != community[node] {
				community[node] = bestComm
				changed = true
			}
		}
	}

	// Store community assignments
	gs.nodeCommunity = community

	// Build community list
	commMembers := make(map[int][]VectorID)
	for node, comm := range community {
		commMembers[comm] = append(commMembers[comm], node)
	}

	result := make([]Community, 0, len(commMembers))
	for id, members := range commMembers {
		result = append(result, Community{ID: id, Members: members})
	}

	gs.communities = result
	metrics.GraphCommunitiesTotal.Set(float64(len(result)))
	return result
}

// GetCommunityForNode returns the community ID for a given node
func (gs *GraphStore) GetCommunityForNode(node VectorID) int {
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()

	if gs.nodeCommunity == nil {
		return -1
	}
	if comm, ok := gs.nodeCommunity[node]; ok {
		return comm
	}
	return -1
}

// CommunityCount returns the number of detected communities
func (gs *GraphStore) CommunityCount() int {
	gs.dataMu.RLock()
	defer gs.dataMu.RUnlock()
	return len(gs.communities)
}

// RankWithGraph re-ranks search results using graph topology (GraphRAG).
// It performs spreading activation from the initial results to boost connected nodes.
// initial: vector search results
// alpha: 0.0=vector score only, 1.0=graph score only
// maxDepth: hops for spreading activation
func (gs *GraphStore) RankWithGraph(initial []SearchResult, alpha float32, maxDepth int) []SearchResult {
	if len(initial) == 0 {
		return nil
	}
	if alpha <= 0 {
		return initial
	}
	if alpha > 1.0 {
		alpha = 1.0
	}

	// 1. Initialize scores using Reciprocal Rank
	// This standardizes metric semantics (Distance vs Similarity) by trusting the sort order.
	scores := make(map[VectorID]float32)
	maxVecScore := float32(0.0)

	for i, res := range initial {
		// 1/(rank+1) decay ensures top results have high mass
		rankScore := float32(1.0) / float32(i+1)
		scores[res.ID] = rankScore
		if rankScore > maxVecScore {
			maxVecScore = rankScore
		}
	}

	// Normalize vector scores
	if maxVecScore > 0 {
		for id := range scores {
			scores[id] /= maxVecScore
		}
	}

	// 2. Spreading Activation (Graph Traversal)
	graphScores := make(map[VectorID]float32)

	seeds := make([]VectorID, 0, len(initial))
	for _, res := range initial {
		seeds = append(seeds, res.ID)
	}

	// Run parallel traversal from all seeds
	opts := TraverseOptions{
		Direction:     DirectionOutgoing, // Promote concepts connected FROM the result
		MaxHops:       maxDepth,
		Decay:         0.5,
		TopKNeighbors: 0,
		Weighted:      true,
	}

	traversalResults := gs.TraverseParallel(seeds, opts)

	// Accumulate graph scores
	for _, paths := range traversalResults {
		for _, path := range paths {
			if len(path.Nodes) < 1 {
				continue
			}

			seedID := path.Nodes[0]
			seedScore := scores[seedID]

			// Last node gets the boost
			target := path.Nodes[len(path.Nodes)-1]

			// Simple decay based on path length
			dist := len(path.Nodes) - 1
			decay := float32(1.0) / float32(dist+1) // 1, 0.5, 0.33

			// Mass accumulation
			inc := seedScore * decay * (1.0 + path.Weight)
			graphScores[target] += inc
		}
	}

	// Normalize graph scores
	maxGraphScore := float32(0.0)
	for _, s := range graphScores {
		if s > maxGraphScore {
			maxGraphScore = s
		}
	}
	if maxGraphScore == 0 {
		maxGraphScore = 1.0
	}
	for id := range graphScores {
		graphScores[id] /= maxGraphScore
	}

	// 3. Combine Scores
	// Final = (1-alpha)*Vec + alpha*Graph
	finalScores := make(map[VectorID]float32)

	// Collect all unique IDs involved
	allIDs := make(map[VectorID]struct{})
	for id := range scores {
		allIDs[id] = struct{}{}
	}
	for id := range graphScores {
		allIDs[id] = struct{}{}
	}

	for id := range allIDs {
		vecS := scores[id]        // 0 if not in initial
		graphS := graphScores[id] // 0 if not reached

		finalScores[id] = (1.0-alpha)*vecS + alpha*graphS
	}

	// 4. Convert to SearchResult
	results := make([]SearchResult, 0, len(finalScores))
	for id, s := range finalScores {
		results = append(results, SearchResult{ID: id, Score: s})
	}

	return dedupeAndSort(results, len(initial)*2) // Return expanded set
}

// Close releases resources associated with the graph store.
func (gs *GraphStore) Close() error {
	gs.dataMu.Lock()
	defer gs.dataMu.Unlock()

	gs.subjects = nil
	gs.objects = nil
	gs.predicates = nil
	gs.weights = nil
	gs.predicateDict = nil
	gs.predicateToIdx = nil

	for i := 0; i < 256; i++ {
		gs.indexShards[i].Lock()
		gs.subjectIndex[i] = nil
		gs.objectIndex[i] = nil
		gs.predicateValueIndex[i] = nil
		gs.indexShards[i].Unlock()
	}

	gs.nodeCommunity = nil
	gs.communities = nil

	return nil
}
