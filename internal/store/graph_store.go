package store

import (
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
Subject   string  // Source entity (e.g., "user:alice")
Predicate string  // Relationship type (e.g., "owns", "likes")
Object    string  // Target entity (e.g., "doc:report1")
Weight    float32 // Edge weight for scoring
}

// GraphStore manages knowledge graph edges for GraphRAG workflows
type GraphStore struct {
mu sync.RWMutex

// All edges stored
edges []Edge

// Index: subject -> edge indices
subjectIndex map[string][]int

// Index: object -> edge indices
objectIndex map[string][]int

// Index: predicate -> edge indices
predicateIndex map[string][]int

// Unique predicates (vocabulary for future Dictionary encoding)
predicates map[string]struct{}

// Community detection results
nodeCommunity map[string]int
communities   []Community
}

// NewGraphStore creates a new empty graph store
func NewGraphStore() *GraphStore {
return &GraphStore{
edges:          make([]Edge, 0),
subjectIndex:   make(map[string][]int),
objectIndex:    make(map[string][]int),
predicateIndex: make(map[string][]int),
predicates:     make(map[string]struct{}),
}
}

// AddEdge adds an edge to the graph store
func (gs *GraphStore) AddEdge(e Edge) error {
gs.mu.Lock()
defer gs.mu.Unlock()

idx := len(gs.edges)
gs.edges = append(gs.edges, e)

// Update subject index
gs.subjectIndex[e.Subject] = append(gs.subjectIndex[e.Subject], idx)

// Update object index
gs.objectIndex[e.Object] = append(gs.objectIndex[e.Object], idx)

// Update predicate index
gs.predicateIndex[e.Predicate] = append(gs.predicateIndex[e.Predicate], idx)

// Track unique predicates
gs.predicates[e.Predicate] = struct{}{}

return nil
}

// EdgeCount returns the total number of edges
func (gs *GraphStore) EdgeCount() int {
gs.mu.RLock()
defer gs.mu.RUnlock()
return len(gs.edges)
}

// GetEdgesBySubject returns all edges with the given subject (outgoing edges)
func (gs *GraphStore) GetEdgesBySubject(subject string) []Edge {
gs.mu.RLock()
defer gs.mu.RUnlock()

indices := gs.subjectIndex[subject]
result := make([]Edge, len(indices))
for i, idx := range indices {
result[i] = gs.edges[idx]
}
return result
}

// GetEdgesByObject returns all edges with the given object (incoming edges)
func (gs *GraphStore) GetEdgesByObject(object string) []Edge {
gs.mu.RLock()
defer gs.mu.RUnlock()

indices := gs.objectIndex[object]
result := make([]Edge, len(indices))
for i, idx := range indices {
result[i] = gs.edges[idx]
}
return result
}

// GetEdgesByPredicate returns all edges with the given predicate
func (gs *GraphStore) GetEdgesByPredicate(predicate string) []Edge {
gs.mu.RLock()
defer gs.mu.RUnlock()

indices := gs.predicateIndex[predicate]
result := make([]Edge, len(indices))
for i, idx := range indices {
result[i] = gs.edges[idx]
}
return result
}

// PredicateVocabulary returns all unique predicate types
func (gs *GraphStore) PredicateVocabulary() []string {
gs.mu.RLock()
defer gs.mu.RUnlock()

result := make([]string, 0, len(gs.predicates))
for p := range gs.predicates {
result = append(result, p)
}
return result
}


// ToArrowBatch converts all edges to an Arrow RecordBatch with Dictionary-encoded predicates
func (gs *GraphStore) ToArrowBatch(mem memory.Allocator) (arrow.Record, error) { //nolint:staticcheck
gs.mu.RLock()
defer gs.mu.RUnlock()

n := len(gs.edges)
if n == 0 {
return nil, fmt.Errorf("no edges to convert")
}

// Build predicate dictionary (maps predicate string -> index)
predicateToIdx := make(map[string]int)
predicateList := make([]string, 0, len(gs.predicates))
for p := range gs.predicates {
predicateToIdx[p] = len(predicateList)
predicateList = append(predicateList, p)
}

// Build schema with Dictionary-encoded predicate column
schema := arrow.NewSchema([]arrow.Field{
{Name: "subject", Type: arrow.BinaryTypes.String},
{Name: "predicate", Type: &arrow.DictionaryType{
IndexType: arrow.PrimitiveTypes.Uint16,
ValueType: arrow.BinaryTypes.String,
}},
{Name: "object", Type: arrow.BinaryTypes.String},
{Name: "weight", Type: arrow.PrimitiveTypes.Float32},
}, nil)

// Build subject column
subjectBuilder := array.NewStringBuilder(mem)
defer subjectBuilder.Release()
for _, e := range gs.edges {
subjectBuilder.Append(e.Subject)
}
subjectArr := subjectBuilder.NewArray()
defer subjectArr.Release()

// Build dictionary for predicates
dictBuilder := array.NewStringBuilder(mem)
defer dictBuilder.Release()
for _, p := range predicateList {
dictBuilder.Append(p)
}
dictArr := dictBuilder.NewArray()
defer dictArr.Release()

// Build predicate indices
indexBuilder := array.NewUint16Builder(mem)
defer indexBuilder.Release()
for _, e := range gs.edges {
indexBuilder.Append(uint16(predicateToIdx[e.Predicate]))
}
indexArr := indexBuilder.NewArray()
defer indexArr.Release()

// Create Dictionary array
predicateArr := array.NewDictionaryArray(&arrow.DictionaryType{
IndexType: arrow.PrimitiveTypes.Uint16,
ValueType: arrow.BinaryTypes.String,
}, indexArr, dictArr)
defer predicateArr.Release()

// Build object column
objectBuilder := array.NewStringBuilder(mem)
defer objectBuilder.Release()
for _, e := range gs.edges {
objectBuilder.Append(e.Object)
}
objectArr := objectBuilder.NewArray()
defer objectArr.Release()

// Build weight column
weightBuilder := array.NewFloat32Builder(mem)
defer weightBuilder.Release()
for _, e := range gs.edges {
weightBuilder.Append(e.Weight)
}
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
gs.mu.Lock()
defer gs.mu.Unlock()

n := int(batch.NumRows())
if n == 0 {
return nil
}

// Extract columns
subjectCol := batch.Column(0).(*array.String)
predicateCol := batch.Column(1).(*array.Dictionary)
objectCol := batch.Column(2).(*array.String)
weightCol := batch.Column(3).(*array.Float32)

// Get predicate dictionary values
predicateDict := predicateCol.Dictionary().(*array.String)

for i := 0; i < n; i++ {
// Get predicate from dictionary
predicateIdx := predicateCol.GetValueIndex(i)
predicate := predicateDict.Value(predicateIdx)

edge := Edge{
Subject:   subjectCol.Value(i),
Predicate: predicate,
Object:    objectCol.Value(i),
Weight:    weightCol.Value(i),
}

idx := len(gs.edges)
gs.edges = append(gs.edges, edge)
gs.subjectIndex[edge.Subject] = append(gs.subjectIndex[edge.Subject], idx)
gs.objectIndex[edge.Object] = append(gs.objectIndex[edge.Object], idx)
gs.predicateIndex[edge.Predicate] = append(gs.predicateIndex[edge.Predicate], idx)
gs.predicates[edge.Predicate] = struct{}{}
}

return nil
}


// Path represents a traversal path through the graph
type Path struct {
Nodes  []string  // Sequence of nodes visited
Edges  []Edge    // Edges traversed
Weight float32   // Cumulative path weight
}

// Traverse performs BFS traversal from start node up to maxHops depth
func (gs *GraphStore) Traverse(start string, maxHops int) []Path {
startTime := time.Now()
defer func() {
metrics.GraphTraversalDurationSeconds.Observe(time.Since(startTime).Seconds())
}()

gs.mu.RLock()
defer gs.mu.RUnlock()

var paths []Path
visited := make(map[string]bool)

// BFS queue: each item is (current path, current depth)
type queueItem struct {
path  Path
depth int
}

queue := []queueItem{{
path:  Path{Nodes: []string{start}, Weight: 0},
depth: 0,
}}

for len(queue) > 0 {
item := queue[0]
queue = queue[1:]

current := item.path.Nodes[len(item.path.Nodes)-1]

// Get outgoing edges
indices := gs.subjectIndex[current]
for _, idx := range indices {
edge := gs.edges[idx]
nextNode := edge.Object

// Skip if already visited in this path
if visited[nextNode] {
continue
}

// Create new path
newNodes := make([]string, len(item.path.Nodes)+1)
copy(newNodes, item.path.Nodes)
newNodes[len(item.path.Nodes)] = nextNode

newEdges := make([]Edge, len(item.path.Edges)+1)
copy(newEdges, item.path.Edges)
newEdges[len(item.path.Edges)] = edge

newPath := Path{
Nodes:  newNodes,
Edges:  newEdges,
Weight: item.path.Weight + edge.Weight,
}

paths = append(paths, newPath)

// Continue traversal if not at max depth
if item.depth+1 < maxHops {
queue = append(queue, queueItem{
path:  newPath,
depth: item.depth + 1,
})
}
}

// Mark current as visited after processing
visited[current] = true
}

return paths
}

// TraverseParallel performs concurrent traversal from multiple starting points
func (gs *GraphStore) TraverseParallel(starts []string, maxHops int) map[string][]Path {
results := make(map[string][]Path)
var mu sync.Mutex
var wg sync.WaitGroup

for _, start := range starts {
wg.Add(1)
go func(s string) {
defer wg.Done()
paths := gs.Traverse(s, maxHops)
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
Members []string
}

// DetectCommunities runs Louvain algorithm to find communities
func (gs *GraphStore) DetectCommunities() []Community {
startTime := time.Now()
defer func() {
metrics.GraphClusteringDurationSeconds.Observe(time.Since(startTime).Seconds())
}()

gs.mu.Lock()
defer gs.mu.Unlock()

// Get all unique nodes
nodes := make(map[string]bool)
for _, e := range gs.edges {
nodes[e.Subject] = true
nodes[e.Object] = true
}

if len(nodes) == 0 {
return nil
}

// Initialize: each node in its own community
nodeList := make([]string, 0, len(nodes))
for n := range nodes {
nodeList = append(nodeList, n)
}

// Community assignment: node -> community ID
community := make(map[string]int)
for i, n := range nodeList {
community[n] = i
}

// Build adjacency with weights
adj := make(map[string]map[string]float32)
for _, e := range gs.edges {
if adj[e.Subject] == nil {
adj[e.Subject] = make(map[string]float32)
}
adj[e.Subject][e.Object] += e.Weight
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
commMembers := make(map[int][]string)
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
func (gs *GraphStore) GetCommunityForNode(node string) int {
gs.mu.RLock()
defer gs.mu.RUnlock()

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
gs.mu.RLock()
defer gs.mu.RUnlock()
return len(gs.communities)
}
