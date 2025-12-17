package store

import (
"container/heap"
"errors"
"fmt"
"runtime"
"sync"
"sync/atomic"

"github.com/coder/hnsw"
)

// ShardedHNSWConfig configures the sharded HNSW index.
type ShardedHNSWConfig struct {
NumShards      int // Number of independent HNSW shards
M              int // HNSW M parameter (max connections per node)
EfConstruction int // HNSW efConstruction parameter
}

// DefaultShardedHNSWConfig returns sensible defaults.
func DefaultShardedHNSWConfig() ShardedHNSWConfig {
return ShardedHNSWConfig{
NumShards:      runtime.NumCPU(),
M:              16,
EfConstruction: 200,
}
}

// Validate checks configuration validity.
func (c ShardedHNSWConfig) Validate() error {
if c.NumShards <= 0 {
return errors.New("NumShards must be positive")
}
if c.M <= 0 {
return errors.New("m must be positive")
}
if c.EfConstruction <= 0 {
return errors.New("EfConstruction must be positive")
}
return nil
}

// hnswShard represents a single HNSW graph with its own lock.
type hnswShard struct {
mu        sync.RWMutex
graph     *hnsw.Graph[VectorID]
locations []Location // Local locations for this shard
localIDs  []VectorID // Maps local index to global VectorID
vectors   [][]float32 // Store vectors for distance calculation
}

// ShardStat holds statistics for a single shard.
type ShardStat struct {
ShardID int
Count   int
}

// ShardedHNSW provides fine-grained locking via multiple independent HNSW shards.
// Each shard has its own lock, enabling parallel insertions to different shards.
type ShardedHNSW struct {
config     ShardedHNSWConfig
shards     []*hnswShard
dataset    *Dataset          // Optional reference for vector retrieval
nextID     atomic.Int64      // Global ID counter
idToShard  sync.Map          // VectorID -> shard index
idToLocal  sync.Map          // VectorID -> local index within shard
globalLocs []Location        // Global location storage
globalMu   sync.RWMutex      // Protects globalLocs
}

// NewShardedHNSW creates a new sharded HNSW index.
func NewShardedHNSW(config ShardedHNSWConfig, dataset *Dataset) *ShardedHNSW {
if config.NumShards <= 0 {
config.NumShards = runtime.NumCPU()
}
if config.M <= 0 {
config.M = 16
}
if config.EfConstruction <= 0 {
config.EfConstruction = 200
}

shards := make([]*hnswShard, config.NumShards)
for i := 0; i < config.NumShards; i++ {
shards[i] = &hnswShard{
graph:     hnsw.NewGraph[VectorID](),
locations: make([]Location, 0, 1024),
localIDs:  make([]VectorID, 0, 1024),
vectors:   make([][]float32, 0, 1024),
}
}

return &ShardedHNSW{
config:     config,
shards:     shards,
dataset:    dataset,
globalLocs: make([]Location, 0, 4096),
}
}

// GetShardForID returns the shard index for a given VectorID using consistent hashing.
func (s *ShardedHNSW) GetShardForID(id VectorID) int {
// FNV-1a inspired hash for good distribution
hash := uint64(id)
hash ^= hash >> 33
hash *= 0xff51afd7ed558ccd
hash ^= hash >> 33
hash *= 0xc4ceb9fe1a85ec53
hash ^= hash >> 33
return int(hash % uint64(s.config.NumShards))
}

// Add inserts a vector into the appropriate shard based on the assigned ID.
// Returns the assigned VectorID.
func (s *ShardedHNSW) Add(loc Location, vec []float32) (VectorID, error) {
if len(vec) == 0 {
return 0, errors.New("empty vector")
}

// Allocate global ID atomically
id := VectorID(s.nextID.Add(1) - 1)

// Store location globally
s.globalMu.Lock()
for len(s.globalLocs) <= int(id) {
s.globalLocs = append(s.globalLocs, Location{})
}
s.globalLocs[id] = loc
s.globalMu.Unlock()

// Determine target shard
shardIdx := s.GetShardForID(id)
shard := s.shards[shardIdx]

// Make a copy of the vector for storage
vecCopy := make([]float32, len(vec))
copy(vecCopy, vec)

// Add to shard with fine-grained lock
shard.mu.Lock()
localIdx := len(shard.localIDs)
shard.locations = append(shard.locations, loc)
shard.localIDs = append(shard.localIDs, id)
shard.vectors = append(shard.vectors, vecCopy)

// Add to HNSW graph
shard.graph.Add(hnsw.MakeNode(id, vecCopy))
shard.mu.Unlock()

// Store mappings
s.idToShard.Store(id, shardIdx)
s.idToLocal.Store(id, localIdx)

return id, nil
}

// shardSearchResult for heap-based merging
type shardSearchResult struct {
ID       VectorID
Score    float32 // Lower = better (distance)
}

// shardResultHeap implements a max-heap for k-NN result merging
type shardResultHeap []shardSearchResult

func (h shardResultHeap) Len() int           { return len(h) }
func (h shardResultHeap) Less(i, j int) bool { return h[i].Score > h[j].Score } // Max-heap
func (h shardResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *shardResultHeap) Push(x interface{}) {
*h = append(*h, x.(shardSearchResult))
}

func (h *shardResultHeap) Pop() interface{} {
old := *h
n := len(old)
x := old[n-1]
*h = old[0 : n-1]
return x
}

// Search performs k-NN search across all shards and merges results.
func (s *ShardedHNSW) Search(query []float32, k int) []SearchResult {
if k <= 0 || len(query) == 0 {
return nil
}

// Search each shard in parallel
type shardResults struct {
nodes []hnsw.Node[VectorID]
}

allResults := make([]shardResults, len(s.shards))
var wg sync.WaitGroup

for i, shard := range s.shards {
wg.Add(1)
go func(idx int, sh *hnswShard) {
defer wg.Done()
sh.mu.RLock()
if sh.graph.Len() > 0 {
allResults[idx].nodes = sh.graph.Search(query, k)
}
sh.mu.RUnlock()
}(i, shard)
}

wg.Wait()

// Merge results using max-heap to keep top-k
// Use search order rank as score (lower index = better = lower score)
h := &shardResultHeap{}
heap.Init(h)

for shardIdx, sr := range allResults {
for rank, node := range sr.nodes {
// Get vector from shard storage for distance calculation
dist := float32(rank) // Default: use rank as proxy

// Try to get actual distance from stored vector
if localIdxVal, ok := s.idToLocal.Load(node.Key); ok {
localIdx := localIdxVal.(int)
shard := s.shards[shardIdx]
shard.mu.RLock()
if localIdx < len(shard.vectors) {
dist = shardedEuclideanDist(query, shard.vectors[localIdx])
}
shard.mu.RUnlock()
}

if h.Len() < k {
heap.Push(h, shardSearchResult{ID: node.Key, Score: dist})
} else if dist < (*h)[0].Score {
heap.Pop(h)
heap.Push(h, shardSearchResult{ID: node.Key, Score: dist})
}
}
}

// Extract and sort results by score (ascending = closer)
results := make([]SearchResult, h.Len())
for i := len(results) - 1; i >= 0; i-- {
r := heap.Pop(h).(shardSearchResult)
results[i] = SearchResult(r)
}

return results
}

// shardedEuclideanDist computes L2 distance between two vectors.
func shardedEuclideanDist(a, b []float32) float32 {
var sum float32
for i := range a {
if i < len(b) {
d := a[i] - b[i]
sum += d * d
}
}
return sum
}

// GetLocation retrieves the location for a given VectorID.
func (s *ShardedHNSW) GetLocation(id VectorID) (Location, bool) {
s.globalMu.RLock()
defer s.globalMu.RUnlock()

if int(id) >= len(s.globalLocs) {
return Location{}, false
}
return s.globalLocs[id], true
}

// Len returns the total number of vectors across all shards.
func (s *ShardedHNSW) Len() int {
return int(s.nextID.Load())
}

// ShardStats returns per-shard statistics.
func (s *ShardedHNSW) ShardStats() []ShardStat {
stats := make([]ShardStat, len(s.shards))
for i, shard := range s.shards {
shard.mu.RLock()
stats[i] = ShardStat{
ShardID: i,
Count:   len(shard.localIDs),
}
shard.mu.RUnlock()
}
return stats
}

// String returns a description of the sharded index.
func (s *ShardedHNSW) String() string {
total := s.Len()
return fmt.Sprintf("ShardedHNSW{shards=%d, vectors=%d, M=%d, ef=%d}",
s.config.NumShards, total, s.config.M, s.config.EfConstruction)
}
