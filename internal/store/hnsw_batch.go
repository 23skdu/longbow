package store

import (
"sort"

"github.com/23skdu/longbow/internal/simd"
)

// RankedResult represents a search result with its distance score.
// Used for reranking and batch search operations that return distances.
type RankedResult struct {
ID       VectorID
Distance float32
}

// BatchDistanceCalculator provides efficient batch distance calculations
// using SIMD-optimized operations for multiple candidates at once.
// This reduces function call overhead and maximizes CPU pipeline throughput.
type BatchDistanceCalculator struct {
dims int
}

// NewBatchDistanceCalculator creates a new batch distance calculator
// for vectors of the specified dimensionality.
func NewBatchDistanceCalculator(dims int) *BatchDistanceCalculator {
return &BatchDistanceCalculator{dims: dims}
}

// Dims returns the vector dimensionality this calculator expects.
func (b *BatchDistanceCalculator) Dims() int {
return b.dims
}

// ComputeDistances calculates Euclidean distances from a query vector
// to multiple candidate vectors using SIMD batch operations.
// Returns a slice of distances in the same order as candidates.
func (b *BatchDistanceCalculator) ComputeDistances(query []float32, candidates [][]float32) []float32 {
if len(candidates) == 0 {
return []float32{}
}

results := make([]float32, len(candidates))
simd.EuclideanDistanceBatch(query, candidates, results)
return results
}

// ComputeDistancesInto calculates distances into a pre-allocated buffer.
// The buffer must be at least len(candidates) in size.
// This avoids allocation overhead for repeated operations.
func (b *BatchDistanceCalculator) ComputeDistancesInto(query []float32, candidates [][]float32, buffer []float32) {
if len(candidates) == 0 {
return
}
simd.EuclideanDistanceBatch(query, candidates, buffer)
}

// ComputeDistanceMatrix calculates distances from multiple query vectors
// to multiple candidate vectors, returning a 2D matrix.
// Result[i][j] = distance from queries[i] to candidates[j].
func (b *BatchDistanceCalculator) ComputeDistanceMatrix(queries, candidates [][]float32) [][]float32 {
if len(queries) == 0 || len(candidates) == 0 {
return make([][]float32, len(queries))
}

results := make([][]float32, len(queries))
for i, query := range queries {
results[i] = make([]float32, len(candidates))
simd.EuclideanDistanceBatch(query, candidates, results[i])
}
return results
}

// SearchBatch performs k-NN search for multiple query vectors.
// This is more efficient than calling Search multiple times when
// you have multiple queries, as it can batch distance calculations.
// Returns a slice of result slices, one per query.
func (h *HNSWIndex) SearchBatch(queries [][]float32, k int) [][]VectorID {
if len(queries) == 0 || k <= 0 {
return nil
}

results := make([][]VectorID, len(queries))

for i, query := range queries {
neighbors := h.Graph.Search(query, k)
results[i] = make([]VectorID, len(neighbors))
for j, n := range neighbors {
results[i][j] = n.Key
}
}

return results
}

// SearchBatchWithArena performs batch k-NN search using arena allocation.
// Results are allocated from the arena to reduce GC pressure.
// If arena is nil or exhausted, falls back to heap allocation.
func (h *HNSWIndex) SearchBatchWithArena(queries [][]float32, k int, arena *SearchArena) [][]VectorID {
if len(queries) == 0 || k <= 0 {
return nil
}

results := make([][]VectorID, len(queries))

for i, query := range queries {
neighbors := h.Graph.Search(query, k)

// Try arena allocation
var res []VectorID
if arena != nil {
res = arena.AllocVectorIDSlice(len(neighbors))
}

// Fallback to heap
if res == nil {
res = make([]VectorID, len(neighbors))
}

for j, n := range neighbors {
res[j] = n.Key
}
results[i] = res
}

return results
}

// RerankBatch takes candidate IDs and reranks them using batch distance
// calculation, returning the top-k results sorted by distance.
// This is useful for two-stage retrieval: coarse retrieval + fine reranking.
func (h *HNSWIndex) RerankBatch(query []float32, candidateIDs []VectorID, k int) []RankedResult {
if len(candidateIDs) == 0 || k <= 0 {
return nil
}

// Clamp k to available candidates
if k > len(candidateIDs) {
k = len(candidateIDs)
}

// Collect candidate vectors
candidateVectors := make([][]float32, 0, len(candidateIDs))
validIDs := make([]VectorID, 0, len(candidateIDs))

for _, id := range candidateIDs {
vec := h.getVector(id)
if vec != nil {
candidateVectors = append(candidateVectors, vec)
validIDs = append(validIDs, id)
}
}

if len(candidateVectors) == 0 {
return nil
}

// Batch distance calculation
distances := make([]float32, len(candidateVectors))
simd.EuclideanDistanceBatch(query, candidateVectors, distances)

// Build ranked results
ranked := make([]RankedResult, len(validIDs))
for i, id := range validIDs {
ranked[i] = RankedResult{ID: id, Distance: distances[i]}
}

// Sort by distance ascending
sort.Slice(ranked, func(i, j int) bool {
return ranked[i].Distance < ranked[j].Distance
})

// Return top-k
if k > len(ranked) {
k = len(ranked)
}

return ranked[:k]
}
