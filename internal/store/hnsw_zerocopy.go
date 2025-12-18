package store

import (
"github.com/23skdu/longbow/internal/metrics"
"github.com/23skdu/longbow/internal/simd"
"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
"sort"
)

// getVectorDirect returns a direct pointer to vector data without copying.
// CALLER MUST hold epoch protection (call enterEpoch before, exitEpoch after).
// This is for batch operations where external epoch management is more efficient.
func (h *HNSWIndex) getVectorDirect(id VectorID) []float32 {
h.mu.RLock()
if int(id) >= len(h.locations) {
h.mu.RUnlock()
return nil
}
loc := h.locations[id]
h.mu.RUnlock()

h.dataset.mu.RLock()
defer h.dataset.mu.RUnlock()

if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
return nil
}
rec := h.dataset.Records[loc.BatchIdx]

var vecCol *array.FixedSizeList
for i, field := range rec.Schema().Fields() {
if field.Name == "vector" {
if fsl, ok := rec.Column(i).(*array.FixedSizeList); ok {
vecCol = fsl
}
break
}
}

if vecCol == nil {
return nil
}

values, ok := vecCol.ListValues().(*array.Float32)
if !ok || loc.RowIdx >= vecCol.Len() {
return nil
}

listSize := int(vecCol.DataType().(*arrow.FixedSizeListType).Len())
start := loc.RowIdx * listSize
end := start + listSize

// Direct slice into Arrow buffer - zero copy!
return values.Float32Values()[start:end]
}

// RerankBatchZeroCopy performs batch reranking with zero-copy vector access.
// Uses single epoch protection for all vector accesses, minimizing overhead.
func (h *HNSWIndex) RerankBatchZeroCopy(query []float32, candidateIDs []VectorID, k int) []RankedResult {
if len(candidateIDs) == 0 || k <= 0 {
return nil
}

// Clamp k to available candidates
if k > len(candidateIDs) {
k = len(candidateIDs)
}

// Enter epoch once for all vector accesses
h.enterEpoch()
defer h.exitEpoch()

// Collect candidate vectors with zero-copy
candidateVectors := make([][]float32, 0, len(candidateIDs))
validIDs := make([]VectorID, 0, len(candidateIDs))

for _, id := range candidateIDs {
vec := h.getVectorDirect(id)
if vec != nil {
candidateVectors = append(candidateVectors, vec)
validIDs = append(validIDs, id)
// Track zero-copy access
metrics.HnswZeroCopyAccessTotal.WithLabelValues(h.dataset.Name).Inc()
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
