package store

import (
"fmt"
"time"
	"go.uber.org/zap"
)

// WarmupStats contains statistics from a warmup operation
type WarmupStats struct {
DatasetsWarmed   int
DatasetsSkipped  int
TotalNodesWarmed int
Duration         time.Duration
}

// String returns a human-readable representation of WarmupStats
func (s WarmupStats) String() string {
return fmt.Sprintf("warmed %d datasets (%d skipped), %d nodes in %v",
s.DatasetsWarmed, s.DatasetsSkipped, s.TotalNodesWarmed, s.Duration)
}

// Warmup traverses all nodes in the HNSW graph to bring them into CPU cache.
// Returns the number of nodes touched during warmup.
func (h *HNSWIndex) Warmup() int {
h.mu.RLock()
defer h.mu.RUnlock()

count := h.Graph.Len()
if count == 0 {
return 0
}

// Iterate through all vector IDs and lookup each one
// This brings the graph nodes into CPU cache
// VectorIDs are sequential starting from 0
touched := 0
for i := 0; i < count; i++ {
// Lookup triggers memory access to the graph node
if _, ok := h.Graph.Lookup(VectorID(i)); ok {
touched++
}
}

return touched
}

// Warmup traverses all datasets and their indexes to bring graph nodes into CPU cache.
// This eliminates cold-start latency spikes after server restart.
func (s *VectorStore) Warmup() WarmupStats {
start := time.Now()
stats := WarmupStats{}

s.vectors.Range(func(name string, ds *Dataset) bool {
if ds.Index != nil {
nodes := ds.Index.Warmup()
stats.TotalNodesWarmed += nodes
stats.DatasetsWarmed++
} else {
stats.DatasetsSkipped++
}
return true
})

stats.Duration = time.Since(start)

if stats.DatasetsWarmed > 0 {
s.logger.Info("Index warmup complete",
		zap.Any("datasets_warmed", stats.DatasetsWarmed),
		zap.Any("datasets_skipped", stats.DatasetsSkipped),
		zap.Any("total_nodes", stats.TotalNodesWarmed),
		zap.Duration("duration", stats.Duration))
}

return stats
}
