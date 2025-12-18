package store

import (
"fmt"
"sync/atomic"

"github.com/23skdu/longbow/internal/metrics"
"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
)

// =============================================================================
// VectorIndex Interface
// =============================================================================

// VectorIndex defines the interface for vector index implementations.
// Both HNSWIndex and ShardedHNSW implement this interface.
type VectorIndex interface {
// AddByLocation adds a vector from the dataset using batch and row indices.
AddByLocation(batchIdx, rowIdx int) error

// SearchVectors returns the k nearest neighbors for the query vector.
SearchVectors(query []float32, k int) []SearchResult

// Len returns the number of vectors in the index.
Len() int
}

// =============================================================================
// HNSWIndex VectorIndex Implementation
// =============================================================================

// AddByLocation implements VectorIndex interface for HNSWIndex.
func (h *HNSWIndex) AddByLocation(batchIdx, rowIdx int) error {
return h.Add(batchIdx, rowIdx)
}

// SearchVectors implements VectorIndex interface for HNSWIndex.
// It adapts the existing SearchWithArena to return SearchResult.
func (h *HNSWIndex) SearchVectors(query []float32, k int) []SearchResult {
arena := NewSearchArena(1024 * 1024) // 1MB arena
defer arena.Reset()

ids := h.SearchWithArena(query, k, arena)
results := make([]SearchResult, len(ids))
for i, id := range ids {
results[i] = SearchResult{
ID:    id,
Score: 0, // Score not available from SearchWithArena
}
}
return results
}

// =============================================================================
// ShardedHNSW VectorIndex Implementation
// =============================================================================

// AddByLocation implements VectorIndex interface for ShardedHNSW.
// It extracts the vector from the dataset and adds it to the appropriate shard.
func (s *ShardedHNSW) AddByLocation(batchIdx, rowIdx int) error {
vec := s.getVectorFromDataset(batchIdx, rowIdx)
if vec == nil {
return fmt.Errorf("failed to get vector at batch %d, row %d", batchIdx, rowIdx)
}

loc := Location{BatchIdx: batchIdx, RowIdx: rowIdx}
_, err := s.Add(loc, vec)
return err
}

// SearchVectors implements VectorIndex interface for ShardedHNSW.
func (s *ShardedHNSW) SearchVectors(query []float32, k int) []SearchResult {
return s.Search(query, k)
}

// getVectorFromDataset extracts a vector from the dataset.
func (s *ShardedHNSW) getVectorFromDataset(batchIdx, rowIdx int) []float32 {
if s.dataset == nil || batchIdx >= len(s.dataset.Records) {
return nil
}

rec := s.dataset.Records[batchIdx]
vecColIdx := -1
for i := 0; i < int(rec.NumCols()); i++ {
if rec.ColumnName(i) == "vector" {
vecColIdx = i
break
}
}

if vecColIdx < 0 {
return nil
}

return extractVectorFromCol(rec.Column(vecColIdx), rowIdx)
}

// extractVectorFromCol extracts a float32 vector from an Arrow column.
func extractVectorFromCol(col arrow.Array, rowIdx int) []float32 {
if col == nil || rowIdx >= col.Len() {
return nil
}

if arr, ok := col.(*array.FixedSizeList); ok {
// Get the flat values array
values, ok := arr.ListValues().(*array.Float32)
if !ok {
return nil
}
listSize := int(arr.DataType().(*arrow.FixedSizeListType).Len())
start := rowIdx * listSize
end := start + listSize
if end > values.Len() {
return nil
}
vec := make([]float32, listSize)
for i := 0; i < listSize; i++ {
vec[i] = values.Value(start + i)
}
return vec
}
return nil
}

// =============================================================================
// Dataset Auto-Sharding Support
// =============================================================================

// IsSharded returns true if the dataset uses ShardedHNSW.
func (d *Dataset) IsSharded() bool {
return d.shardedIndex != nil
}

// IndexLen returns the number of vectors in the index.
func (d *Dataset) IndexLen() int {
if d.shardedIndex != nil {
return d.shardedIndex.Len()
}
if d.Index != nil {
return d.Index.Len()
}
return 0
}

// SearchDataset performs k-NN search on the appropriate index.
func (d *Dataset) SearchDataset(query []float32, k int) []SearchResult {
if d.shardedIndex != nil {
return d.shardedIndex.SearchVectors(query, k)
}
if d.Index != nil {
return d.Index.SearchVectors(query, k)
}
return nil
}

// GetVectorIndex returns the active index implementing VectorIndex.
func (d *Dataset) GetVectorIndex() VectorIndex {
if d.shardedIndex != nil {
return d.shardedIndex
}
if d.Index != nil {
return d.Index
}
return nil
}

// AddToIndex adds a vector to the active index.
func (d *Dataset) AddToIndex(batchIdx, rowIdx int) error {
idx := d.GetVectorIndex()
if idx == nil {
return fmt.Errorf("no index available")
}
return idx.AddByLocation(batchIdx, rowIdx)
}

// MigrateToShardedIndex converts the HNSWIndex to ShardedHNSW.
func (d *Dataset) MigrateToShardedIndex(cfg AutoShardingConfig) error {
if d.shardedIndex != nil {
return nil // Already sharded
}
if d.Index == nil {
return fmt.Errorf("no index to migrate")
}

sharded, err := MigrateToSharded(d.Index, cfg)
if err != nil {
return fmt.Errorf("migration failed: %w", err)
}

// Atomic swap
d.shardedIndex = sharded
d.Index = nil // Clear old index

return nil
}

// =============================================================================
// VectorStore Auto-Sharding Support
// =============================================================================

// autoShardingConfig stores the auto-sharding configuration.
var autoShardingConfig atomic.Value

func init() {
// Initialize with default config
autoShardingConfig.Store(DefaultAutoShardingConfig())
}

// SetAutoShardingConfig sets the auto-sharding configuration.
func (vs *VectorStore) SetAutoShardingConfig(cfg AutoShardingConfig) {
autoShardingConfig.Store(cfg)
}

// GetAutoShardingConfig returns the current auto-sharding configuration.
func (vs *VectorStore) GetAutoShardingConfig() AutoShardingConfig {
return autoShardingConfig.Load().(AutoShardingConfig)
}

// getDataset retrieves a dataset by name.
func (vs *VectorStore) getDataset(name string) (*Dataset, error) {
ds, ok := vs.vectors.Get(name)
if !ok {
return nil, fmt.Errorf("dataset %q not found", name)
}
return ds, nil
}

// checkAndMigrateToSharded checks if auto-sharding should trigger.
func (vs *VectorStore) checkAndMigrateToSharded(ds *Dataset) error {
cfg := vs.GetAutoShardingConfig()
if !cfg.Enabled {
return nil
}

if ds.IsSharded() {
return nil // Already sharded
}

vectorCount := ds.IndexLen()
if !cfg.ShouldShard(vectorCount) {
return nil // Below threshold
}

// Trigger migration
if err := ds.MigrateToShardedIndex(cfg); err != nil {
return fmt.Errorf("auto-sharding migration failed: %w", err)
}

metrics.HnswShardingMigrationsTotal.Inc()
return nil
}


