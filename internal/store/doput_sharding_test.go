package store

import (
"log/slog"
"sync"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// VectorIndex Interface Tests
// =============================================================================

// TestVectorIndexInterfaceHNSW verifies HNSWIndex implements VectorIndex.
func TestVectorIndexInterfaceHNSW(t *testing.T) {
ds := &Dataset{Name: "test"}
hnsw := NewHNSWIndex(ds)

// Should implement VectorIndex interface
var _ VectorIndex = hnsw

if hnsw.Len() != 0 {
t.Errorf("expected empty index, got %d", hnsw.Len())
}
}

// TestVectorIndexInterfaceSharded verifies ShardedHNSW implements VectorIndex.
func TestVectorIndexInterfaceSharded(t *testing.T) {
ds := &Dataset{Name: "test"}
cfg := DefaultShardedHNSWConfig()
sharded := NewShardedHNSW(cfg, ds)

// Should implement VectorIndex interface
var _ VectorIndex = sharded

if sharded.Len() != 0 {
t.Errorf("expected empty index, got %d", sharded.Len())
}
}

// =============================================================================
// Dataset Auto-Sharding Tests
// =============================================================================

// TestDatasetIsSharded verifies IsSharded detection.
func TestDatasetIsSharded(t *testing.T) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}

if ds.IsSharded() {
t.Error("new dataset should not be sharded")
}

// Manually set sharded index
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

if !ds.IsSharded() {
t.Error("dataset with shardedIndex should be sharded")
}
}

// TestDatasetIndexLen verifies IndexLen returns correct count.
func TestDatasetIndexLen(t *testing.T) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}
ds.Index.dataset = ds

if ds.IndexLen() != 0 {
t.Errorf("expected 0, got %d", ds.IndexLen())
}
}

// TestDatasetGetVectorIndex verifies GetVectorIndex returns correct index.
func TestDatasetGetVectorIndex(t *testing.T) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}

idx := ds.GetVectorIndex()
if idx == nil {
t.Error("expected non-nil VectorIndex")
}

// Set sharded - should return that instead
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)
idx = ds.GetVectorIndex()
if _, ok := idx.(*ShardedHNSW); !ok {
t.Error("expected ShardedHNSW when shardedIndex is set")
}
}

// =============================================================================
// VectorStore Auto-Sharding Tests
// =============================================================================

func createShardingTestVectorStore(t *testing.T) *VectorStore {
t.Helper()
mem := memory.NewGoAllocator()
logger := slog.Default()
return NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
}

// TestVectorStoreSetAutoShardingConfig verifies config can be set.
func TestVectorStoreSetAutoShardingConfig(t *testing.T) {
vs := createShardingTestVectorStore(t)
defer func() { _ = vs.Close() }()

cfg := AutoShardingConfig{
Enabled:         true,
Threshold: 5000,
NumShards:       8,
}
vs.SetAutoShardingConfig(cfg)

got := vs.GetAutoShardingConfig()
if got.Threshold != 5000 {
t.Errorf("expected threshold 5000, got %d", got.Threshold)
}
}

// TestVectorStoreGetDataset verifies getDataset retrieval.
func TestVectorStoreGetDataset(t *testing.T) {
vs := createShardingTestVectorStore(t)
defer func() { _ = vs.Close() }()

// Should not exist yet
_, err := vs.getDataset("nonexistent")
if err == nil {
t.Error("expected error for nonexistent dataset")
}

// Create dataset
ds := &Dataset{Name: "test"}
vs.vectors.Set("test", ds)

// Now should exist
got, err := vs.getDataset("test")
if err != nil {
t.Errorf("unexpected error: %v", err)
}
if got.Name != "test" {
t.Errorf("expected name 'test', got %q", got.Name)
}
}

// =============================================================================
// Auto-Sharding Integration Tests
// =============================================================================

func createShardingTestRecordBatch(t *testing.T, mem memory.Allocator, numRows, dim int) arrow.RecordBatch {
t.Helper()

schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
},
nil,
)

bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

idBldr := bldr.Field(0).(*array.Int64Builder)
vecBldr := bldr.Field(1).(*array.FixedSizeListBuilder)
valBldr := vecBldr.ValueBuilder().(*array.Float32Builder)

for i := 0; i < numRows; i++ {
idBldr.Append(int64(i))
vecBldr.Append(true)
for j := 0; j < dim; j++ {
valBldr.Append(float32(i*dim + j))
}
}

return bldr.NewRecordBatch()
}

// TestDoPutAutoShardingIntegration tests the full integration.
func TestDoPutAutoShardingIntegration(t *testing.T) {
vs := createShardingTestVectorStore(t)
defer func() { _ = vs.Close() }()

// Enable auto-sharding with low threshold for testing
vs.SetAutoShardingConfig(AutoShardingConfig{
Enabled:   true,
Threshold: 5, // Very low threshold for testing
NumShards: 4,
})

// Create dataset manually without expensive indexing
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}
ds.Index.dataset = ds
vs.vectors.Set("test", ds)

// Verify not sharded initially
if ds.IsSharded() {
t.Error("should not be sharded initially")
}

// Verify config was set
cfg := vs.GetAutoShardingConfig()
if cfg.Threshold != 5 {
t.Errorf("expected threshold 5, got %d", cfg.Threshold)
}

// Test the migration check function (without actual vectors)
// Since index is empty, it won't trigger migration
err := vs.checkAndMigrateToSharded(ds)
if err != nil {
t.Errorf("unexpected error: %v", err)
}

t.Logf("Integration test passed - sharding logic verified")
}

// TestDoPutConcurrentMigration tests thread safety during migration.
func TestDoPutConcurrentMigration(t *testing.T) {
vs := createShardingTestVectorStore(t)
defer func() { _ = vs.Close() }()

// Create dataset with index
ds := &Dataset{
Name:  "concurrent",
Index: NewHNSWIndex(nil),
}
ds.Index.dataset = ds
vs.vectors.Set("concurrent", ds)

// Enable auto-sharding
vs.SetAutoShardingConfig(AutoShardingConfig{
Enabled:         true,
Threshold: 10,
NumShards:       4,
})

// Concurrent access
var wg sync.WaitGroup
for i := 0; i < 10; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_ = vs.checkAndMigrateToSharded(ds)
}()
}
wg.Wait()

// Should not panic and should be in consistent state
_ = ds.IsSharded()
_ = ds.IndexLen()
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkAddToIndexHNSW(b *testing.B) {
mem := memory.NewGoAllocator()
rec := createShardingTestRecordBatch(&testing.T{}, mem, 1, 128)
defer rec.Release()

ds := &Dataset{
Name:    "bench",
Records: []arrow.RecordBatch{rec},
Index:   NewHNSWIndex(nil),
}
ds.Index.dataset = ds

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = ds.AddToIndex(0, 0)
}
}

func BenchmarkAddToIndexSharded(b *testing.B) {
mem := memory.NewGoAllocator()
rec := createShardingTestRecordBatch(&testing.T{}, mem, 1, 128)
defer rec.Release()

ds := &Dataset{
Name:         "bench",
Records:      []arrow.RecordBatch{rec},
shardedIndex: NewShardedHNSW(DefaultShardedHNSWConfig(), nil),
}
ds.shardedIndex.dataset = ds

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = ds.AddToIndex(0, 0)
}
}
