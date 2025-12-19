package store

import (
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
"go.uber.org/zap"
)

// =============================================================================
// extractVectorFromCol Tests
// =============================================================================

func TestExtractVectorFromCol_Nil(t *testing.T) {
result := extractVectorFromCol(nil, 0)
if result != nil {
t.Error("expected nil for nil column")
}
}

func TestExtractVectorFromCol_OutOfBounds(t *testing.T) {
mem := memory.NewGoAllocator()

// Create a small fixed size list array
listType := arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)
bldr := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
defer bldr.Release()

valBldr := bldr.ValueBuilder().(*array.Float32Builder)
bldr.Append(true)
valBldr.Append(1.0)
valBldr.Append(2.0)
valBldr.Append(3.0)

arr := bldr.NewArray()
defer arr.Release()

// Test out of bounds
result := extractVectorFromCol(arr, 999)
if result != nil {
t.Error("expected nil for out of bounds index")
}

_ = listType // silence unused
}

func TestExtractVectorFromCol_WrongType(t *testing.T) {
mem := memory.NewGoAllocator()

// Create a non-list array (Int64)
bldr := array.NewInt64Builder(mem)
defer bldr.Release()
bldr.Append(123)

arr := bldr.NewArray()
defer arr.Release()

// Should return nil for non-FixedSizeList
result := extractVectorFromCol(arr, 0)
if result != nil {
t.Error("expected nil for non-FixedSizeList column")
}
}

func TestExtractVectorFromCol_Success(t *testing.T) {
mem := memory.NewGoAllocator()

bldr := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
defer bldr.Release()

valBldr := bldr.ValueBuilder().(*array.Float32Builder)
bldr.Append(true)
valBldr.Append(1.0)
valBldr.Append(2.0)
valBldr.Append(3.0)

arr := bldr.NewArray()
defer arr.Release()

result := extractVectorFromCol(arr, 0)
if result == nil {
t.Fatal("expected non-nil result")
}
if len(result) != 3 {
t.Errorf("expected 3 elements, got %d", len(result))
}
if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
t.Errorf("unexpected values: %v", result)
}
}

// =============================================================================
// Dataset.SearchDataset Tests
// =============================================================================

func TestDatasetSearchDataset_NoIndex(t *testing.T) {
ds := &Dataset{Name: "test"}

results := ds.SearchDataset([]float32{1, 2, 3}, 10)
if results != nil {
t.Error("expected nil results with no index")
}
}

func TestDatasetSearchDataset_WithHNSW(t *testing.T) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}
ds.Index.dataset = ds

// Empty index should return empty results
results := ds.SearchDataset([]float32{1, 2, 3}, 10)
if len(results) != 0 {
t.Errorf("expected empty results, got %d", len(results))
}
}

func TestDatasetSearchDataset_WithSharded(t *testing.T) {
ds := &Dataset{Name: "test"}
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

// Empty index should return empty results
results := ds.SearchDataset([]float32{1, 2, 3}, 10)
if len(results) != 0 {
t.Errorf("expected empty results, got %d", len(results))
}
}

// =============================================================================
// Dataset.AddToIndex Tests
// =============================================================================

func TestDatasetAddToIndex_NoIndex(t *testing.T) {
ds := &Dataset{Name: "test"}

err := ds.AddToIndex(0, 0)
if err == nil {
t.Error("expected error when no index available")
}
}

// =============================================================================
// Dataset.MigrateToShardedIndex Tests
// =============================================================================

func TestDatasetMigrateToShardedIndex_AlreadySharded(t *testing.T) {
ds := &Dataset{Name: "test"}
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

// Should return nil (no-op) when already sharded
err := ds.MigrateToShardedIndex(DefaultAutoShardingConfig())
if err != nil {
t.Errorf("unexpected error: %v", err)
}
}

func TestDatasetMigrateToShardedIndex_NoIndex(t *testing.T) {
ds := &Dataset{Name: "test"}

err := ds.MigrateToShardedIndex(DefaultAutoShardingConfig())
if err == nil {
t.Error("expected error when no index to migrate")
}
}

// =============================================================================
// ShardedHNSW.getVectorFromDataset Tests
// =============================================================================

func TestShardedHNSW_GetVectorFromDataset_NilDataset(t *testing.T) {
s := &ShardedHNSW{dataset: nil}

result := s.getVectorFromDataset(0, 0)
if result != nil {
t.Error("expected nil for nil dataset")
}
}

func TestShardedHNSW_GetVectorFromDataset_OutOfBounds(t *testing.T) {
ds := &Dataset{Name: "test", Records: nil}
s := &ShardedHNSW{dataset: ds}

result := s.getVectorFromDataset(999, 0)
if result != nil {
t.Error("expected nil for out of bounds batch")
}
}

func TestShardedHNSW_GetVectorFromDataset_NoVectorColumn(t *testing.T) {
mem := memory.NewGoAllocator()

// Create record without vector column
schema := arrow.NewSchema(
[]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}},
nil,
)
bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

bldr.Field(0).(*array.Int64Builder).Append(1)
rec := bldr.NewRecord() //nolint:staticcheck
defer rec.Release()

//nolint:staticcheck // test uses existing codebase patterns
	ds := &Dataset{Name: "test", Records: []arrow.Record{rec}} //nolint:staticcheck
s := &ShardedHNSW{dataset: ds}

result := s.getVectorFromDataset(0, 0)
if result != nil {
t.Error("expected nil when no vector column")
}
}

// =============================================================================
// VectorStore.checkAndMigrateToSharded Tests
// =============================================================================

func TestCheckAndMigrateToSharded_Disabled(t *testing.T) {
mem := memory.NewGoAllocator()
logger := zap.NewNop()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
defer func() { _ = vs.Close() }()

vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: false})

ds := &Dataset{Name: "test", Index: NewHNSWIndex(nil)}

err := vs.checkAndMigrateToSharded(ds)
if err != nil {
t.Errorf("unexpected error: %v", err)
}
if ds.IsSharded() {
t.Error("should not shard when disabled")
}
}

func TestCheckAndMigrateToSharded_AlreadySharded(t *testing.T) {
mem := memory.NewGoAllocator()
logger := zap.NewNop()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
defer func() { _ = vs.Close() }()

vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: true, Threshold: 1})

ds := &Dataset{Name: "test"}
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

err := vs.checkAndMigrateToSharded(ds)
if err != nil {
t.Errorf("unexpected error: %v", err)
}
}

func TestCheckAndMigrateToSharded_BelowThreshold(t *testing.T) {
mem := memory.NewGoAllocator()
logger := zap.NewNop()
vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
defer func() { _ = vs.Close() }()

vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: true, Threshold: 1000000})

ds := &Dataset{Name: "test", Index: NewHNSWIndex(nil)}
ds.Index.dataset = ds

err := vs.checkAndMigrateToSharded(ds)
if err != nil {
t.Errorf("unexpected error: %v", err)
}
if ds.IsSharded() {
t.Error("should not shard when below threshold")
}
}

// =============================================================================
// HNSWIndex.SearchVectors Tests
// =============================================================================

func TestHNSWIndex_SearchVectors_Empty(t *testing.T) {
ds := &Dataset{Name: "test"}
hnsw := NewHNSWIndex(ds)

results := hnsw.SearchVectors([]float32{1, 2, 3}, 10)
if len(results) != 0 {
t.Errorf("expected empty results, got %d", len(results))
}
}

// =============================================================================
// ShardedHNSW.SearchVectors Tests
// =============================================================================

func TestShardedHNSW_SearchVectors_Empty(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
ds := &Dataset{Name: "test"}
sharded := NewShardedHNSW(cfg, ds)

results := sharded.SearchVectors([]float32{1, 2, 3}, 10)
if len(results) != 0 {
t.Errorf("expected empty results, got %d", len(results))
}
}

// =============================================================================
// Dataset.GetVectorIndex Edge Cases
// =============================================================================

func TestDatasetGetVectorIndex_NilBoth(t *testing.T) {
ds := &Dataset{Name: "test"}

idx := ds.GetVectorIndex()
if idx != nil {
t.Error("expected nil when both indexes are nil")
}
}

func TestDatasetGetVectorIndex_ShardedPriority(t *testing.T) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}
ds.shardedIndex = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

// Sharded should have priority
idx := ds.GetVectorIndex()
if _, ok := idx.(*ShardedHNSW); !ok {
t.Error("shardedIndex should have priority")
}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkExtractVectorFromCol(b *testing.B) {
mem := memory.NewGoAllocator()

bldr := array.NewFixedSizeListBuilder(mem, 128, arrow.PrimitiveTypes.Float32)
defer bldr.Release()

valBldr := bldr.ValueBuilder().(*array.Float32Builder)
for i := 0; i < 100; i++ {
bldr.Append(true)
for j := 0; j < 128; j++ {
valBldr.Append(float32(j))
}
}

arr := bldr.NewArray()
defer arr.Release()

b.ResetTimer()
for i := 0; i < b.N; i++ {
extractVectorFromCol(arr, i%100)
}
}

func BenchmarkDatasetSearchDataset(b *testing.B) {
ds := &Dataset{
Name:  "test",
Index: NewHNSWIndex(nil),
}
ds.Index.dataset = ds
query := make([]float32, 128)

b.ResetTimer()
for i := 0; i < b.N; i++ {
ds.SearchDataset(query, 10)
}
}
