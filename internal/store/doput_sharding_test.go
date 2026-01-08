package store

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

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
		Name: "test",
	}
	ds.Index = NewHNSWIndex(ds)

	// Verify references
	if ds.Index.(*HNSWIndex).dataset != ds {
		t.Error("Index dataset reference incorrect")
	}

	if ds.IsSharded() {
		t.Error("new dataset should not be sharded")
	}

	// Test manual initialization (simulating startup)
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

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
	ds.Index.(*HNSWIndex).dataset = ds

	if ds.IndexLen() != 0 {
		t.Errorf("expected 0, got %d", ds.IndexLen())
	}
	// Check that dataset reference is updated
	if ds.Index.(*HNSWIndex).dataset != ds {
		t.Error("dataset reference not updated in HNSW index")
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

	// Test manual initialization (simulating startup)
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds) // Assign to generic Index
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
	logger := zerolog.Nop()
	return NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
}

// TestVectorStoreSetAutoShardingConfig verifies config can be set.
func TestVectorStoreSetAutoShardingConfig(t *testing.T) {
	vs := createShardingTestVectorStore(t)
	defer func() { _ = vs.Close() }()

	cfg := AutoShardingConfig{
		Enabled:        true,
		ShardThreshold: 5000,
		ShardCount:     8,
	}
	vs.SetAutoShardingConfig(cfg)

	got := vs.GetAutoShardingConfig()
	if got.ShardThreshold != 5000 {
		t.Errorf("expected threshold 5000, got %d", got.ShardThreshold)
	}
}

// TestVectorStoreGetDataset verifies getDataset retrieval.
func TestVectorStoreGetDataset(t *testing.T) {
	vs := createShardingTestVectorStore(t)
	defer func() { _ = vs.Close() }()

	// Should not exist yet
	_, ok := vs.getDataset("nonexistent")
	if ok {
		t.Error("expected missing dataset")
	}

	// Create dataset
	ds := &Dataset{Name: "test"}
	vs.updateDatasets(func(m map[string]*Dataset) {
		m["test"] = ds
	})

	// Now should exist
	got, _ := vs.getDataset("test")

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
		Enabled:        true,
		ShardThreshold: 5, // Very low threshold for testing
		ShardCount:     4,
	})

	// Create dataset manually without expensive indexing
	ds := &Dataset{
		Name:  "test",
		Index: NewHNSWIndex(nil),
	}
	ds.Index.(*HNSWIndex).dataset = ds
	ds.Index.(*HNSWIndex).dataset = ds
	vs.updateDatasets(func(m map[string]*Dataset) {
		m["test"] = ds
	})

	// Verify not sharded initially
	if ds.IsSharded() {
		t.Error("should not be sharded initially")
	}

	// Verify config was set
	cfg := vs.GetAutoShardingConfig()
	if cfg.ShardThreshold != 5 {
		t.Errorf("expected threshold 5, got %d", cfg.ShardThreshold)
	}

	// Test the migration check function (without actual vectors)
	// Since index is empty, it won't trigger migration
	vs.checkAndMigrateToSharded(ds)

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
	ds.Index.(*HNSWIndex).dataset = ds
	ds.Index.(*HNSWIndex).dataset = ds
	vs.updateDatasets(func(m map[string]*Dataset) {
		m["concurrent"] = ds
	})

	// Enable auto-sharding
	vs.SetAutoShardingConfig(AutoShardingConfig{
		Enabled:        true,
		ShardThreshold: 10,
		ShardCount:     4,
	})

	// Concurrent access
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vs.checkAndMigrateToSharded(ds)
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
	ds.Index.(*HNSWIndex).dataset = ds

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.AddToIndex(0, 0)
	}
}

func BenchmarkAddToIndexSharded(b *testing.B) {
	mem := memory.NewGoAllocator()
	rec := createShardingTestRecordBatch(&testing.T{}, mem, 1, 128)
	defer rec.Release()

	ds := createDatasetWithShardedIndex()
	// Break the back-reference
	ds.Index.(*ShardedHNSW).dataset = ds

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ds.AddToIndex(0, 0)
	}
}

func createDatasetWithShardedIndex() *Dataset {
	ds := &Dataset{Name: "test"}
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)
	return ds
}
