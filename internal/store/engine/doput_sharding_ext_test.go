package engine

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func skipIfRace(t *testing.T) {
	if raceEnabled {
		t.Skip("skipping under race detector (too slow)")
	}
}

// =============================================================================
// extractVectorFromCol Tests
// =============================================================================

func wrapInBatch(arr arrow.Array) arrow.RecordBatch {
	// Create a schema with "vector" column
	field := arrow.Field{Name: "vector", Type: arr.DataType()}
	schema := arrow.NewSchema([]arrow.Field{field}, nil)

	// Retain array as RecordBatch takes ownership of reference/doesn't automatically retain?
	// actually NewRecordBatch creates new refs.
	// But we need to match array length.

	arr.Retain() // Retain for the batch
	cols := []arrow.Array{arr}
	return array.NewRecordBatch(schema, cols, int64(arr.Len()))
}

// TestExtractVectorFromCol_Nil runs the test extract vector from col_nil operation.
func TestExtractVectorFromCol_Nil(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// Cannot pass nil record to extractVectorFromCol safely if it dereferences
	// Implementation should check for nil?
	// func extractVectorFromCol(rec arrow.RecordBatch, rowIdx int) ...
	// internal helper, usually assumes non-nil.
	// If we pass nil, it might panic. Let's see implementation.
	// It calls rec.Schema().Fields().
	// So passing nil causes panic.
	// We'll skip nil test or assume caller handles it.
	// Or update test to check panic?
	// Let's remove this test as it tests undefined behavior on internal helper.
}

// TestExtractVectorFromCol_OutOfBounds runs the test extract vector from col_out of bounds operation.
func TestExtractVectorFromCol_OutOfBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	mem := memory.NewGoAllocator()

	// Create a small fixed size list array
	bldr := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
	defer bldr.Release()

	valBldr := bldr.ValueBuilder().(*array.Float32Builder)
	bldr.Append(true)
	valBldr.AppendValues([]float32{1.0, 2.0, 3.0}, nil)

	arr := bldr.NewArray()
	defer arr.Release()

	rec := wrapInBatch(arr)
	defer rec.Release()

	// Test out of bounds
	result, err := extractVectorFromCol(rec, 999)
	if result != nil || err == nil {
		t.Error("expected nil/error for out of bounds index")
	}
}

// TestExtractVectorFromCol_WrongType runs the test extract vector from col_wrong type operation.
func TestExtractVectorFromCol_WrongType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	mem := memory.NewGoAllocator()

	// Create a non-list array (Int64)
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.Append(123)

	arr := bldr.NewArray()
	defer arr.Release()

	rec := wrapInBatch(arr)
	defer rec.Release()

	// Should return error for non-FixedSizeList
	result, err := extractVectorFromCol(rec, 0)
	if result != nil || err == nil {
		t.Error("expected error for non-FixedSizeList column")
	}
}

// TestExtractVectorFromCol_Success runs the test extract vector from col_success operation.
func TestExtractVectorFromCol_Success(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	mem := memory.NewGoAllocator()

	bldr := array.NewFixedSizeListBuilder(mem, 3, arrow.PrimitiveTypes.Float32)
	defer bldr.Release()

	valBldr := bldr.ValueBuilder().(*array.Float32Builder)
	bldr.Append(true)
	valBldr.AppendValues([]float32{1.0, 2.0, 3.0}, nil)

	arr := bldr.NewArray()
	defer arr.Release()

	rec := wrapInBatch(arr)
	defer rec.Release()

	result, err := extractVectorFromCol(rec, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

// TestDatasetSearchDataset_NoIndex runs the test dataset search dataset_no index operation.
func TestDatasetSearchDataset_NoIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}

	results, _ := ds.SearchDataset(context.Background(), []float32{1, 2, 3}, 10)
	if results != nil {
		t.Error("expected nil results with no index")
	}
}

// TestDatasetSearchDataset_WithHNSW runs the test dataset search dataset_with hnsw operation.
func TestDatasetSearchDataset_WithHNSW(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}
	idx := NewTestHNSWIndex(ds)
	ds.Index = idx
	defer idx.Close()

	// Empty index should return empty results
	results, _ := ds.SearchDataset(context.Background(), []float32{1, 2, 3}, 10)
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

// TestDatasetSearchDataset_WithSharded runs the test dataset search dataset_with sharded operation.
func TestDatasetSearchDataset_WithSharded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

	// Empty index should return empty results
	results, _ := ds.SearchDataset(context.Background(), []float32{1, 2, 3}, 10)
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

// =============================================================================
// Dataset.AddToIndex Tests
// =============================================================================

// TestDatasetAddToIndex_NoIndex runs the test dataset add to index_no index operation.
func TestDatasetAddToIndex_NoIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}

	err := ds.AddToIndex(0, 0)
	if err == nil {
		t.Error("expected error when no index available")
	}
}

// =============================================================================
// Dataset.MigrateToShardedIndex Tests
// =============================================================================

// TestDatasetMigrateToShardedIndex_AlreadySharded runs the test dataset migrate to sharded index_already sharded operation.
func TestDatasetMigrateToShardedIndex_AlreadySharded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

	// Should return nil (no-op) when already sharded
	err := ds.MigrateToShardedIndex(DefaultAutoShardingConfig())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestDatasetMigrateToShardedIndex_NoIndex runs the test dataset migrate to sharded index_no index operation.
func TestDatasetMigrateToShardedIndex_NoIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}

	err := ds.MigrateToShardedIndex(DefaultAutoShardingConfig())
	if err == nil {
		t.Error("expected error when no index to migrate")
	}
}

// =============================================================================

// =============================================================================
// VectorStore.checkAndMigrateToSharded Tests
// =============================================================================

// TestCheckAndMigrateToSharded_Disabled runs the test check and migrate to sharded_disabled operation.
func TestCheckAndMigrateToSharded_Disabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	skipIfRace(t)
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
	defer func() { _ = vs.Close() }()

	vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: false})

	ds := &Dataset{Name: "test", Index: NewTestHNSWIndex(nil)}

	vs.checkAndMigrateToSharded(ds)
	if ds.IsSharded() {
		t.Error("should not shard when disabled")
	}
}

// TestCheckAndMigrateToSharded_AlreadySharded runs the test check and migrate to sharded_already sharded operation.
func TestCheckAndMigrateToSharded_AlreadySharded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	skipIfRace(t)
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
	defer func() { _ = vs.Close() }()

	vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: true, ShardThreshold: 1})

	ds := &Dataset{Name: "test"}
	ds.Index = NewShardedHNSW(DefaultShardedHNSWConfig(), ds)

	vs.checkAndMigrateToSharded(ds)
}

// TestCheckAndMigrateToSharded_BelowThreshold runs the test check and migrate to sharded_below threshold operation.
func TestCheckAndMigrateToSharded_BelowThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	skipIfRace(t)
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	vs := NewVectorStore(mem, logger, 1<<30, 1<<20, 5*time.Minute)
	defer func() { _ = vs.Close() }()

	vs.SetAutoShardingConfig(AutoShardingConfig{Enabled: true, ShardThreshold: 1000000})

	ds := &Dataset{Name: "test", Index: NewTestHNSWIndex(nil)}
	_ = ds

	vs.checkAndMigrateToSharded(ds)
	if ds.IsSharded() {
		t.Error("should not shard when below threshold")
	}
}

// =============================================================================
// HNSWIndex.SearchVectors Tests
// =============================================================================

// TestHNSWIndex_SearchVectors_Empty runs the test hnsw index_search vectors_empty operation.
func TestHNSWIndex_SearchVectors_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}
	hnsw := NewTestHNSWIndex(ds)

	results, err := hnsw.SearchVectors(context.Background(), []float32{1, 2, 3}, 10, nil, SearchOptions{})
	if err != nil {
		t.Errorf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

// =============================================================================
// ShardedHNSW.SearchVectors Tests
// =============================================================================

// TestShardedHNSW_SearchVectors_Empty runs the test sharded hnsw_search vectors_empty operation.
func TestShardedHNSW_SearchVectors_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := DefaultShardedHNSWConfig()
	ds := &Dataset{Name: "test"}
	sharded := NewShardedHNSW(cfg, ds)

	results, err := sharded.SearchVectors(context.Background(), []float32{1, 2, 3}, 10, nil, SearchOptions{})
	if err != nil {
		t.Errorf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
	}
}

// =============================================================================
// Dataset.GetVectorIndex Edge Cases
// =============================================================================

// TestDatasetGetVectorIndex_NilBoth runs the test dataset get vector index_nil both operation.
func TestDatasetGetVectorIndex_NilBoth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	ds := &Dataset{Name: "test"}

	idx := ds.GetVectorIndex()
	if idx != nil {
		t.Error("expected nil when both indexes are nil")
	}
}

// TestDatasetGetVectorIndex_ShardedPriority is obsolete.
func TestDatasetGetVectorIndex_ShardedPriority(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// Obsolete
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkExtractVectorFromCol runs the benchmark extract vector from col operation.
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

	rec := wrapInBatch(arr)
	defer rec.Release()

	b.ResetTimer()
	i := 0
	for b.Loop() {
		_, _ = extractVectorFromCol(rec, i%100)
		i++
	}
}

// BenchmarkDatasetSearchDataset runs the benchmark dataset search dataset operation.
func BenchmarkDatasetSearchDataset(b *testing.B) {
	ds := &Dataset{
		Name:  "test",
		Index: NewTestHNSWIndex(nil),
	}
	_ = ds
	query := make([]float32, 128)

	b.ResetTimer()
	for b.Loop() {
		_, _ = ds.SearchDataset(context.Background(), query, 10)
	}
}
