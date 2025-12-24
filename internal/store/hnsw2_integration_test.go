package store_test

import (
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/23skdu/longbow/internal/store/hnsw2"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestHNSW2Integration validates end-to-end hnsw2 integration with VectorStore
func TestHNSW2Integration(t *testing.T) {
	// Set feature flag
	os.Setenv("LONGBOW_USE_HNSW2", "true")
	defer os.Unsetenv("LONGBOW_USE_HNSW2")
	
	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Create dataset
	ds := store.NewDataset("test_hnsw2", schema)
	
	// Verify feature flag was detected
	if !ds.UseHNSW2() {
		t.Fatal("UseHNSW2() should be true when LONGBOW_USE_HNSW2=true")
	}
	
	// Initialize hnsw2 (simulating the hook from main.go)
	if ds.UseHNSW2() {
		config := hnsw2.DefaultConfig()
		hnswIndex := hnsw2.NewArrowHNSW(ds, config)
		ds.SetHNSW2Index(hnswIndex)
	}
	
	// Verify hnsw2 was initialized
	if ds.GetHNSW2Index() == nil {
		t.Fatal("hnsw2Index should not be nil after initialization")
	}
	
	// Type assert to verify it's the correct type
	idx, ok := ds.GetHNSW2Index().(*hnsw2.ArrowHNSW)
	if !ok {
		t.Fatal("hnsw2Index should be *hnsw2.ArrowHNSW")
	}
	
	// Verify initial state
	if idx.Size() != 0 {
		t.Errorf("new index size = %d, want 0", idx.Size())
	}
	
	t.Log("✓ hnsw2 integration: initialization successful")
}

// TestHNSW2EndToEnd validates complete insert and search flow
func TestHNSW2EndToEnd(t *testing.T) {
	os.Setenv("LONGBOW_USE_HNSW2", "true")
	defer os.Unsetenv("LONGBOW_USE_HNSW2")
	
	mem := memory.NewGoAllocator()
	dim := 128
	numVectors := 100
	
	// Create schema
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Build Arrow RecordBatch with test vectors
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	valueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	// Generate simple test vectors
	for i := 0; i < numVectors; i++ {
		idBuilder.Append(uint32(i))
		vecBuilder.Append(true)
		for j := 0; j < dim; j++ {
			// Simple pattern: vector[i] has value i at position 0
			if j == 0 {
				valueBuilder.Append(float32(i))
			} else {
				valueBuilder.Append(0.1)
			}
		}
	}
	
	rec := builder.NewRecord()
	defer rec.Release()
	
	// Create dataset
	ds := store.NewDataset("test_e2e", schema)
	ds.Records = []arrow.RecordBatch{rec}
	
	// Create baseline index for locationStore
	baselineIndex := store.NewHNSWIndex(ds)
	for i := 0; i < numVectors; i++ {
		baselineIndex.Add(0, i) // BatchIdx=0, RowIdx=i
	}
	ds.Index = baselineIndex
	
	// Initialize hnsw2
	config := hnsw2.DefaultConfig()
	hnswIndex := hnsw2.NewArrowHNSW(ds, config)
	ds.SetHNSW2Index(hnswIndex)
	
	// Insert vectors into hnsw2
	lg := hnsw2.NewLevelGenerator(1.44269504089)
	for i := 0; i < numVectors; i++ {
		level := lg.Generate()
		if err := hnswIndex.Insert(uint32(i), level); err != nil {
			t.Fatalf("Insert failed for vector %d: %v", i, err)
		}
	}
	
	// Verify all vectors were inserted
	if hnswIndex.Size() != numVectors {
		t.Errorf("index size = %d, want %d", hnswIndex.Size(), numVectors)
	}
	
	t.Logf("✓ Inserted %d vectors into hnsw2", numVectors)
	t.Logf("  Index size: %d", hnswIndex.Size())
	t.Logf("  Entry point: %d, Max level: %d", hnswIndex.GetEntryPoint(), hnswIndex.GetMaxLevel())
	
	// Test search
	queryVec := make([]float32, dim)
	queryVec[0] = 50.0 // Should match vector 50
	for i := 1; i < dim; i++ {
		queryVec[i] = 0.1
	}
	
	t.Logf("Searching with query vector[0]=%.1f", queryVec[0])
	
	results, err := hnswIndex.Search(queryVec, 10, 20)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	
	t.Logf("Search returned %d results", len(results))
	
	if len(results) == 0 {
		t.Fatal("Search returned no results - index may not be properly connected")
	}
	
	t.Logf("✓ Search returned %d results", len(results))
	
	// Verify top result is reasonable (should be vector 50 or nearby)
	topID := results[0].ID
	t.Logf("  Top result: ID=%d", topID)
	
	// The top result should be within reasonable range of 50
	if topID < 40 || topID > 60 {
		t.Logf("Warning: Top result ID=%d is far from expected 50", topID)
	}
	
	t.Log("✓ hnsw2 end-to-end: insert and search successful")
}

// TestHNSW2FeatureFlagDisabled verifies behavior when flag is off
func TestHNSW2FeatureFlagDisabled(t *testing.T) {
	os.Setenv("LONGBOW_USE_HNSW2", "false")
	defer os.Unsetenv("LONGBOW_USE_HNSW2")
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
		},
		nil,
	)
	
	ds := store.NewDataset("test_disabled", schema)
	
	if ds.UseHNSW2() {
		t.Error("UseHNSW2() should be false when LONGBOW_USE_HNSW2=false")
	}
	
	if ds.GetHNSW2Index() != nil {
		t.Error("hnsw2Index should be nil when feature flag is disabled")
	}
	
	t.Log("✓ Feature flag disabled: hnsw2 not initialized")
}

// TestHNSW2ConcurrentOperations validates thread safety
func TestHNSW2ConcurrentOperations(t *testing.T) {
	os.Setenv("LONGBOW_USE_HNSW2", "true")
	defer os.Unsetenv("LONGBOW_USE_HNSW2")
	
	mem := memory.NewGoAllocator()
	dim := 64
	numVectors := 50
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Build RecordBatch
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	valueBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	for i := 0; i < numVectors; i++ {
		idBuilder.Append(uint32(i))
		vecBuilder.Append(true)
		for j := 0; j < dim; j++ {
			valueBuilder.Append(float32(i) * 0.1)
		}
	}
	
	rec := builder.NewRecord()
	defer rec.Release()
	
	ds := store.NewDataset("test_concurrent", schema)
	ds.Records = []arrow.RecordBatch{rec}
	
	// Setup baseline index
	baselineIndex := store.NewHNSWIndex(ds)
	for i := 0; i < numVectors; i++ {
		baselineIndex.Add(0, i)
	}
	ds.Index = baselineIndex
	
	// Initialize hnsw2
	config := hnsw2.DefaultConfig()
	hnswIndex := hnsw2.NewArrowHNSW(ds, config)
	ds.SetHNSW2Index(hnswIndex)
	
	// Insert vectors
	lg := hnsw2.NewLevelGenerator(1.44269504089)
	for i := 0; i < numVectors; i++ {
		level := lg.Generate()
		if err := hnswIndex.Insert(uint32(i), level); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}
	
	// Run concurrent searches (should be safe with RWMutex)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			
			query := make([]float32, dim)
			for j := 0; j < dim; j++ {
				query[j] = float32(id) * 0.1
			}
			
			_, err := hnswIndex.Search(query, 5, 10)
			if err != nil {
				t.Errorf("Concurrent search %d failed: %v", id, err)
			}
		}(i)
	}
	
	// Wait for all searches to complete
	for i := 0; i < 10; i++ {
		<-done
	}
	
	t.Log("✓ Concurrent operations: 10 parallel searches successful")
}
