package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Subtask 2: AdaptiveIndexConfig Tests (TDD Red Phase)
// =============================================================================

func TestDefaultAdaptiveIndexConfig(t *testing.T) {
	cfg := DefaultAdaptiveIndexConfig()

	if cfg.Threshold != 1000 {
		t.Errorf("expected default Threshold=1000, got %d", cfg.Threshold)
	}
	if !cfg.Enabled {
		t.Error("expected default Enabled=true")
	}
}

func TestAdaptiveIndexConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     AdaptiveIndexConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     AdaptiveIndexConfig{Threshold: 1000, Enabled: true},
			wantErr: false,
		},
		{
			name:    "disabled config skips validation",
			cfg:     AdaptiveIndexConfig{Threshold: 0, Enabled: false},
			wantErr: false,
		},
		{
			name:    "zero threshold when enabled",
			cfg:     AdaptiveIndexConfig{Threshold: 0, Enabled: true},
			wantErr: true,
		},
		{
			name:    "negative threshold",
			cfg:     AdaptiveIndexConfig{Threshold: -1, Enabled: true},
			wantErr: true,
		},
		{
			name:    "threshold of 1 is valid",
			cfg:     AdaptiveIndexConfig{Threshold: 1, Enabled: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Subtask 3: BruteForceIndex Tests (TDD Red Phase)
// =============================================================================

func TestNewBruteForceIndex(t *testing.T) {
	ds := &Dataset{Name: "test"}
	idx := NewBruteForceIndex(ds)

	if idx == nil {
		t.Fatal("expected non-nil index")
	}
	if idx.Len() != 0 {
		t.Errorf("expected empty index, got Len=%d", idx.Len())
	}
}

func TestBruteForceIndexAddByLocation(t *testing.T) {
	ds := createTestDatasetWithVectors(t, "bf_test", 10)
	idx := NewBruteForceIndex(ds)

	// Add vectors from batch 0
	for i := 0; i < 10; i++ {
		err := idx.AddByLocation(0, i)
		if err != nil {
			t.Fatalf("AddByLocation failed: %v", err)
		}
	}

	if idx.Len() != 10 {
		t.Errorf("expected Len=10, got %d", idx.Len())
	}
}

func TestBruteForceIndexSearchVectors(t *testing.T) {
	ds := createTestDatasetWithVectors(t, "bf_search", 100)
	idx := NewBruteForceIndex(ds)

	// Add all vectors
	for i := 0; i < 100; i++ {
		if err := idx.AddByLocation(0, i); err != nil {
			t.Fatalf("AddByLocation failed: %v", err)
		}
	}

	// Search for k=10 nearest neighbors
	query := []float32{0.1, 0.2, 0.3, 0.4}
	results := idx.SearchVectors(query, 10)

	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	// Results should be sorted by score (ascending distance)
	for i := 1; i < len(results); i++ {
		if results[i].Score < results[i-1].Score {
			t.Errorf("results not sorted: score[%d]=%f < score[%d]=%f",
				i, results[i].Score, i-1, results[i-1].Score)
		}
	}
}

func TestBruteForceIndexSearchExact(t *testing.T) {
	// Create dataset with known vectors
	ds := createTestDatasetWithVectors(t, "bf_exact", 5)
	idx := NewBruteForceIndex(ds)

	for i := 0; i < 5; i++ {
		if err := idx.AddByLocation(0, i); err != nil {
			t.Fatalf("AddByLocation failed: %v", err)
		}
	}

	// Search with k larger than index size
	query := []float32{1.0, 0.0, 0.0, 0.0}
	results := idx.SearchVectors(query, 100)

	// Should return all 5 vectors, not 100
	if len(results) != 5 {
		t.Errorf("expected 5 results when k > index size, got %d", len(results))
	}
}

func TestBruteForceIndexEmptySearch(t *testing.T) {
	ds := &Dataset{Name: "empty"}
	idx := NewBruteForceIndex(ds)

	query := []float32{1.0, 2.0, 3.0, 4.0}
	results := idx.SearchVectors(query, 10)

	if len(results) != 0 {
		t.Errorf("expected 0 results for empty index, got %d", len(results))
	}
}

// =============================================================================
// Subtask 4: AdaptiveIndex Tests (TDD Red Phase)
// =============================================================================

func TestNewAdaptiveIndex(t *testing.T) {
	ds := createTestDatasetWithVectors(t, "adaptive_test", 10)
	cfg := DefaultAdaptiveIndexConfig()

	idx := NewAdaptiveIndex(ds, cfg)
	if idx == nil {
		t.Fatal("expected non-nil AdaptiveIndex")
	}

	// Should start with BruteForce for empty/small datasets
	if idx.GetIndexType() != "brute_force" {
		t.Errorf("expected brute_force initially, got %s", idx.GetIndexType())
	}
}

func TestAdaptiveIndexSwitchesToHNSW(t *testing.T) {
	// Use low threshold for testing
	cfg := AdaptiveIndexConfig{Threshold: 50, Enabled: true}
	ds := createTestDatasetWithVectors(t, "adaptive_switch", 100)

	idx := NewAdaptiveIndex(ds, cfg)

	// Add vectors up to threshold
	for i := 0; i < 100; i++ {
		if err := idx.AddByLocation(0, i); err != nil {
			t.Fatalf("AddByLocation failed at %d: %v", i, err)
		}
	}

	// Should have switched to HNSW after exceeding threshold
	if idx.GetIndexType() != "hnsw" {
		t.Errorf("expected hnsw after threshold, got %s", idx.GetIndexType())
	}
}

func TestAdaptiveIndexSearchAfterMigration(t *testing.T) {
	cfg := AdaptiveIndexConfig{Threshold: 20, Enabled: true}
	ds := createTestDatasetWithVectors(t, "adaptive_search", 50)

	idx := NewAdaptiveIndex(ds, cfg)

	// Add vectors to trigger migration
	for i := 0; i < 50; i++ {
		if err := idx.AddByLocation(0, i); err != nil {
			t.Fatalf("AddByLocation failed: %v", err)
		}
	}

	// Search should work after migration
	query := []float32{0.1, 0.2, 0.3, 0.4}
	results := idx.SearchVectors(query, 10)

	if len(results) == 0 {
		t.Error("expected search results after migration")
	}
}

func TestAdaptiveIndexGetMigrationCount(t *testing.T) {
	cfg := AdaptiveIndexConfig{Threshold: 10, Enabled: true}
	ds := createTestDatasetWithVectors(t, "adaptive_count", 20)

	idx := NewAdaptiveIndex(ds, cfg)

	initialCount := idx.GetMigrationCount()

	// Add vectors to trigger migration
	for i := 0; i < 20; i++ {
		_ = idx.AddByLocation(0, i)
	}

	// Migration count should have increased
	if idx.GetMigrationCount() <= initialCount {
		t.Error("expected migration count to increase")
	}
}

func TestAdaptiveIndexConcurrentAccess(t *testing.T) {
	cfg := AdaptiveIndexConfig{Threshold: 100, Enabled: true}
	ds := createTestDatasetWithVectors(t, "adaptive_concurrent", 500)

	idx := NewAdaptiveIndex(ds, cfg)

	// Concurrent writes
	done := make(chan bool)
	for g := 0; g < 4; g++ {
		go func(start int) {
			for i := start; i < start+100; i++ {
				_ = idx.AddByLocation(0, i%500)
			}
			done <- true
		}(g * 100)
	}

	for i := 0; i < 4; i++ {
		<-done
	}

	// Should have migrated and still be functional
	query := []float32{0.5, 0.5, 0.5, 0.5}
	results := idx.SearchVectors(query, 5)
	if len(results) == 0 {
		t.Error("expected results after concurrent access")
	}
}

// =============================================================================
// Helper: Create test dataset with vectors
// =============================================================================

func createTestDatasetWithVectors(t *testing.T, name string, numVectors int) *Dataset {
	t.Helper()
	const dims = 4

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		vecBuilder.Append(true)
		for d := 0; d < dims; d++ {
			floatBuilder.Append(float32(i*dims+d) * 0.01)
		}
	}

	record := builder.NewRecordBatch()

	return &Dataset{
		Name:    name,
		Records: []arrow.RecordBatch{record},
	}
}
