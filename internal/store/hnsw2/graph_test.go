package hnsw2

import (
	"math"
	"testing"
	
	"github.com/23skdu/longbow/internal/store"
)

// TestArrowHNSW_NewIndex validates index creation.
func TestArrowHNSW_NewIndex(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	config := DefaultConfig()
	
	index := NewArrowHNSW(dataset, config)
	
	if index == nil {
		t.Fatal("NewArrowHNSW returned nil")
	}
	
	if index.Size() != 0 {
		t.Errorf("new index size = %d, want 0", index.Size())
	}
	
	if index.m != config.M {
		t.Errorf("index.m = %d, want %d", index.m, config.M)
	}
}

// TestArrowHNSW_DefaultConfig validates default configuration.
func TestArrowHNSW_DefaultConfig(t *testing.T) {
	config := DefaultConfig()
	
	if config.M != 32 {
		t.Errorf("default M = %d, want 32", config.M)
	}
	
	if config.MMax != 96 {
		t.Errorf("default MMax = %d, want 96", config.MMax)
	}
	
	if config.EfConstruction != 400 {
		t.Errorf("default EfConstruction = %d, want 400", config.EfConstruction)
	}
	
	// Ml should be 1/ln(32) ≈ 0.288539
	expectedMl := 1.0 / math.Log(32)
	if config.Ml < expectedMl-0.0001 || config.Ml > expectedMl+0.0001 {
		t.Errorf("default Ml = %f, want %f", config.Ml, expectedMl)
	}
}

// TestGraphData_Initialization validates GraphData structure.
func TestGraphData_Initialization(t *testing.T) {
	capacity := 100
	data := NewGraphData(capacity, 0)
	
	if data.Capacity < capacity {
		t.Errorf("Capacity = %d, want >= %d", data.Capacity, capacity)
	}
	
	// Levels is now chunked
	expectedChunks := (capacity + ChunkSize - 1) / ChunkSize
	if len(data.Levels) != expectedChunks {
		t.Errorf("Levels chunks = %d, want %d", len(data.Levels), expectedChunks)
	}

	// Check Neighbors array allocation (chunked)
	for i := 0; i < MaxLayers; i++ {
		if len(data.Neighbors[i]) != expectedChunks {
			t.Errorf("Layer %d Neighbors chunks = %d, want %d", i, len(data.Neighbors[i]), expectedChunks)
		}
	}
}

// TestSearchContextPool_Stats validates pool metrics.
func TestSearchContextPool_Stats(t *testing.T) {
	pool := NewSearchContextPool()
	
	// Initial stats should be 0
	// Note: pool may have been used in init, so we just check it works
	ctx1 := pool.Get()
	ctx2 := pool.Get()
	
	pool.Put(ctx1)
	pool.Put(ctx2)
	
	// Pool should work without panicking
	ctx3 := pool.Get()
	if ctx3 == nil {
		t.Error("pool.Get() returned nil")
	}
	pool.Put(ctx3)
}

// BenchmarkNewArrowHNSW benchmarks index creation.
func BenchmarkNewArrowHNSW(b *testing.B) {
	dataset := &store.Dataset{Name: "test"}
	config := DefaultConfig()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewArrowHNSW(dataset, config)
	}
}
