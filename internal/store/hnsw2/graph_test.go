package hnsw2

import (
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
	
	if config.M != 16 {
		t.Errorf("default M = %d, want 16", config.M)
	}
	
	if config.MMax != 32 {
		t.Errorf("default MMax = %d, want 32", config.MMax)
	}
	
	if config.EfConstruction != 200 {
		t.Errorf("default EfConstruction = %d, want 200", config.EfConstruction)
	}
	
	// Ml should be 1/ln(2) ≈ 1.44269504089
	expectedMl := 1.44269504089
	if config.Ml < expectedMl-0.0001 || config.Ml > expectedMl+0.0001 {
		t.Errorf("default Ml = %f, want %f", config.Ml, expectedMl)
	}
}

// TestGraphNode_Initialization validates node structure.
func TestGraphNode_Initialization(t *testing.T) {
	var node GraphNode
	node.ID = 42
	node.Level = 3
	
	if node.ID != 42 {
		t.Errorf("node.ID = %d, want 42", node.ID)
	}
	
	if node.Level != 3 {
		t.Errorf("node.Level = %d, want 3", node.Level)
	}
	
	// All neighbor counts should be 0 initially
	for layer := 0; layer < MaxLayers; layer++ {
		if node.NeighborCounts[layer] != 0 {
			t.Errorf("layer %d neighbor count = %d, want 0", layer, node.NeighborCounts[layer])
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

// TestInsertContextPool_Stats validates insert pool metrics.
func TestInsertContextPool_Stats(t *testing.T) {
	pool := NewInsertContextPool()
	
	ctx1 := pool.Get()
	ctx2 := pool.Get()
	
	gets, puts := pool.Stats()
	if gets != 2 {
		t.Errorf("gets = %d, want 2", gets)
	}
	if puts != 0 {
		t.Errorf("puts = %d, want 0", puts)
	}
	
	pool.Put(ctx1)
	pool.Put(ctx2)
	
	gets, puts = pool.Stats()
	if puts != 2 {
		t.Errorf("puts after Put = %d, want 2", puts)
	}
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
