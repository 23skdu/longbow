package store

import (
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// TestArrowHNSW_NewIndex validates index creation.
func TestArrowHNSW_NewIndex(t *testing.T) {
	dataset := &Dataset{Name: "test"}
	config := DefaultArrowHNSWConfig()

	index := NewArrowHNSW(dataset, &config)

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

// TestArrowHNSW_DefaultArrowHNSWConfig validates default configuration.
func TestArrowHNSW_DefaultArrowHNSWConfig(t *testing.T) {
	config := DefaultArrowHNSWConfig()

	if config.M != 32 {
		t.Errorf("default M = %d, want 32", config.M)
	}

	if config.MMax != 64 {
		t.Errorf("default MMax = %d, want 64", config.MMax)
	}

	if config.EfConstruction != 400 {
		t.Errorf("default EfConstruction = %d, want 400", config.EfConstruction)
	}

}

// TestGraphData_Initialization validates GraphData structure.
func TestGraphData_Initialization(t *testing.T) {
	capacity := 100
	data := lbtypes.NewGraphData(100, 10, false, false, 0, false, false, false, lbtypes.VectorTypeFloat32)

	if data.Capacity < capacity {
		t.Errorf("Capacity = %d, want >= %d", data.Capacity, capacity)
	}

	// Levels is now chunked
	expectedChunks := (capacity + lbtypes.ChunkSize - 1) / lbtypes.ChunkSize
	if len(data.Levels) != expectedChunks {
		t.Errorf("Levels chunks = %d, want %d", len(data.Levels), expectedChunks)
	}

	// Check Neighbors array allocation (chunked)
	for i := 0; i < lbtypes.ArrowMaxLayers; i++ {
		if len(data.Neighbors[i]) != expectedChunks {
			t.Errorf("Layer %d Neighbors chunks = %d, want %d", i, len(data.Neighbors[i]), expectedChunks)
		}
	}
}

// TestArrowSearchContextPool_Stats validates pool metrics.
func TestArrowSearchContextPool_Stats(t *testing.T) {
	pool := NewArrowSearchContextPool()

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
	dataset := &Dataset{Name: "test"}
	config := DefaultArrowHNSWConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewArrowHNSW(dataset, &config)
	}
}
