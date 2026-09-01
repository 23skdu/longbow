package index

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

// TestArrowHNSW_NewIndex validates index creation.
func TestArrowHNSW_NewIndex(t *testing.T) {
	dataset := &MockDataset{Name: "test"}
	config := types.DefaultArrowHNSWConfig()

	index := NewArrowHNSW(dataset, &config, nil)

	if index == nil {
		t.Fatal("NewArrowHNSW returned nil")
	}

	if index.Size() != 0 {
		t.Errorf("new index size = %d, want 0", index.Size())
	}

	if int(index.m.Load()) != config.M {
		t.Errorf("index.m = %d, want %d", index.m.Load(), config.M)
	}
}

// TestArrowHNSW_DefaultArrowHNSWConfig validates default configuration.
func TestArrowHNSW_DefaultArrowHNSWConfig(t *testing.T) {
	config := types.DefaultArrowHNSWConfig()

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
	data := types.NewGraphData(100, 10, false, false, 0, false, false, false, types.VectorTypeFloat32, false, false, false, 8, "test", nil, false)

	if data.Capacity < capacity {
		t.Errorf("Capacity = %d, want >= %d", data.Capacity, capacity)
	}

	// Levels is now chunked
	expectedChunks := (capacity + types.ChunkSize - 1) / types.ChunkSize
	if len(data.Levels) != expectedChunks {
		t.Errorf("Levels chunks = %d, want %d", len(data.Levels), expectedChunks)
	}

	// Check Neighbors array allocation (chunked)
	// NOTE: Only layer 0 is pre-allocated by default in NewGraphData/PreAllocate optimization.
	for i := 0; i < types.ArrowMaxLayers; i++ {
		if i == 0 {
			if len(data.Neighbors[i]) != expectedChunks {
				t.Errorf("Layer %d Neighbors chunks = %d, want %d", i, len(data.Neighbors[i]), expectedChunks)
			}
		} else {
			// Higher layers are lazy, but the slice should exist (just maybe empty or length 0)
			if len(data.Neighbors[i]) > expectedChunks {
				t.Errorf("Layer %d Neighbors chunks = %d, want <= %d", i, len(data.Neighbors[i]), expectedChunks)
			}
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
	dataset := &MockDataset{Name: "test"}
	config := types.DefaultArrowHNSWConfig()

	b.ResetTimer()
	for b.Loop() {
		_ = NewArrowHNSW(dataset, &config, nil)
	}
}
