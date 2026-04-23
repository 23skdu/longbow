package core

import (
	"math/rand"
	"testing"
	
	"github.com/23skdu/longbow/internal/store/types"
)

// BenchmarkHNSW_InsertAllocations measures memory allocations during insertions.
func BenchmarkHNSW_InsertAllocations(b *testing.B) {
	// Setup
	dims := 384
	config := types.DefaultArrowHNSWConfig()
	config.M = 32
	config.EfConstruction = 100
	config.InitialCapacity = 1000 // Small start to force growth

	ds := &MockDataset{Name: "bench_alloc"}
	h := NewArrowHNSW(ds, &config, nil)

	// Pre-generate vectors
	vecs := make([][]float32, b.N)
	for i := 0; i < b.N; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = rand.Float32()
		}
		vecs[i] = vec
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := h.InsertWithVector(uint32(i), vecs[i], 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TestHNSW_VisitedGrowth validates types.ArrowBitset.Grow logic
