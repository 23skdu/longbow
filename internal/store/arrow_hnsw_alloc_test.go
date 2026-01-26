package store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkHNSW_InsertAllocations measures memory allocations during insertions.
func BenchmarkHNSW_InsertAllocations(b *testing.B) {
	// Setup
	dims := 384
	config := DefaultArrowHNSWConfig()
	config.M = 32
	config.EfConstruction = 100
	config.InitialCapacity = 1000 // Small start to force growth

	ds := &Dataset{Name: "bench_alloc"}
	h := NewArrowHNSW(ds, config)

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

// TestHNSW_VisitedGrowth validates ArrowBitset.Grow logic
func TestHNSW_VisitedGrowth(t *testing.T) {
	// This test simulates the growth pattern in HNSW
	bs := NewArrowBitset(100)

	// Track allocations indirectly by address change? Hard in Go.
	// But we can check capacity behavior.

	initialCap := cap(bs.bits)

	// Grow slightly
	bs.Grow(101)
	cap1 := cap(bs.bits)

	// Depending on start size (100 bits = 2 uint64s), cap=2.
	// 101 bits = 2 uint64s.
	// Wait, 100/64 = 1. 100%64=36. So 2 words.
	// 101/64 = 1. 101%64=37. So 2 words.
	// So it shouldn't grow.

	// Grow to 129 (3 words)
	bs.Grow(129)
	cap2 := cap(bs.bits)

	// Current impl: cap2 should be 3.
	// Optimized impl: cap2 should be 4 or more (doubling).

	fmt.Printf("Initial Cap: %d, Cap after small grow: %d, Cap after cross-word grow: %d\n", initialCap, cap1, cap2)

	require.GreaterOrEqual(t, cap2, 3)
}
