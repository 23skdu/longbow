package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkHNSW_InitializationOverhead measures the cost of multiple inserts
// focusing on the hot path checks for initialization.
func BenchmarkHNSW_InitializationOverhead(b *testing.B) {
	cfg := DefaultArrowHNSWConfig()
	cfg.InitialCapacity = 1000
	cfg.M = 16
	cfg.EfConstruction = 100

	// We re-create the index periodically to force "warmup" checks if any
	// but here we want to measure steady state overhead of the "if dims > 0 && vectors == nil" check.
	// So we use a single long-lived index.

	cfg.Dims = 128
	h := NewArrowHNSW(nil, &cfg, nil)
	// Force allocate to simulate "steady state"
	if err := h.Grow(1000, 0); err != nil {
		b.Fatalf("Grow failed: %v", err)
	}

	// Insert once to fully initialize internal structures (chunks, etc.)
	vectorSize := 128
	vec := make([]float32, vectorSize)
	_ = h.InsertWithVector(0, vec, 0)



	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Just insert same ID repeatedly to hit the "already initialized" path
		// or distinct IDs. Distinct is better for realism.
		// Wrapping to avoid massive growth (we care about check overhead, not growth overhead)
		id := uint32(i % 10000)
		_ = h.InsertWithVector(id, vec, 0)
	}
}

// TestHNSW_DimensionTransition verifies correctness when dims change 0 -> N
func TestHNSW_DimensionTransition(t *testing.T) {
	cfg := DefaultArrowHNSWConfig()
	cfg.InitialCapacity = 10
	h := NewArrowHNSW(nil, &cfg, nil)

	// Initially dims = 0
	require.Equal(t, int(0), h.GetConfig().Dims)
	// nodeCount should be 0 since no vectors inserted yet
	require.Equal(t, int(0), h.Len(), "Len should be 0 initially")

	// Insert first vector -> should trigger init
	vec := make([]float32, 128)
	err := h.InsertWithVector(0, vec, 0)
	require.NoError(t, err)

	// Verify state
	require.Equal(t, int(128), h.GetConfig().Dims)
	require.Equal(t, int(1), h.Len())

	// Verify subsequent insert
	err = h.InsertWithVector(1, vec, 0)
	require.NoError(t, err)
}
