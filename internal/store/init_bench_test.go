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

	h := NewArrowHNSW(nil, cfg, nil)
	// Pre-set dims to avoid init mutex on every insert in this specific bench?
	// Actually we want to measure standard flow.
	h.dims.Store(128)
	// Force allocate to simulate "steady state"
	h.Grow(1000, 0)
	// Manually ensure chunks for 0
	data := h.data.Load()
	_, _ = h.ensureChunk(data, 0, 0, 128)

	vectorSize := 128
	vec := make([]float32, vectorSize) // Zero vector

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
	h := NewArrowHNSW(nil, cfg, nil)

	// Initially dims = 0
	require.Equal(t, int32(0), h.dims.Load())
	data := h.data.Load()
	require.Nil(t, data.Vectors, "Vectors should be nil initially")

	// Insert first vector -> should trigger init
	vec := make([]float32, 128)
	err := h.InsertWithVector(0, vec, 0)
	require.NoError(t, err)

	// Verify state
	require.Equal(t, int32(128), h.dims.Load())
	data = h.data.Load()
	require.NotNil(t, data.Vectors, "Vectors should be allocated")
	require.NotNil(t, data.Vectors[0], "Chunk 0 should be allocated")

	// Verify subsequent insert
	err = h.InsertWithVector(1, vec, 0)
	require.NoError(t, err)
}
