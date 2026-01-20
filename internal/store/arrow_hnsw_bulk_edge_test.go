package store

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_AddBatchBulk_EdgeCases tests edge cases for the bulk insertion path.
// Note: We access AddBatchBulk directly or via AddBatch with preconditions if possible,
// but AddBatchBulk is an internal method of ArrowHNSW (exported but assumes internal state).
// To test it safely, we should use AddBatch but force the bulk path or mock internal state.
// However, AddBatchBulk is exported, so we can call it if we setup the index correctly.
func TestArrowHNSW_AddBatchBulk_EdgeCases(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dims := 4
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	ds := NewDataset("test_edge", schema)
	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims

	idx := NewArrowHNSW(ds, cfg, nil)
	defer func() { _ = idx.Close() }()

	t.Run("EmptyBatch", func(t *testing.T) {
		// Call with n=0
		err := idx.AddBatchBulk(context.Background(), 0, 0, [][]float32{})
		// Should likely be a no-op or return nil
		require.NoError(t, err)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		// Create a large-ish batch to ensure it doesn't finish instantly (though mocked logic might)
		n := 1000
		vecs := make([][]float32, n)
		for i := 0; i < n; i++ {
			vecs[i] = make([]float32, dims)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// StartID shouldn't matter as we fail early
		err := idx.AddBatchBulk(ctx, 100, n, vecs)
		require.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		// Pass an unsupported type to AddBatchBulk generic arg
		vecs := []string{"not", "a", "vector"}
		err := idx.AddBatchBulk(context.Background(), 200, 3, vecs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported vector type")
	})

	t.Run("NilVectorInBatch", func(t *testing.T) {
		// Create a batch with a nil vector
		vecs := make([][]float32, 5)
		vecs[0] = make([]float32, dims)
		vecs[2] = nil // Missing

		// Note: The implementation checks `if v == nil` inside the worker loop
		// But a nil slice of []float32 is not nil interface, and has len 0.
		// So it triggers dimension mismatch (expected 4, got 0)
		err := idx.AddBatchBulk(context.Background(), 300, 5, vecs)
		require.Error(t, err)
		// We accept either "vector missing" or "dimension mismatch"
		assert.Condition(t, func() bool {
			return strings.Contains(err.Error(), "vector missing") || strings.Contains(err.Error(), "dimension mismatch")
		}, "Error should be about missing vector or dimension mismatch: %v", err)
	})

	t.Run("DimensionMismatch", func(t *testing.T) {
		// Create a batch with wrong dimensions
		vecs := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vecs[i] = make([]float32, dims+1) // Wrong dim
		}

		err := idx.AddBatchBulk(context.Background(), 400, 5, vecs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dimension mismatch")
	})
}
