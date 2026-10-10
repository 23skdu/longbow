package index_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_AddBatchBulk_EdgeCases tests edge cases for the bulk insertion path.
// Note: We access AddBatchBulk directly or via AddBatch with preconditions if possible,
// but AddBatchBulk is an internal method of index.ArrowHNSW (exported but assumes internal state).
// To test it safely, we should use AddBatch but force the bulk path or mock internal state.
// However, AddBatchBulk is exported, so we can call it if we setup the index correctly.
func TestArrowHNSW_AddBatchBulk_EdgeCases(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dims := 4
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	ds := index.NewMockDataset("test_edge", schema)
	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dims

	idx := index.NewArrowHNSW(ds, &cfg, nil)
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

		// Use current length as startID to avoid waiting for non-existent preceding IDs
		err := idx.AddBatchBulk(ctx, uint32(idx.Len()), n, vecs)
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "Should be context error: %v", err)
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		// Pass an unsupported type to AddBatchBulk generic arg
		vecs := []string{"not", "a", "vector"}
		err := idx.AddBatchBulk(context.Background(), uint32(idx.Len()), 3, vecs)
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
		err := idx.AddBatchBulk(context.Background(), uint32(idx.Len()), 5, vecs)
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

		err := idx.AddBatchBulk(context.Background(), uint32(idx.Len()), 5, vecs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dimension mismatch")
	})
}

// TestArrowHNSW_AddBatch_CancelMidFlight verifies that AddBatch honours a
// context deadline that expires partway through a bulk-insertable batch,
// instead of silently redoing the whole batch on the sequential fallback path
// and reporting success.
//
// Regression test: addBatchBulkInternal aborted on ctx but AddBatch discarded
// that error and fell through to sequential insertion, which checked neither
// ctx nor the already-expired deadline. A 400ms deadline over a 20k float32
// batch previously ran for ~2 minutes and returned nil.
func TestArrowHNSW_AddBatch_CancelMidFlight(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	dims := 128
	n := 20000
	if testing.Short() {
		n = 2000
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := arrowarray.NewRecordBuilder(mem, schema)
	idB := builder.Field(0).(*arrowarray.Int64Builder)
	vecB := builder.Field(1).(*arrowarray.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*arrowarray.Float32Builder)
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32((i*7 + j*13) % 251)
		}
		valB.AppendValues(v, nil)
	}
	rec := builder.NewRecordBatch()
	builder.Release()
	defer rec.Release()

	ds := index.NewMockDataset("test_cancel_midflight", schema)
	cfg := types.DefaultArrowHNSWConfig()
	cfg.Dims = dims

	idx := index.NewArrowHNSW(ds, &cfg, nil)
	defer func() { _ = idx.Close() }()

	rowIdxs := make([]int, n)
	batchIdxs := make([]int, n)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
		done <- err
	}()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatalf("AddBatch ignored its 400ms deadline: returned success after %v", elapsed)
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context error, got %v", err)
		}
		// The batch takes ~11s on its own, so a prompt abort must be far
		// quicker than running it to completion.
		if elapsed > 30*time.Second {
			t.Fatalf("cancellation took %v; deadline was not honoured", elapsed)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("AddBatch ignored cancellation and hung past its deadline")
	}
}
