package core_test

import (
	"github.com/23skdu/longbow/internal/store/internal/core"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowSearchContext_LifeCycle(t *testing.T) {
	pool := core.NewArrowSearchContextPool()
	require.NotNil(t, pool)

	t.Run("GetAndPut", func(t *testing.T) {
		ctx := pool.Get()
		require.NotNil(t, ctx)
		
		ctx.MarkDirty()
		assert.True(t, ctx.IsDirty())
		
		ctx.RecordEarlyExit("timeout")
		
		pool.Put(ctx)
		
		gets, puts := pool.Stats()
		assert.Equal(t, int64(1), gets)
		assert.Equal(t, int64(1), puts)
	})

	t.Run("ResetVerification", func(t *testing.T) {
		ctx := pool.Get()
		ctx.MarkDirty()
		
		// Fill some buffers
		// Note: We can't access private fields easily since we are in core_test,
		// but we can call Reset and verify it works via public methods if available.
		ctx.Reset()
		assert.False(t, ctx.IsDirty())
		
		pool.Put(ctx)
	})

	t.Run("MetricsFlush", func(t *testing.T) {
		ctx := pool.Get()
		// Mock some computation
		// Again, we'd need exported setters/incrementors or use internal package
		pool.PutWithMetrics(ctx, "float32", "128")
	})
}

func TestCandidateHeap(t *testing.T) {
	// CandidateHeap is unexported, so this test should be in search_context_internal_test.go
	// if we wanted to test the methods directly.
	// But we can test the behavior via NewArrowSearchContext if it expose anything.
	// Since it doesn't, I'll create a small internal test file.
}
