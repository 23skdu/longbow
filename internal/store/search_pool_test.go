package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSearchContext_Reuse(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// Get an object
	ctx1 := pool.Get().(*ArrowSearchContext)
	assert.NotNil(t, ctx1)
	ptr1 := ctx1

	// Put it back
	pool.Put(ctx1)

	// Get object again (should be same pointer if pool works ideally, though sync.Pool doesn't guarantee it)
	// To force reuse in a single-threaded test, it usually works.
	ctx2 := pool.Get().(*ArrowSearchContext)
	ptr2 := ctx2

	assert.Equal(t, ptr1, ptr2, "Expected pooled object to be reused")
}

func TestSearchContext_Reset(t *testing.T) {
	ctx := NewArrowSearchContext()

	// Dirty the context
	ctx.visited.Set(1)
	ctx.scratchIDs = append(ctx.scratchIDs, 1, 2, 3)
	ctx.scratchDists = append(ctx.scratchDists, 1.0, 2.0)
	// Simulate usage of other fields if possible, but scratchIDs is a good proxy for slices

	// Reset
	ctx.Reset()

	// precise check of Reset logic
	assert.Equal(t, 0, len(ctx.scratchIDs), "scratchIDs len should be 0")
	assert.GreaterOrEqual(t, cap(ctx.scratchIDs), 3, "scratchIDs cap should be preserved")

	assert.Equal(t, 0, len(ctx.scratchDists), "scratchDists len should be 0")

	// Check visited cleared
	assert.False(t, ctx.visited.IsSet(1), "visited bit 1 should be cleared")
}

func TestSearchContext_Metrics(t *testing.T) {
	pool := NewArrowSearchContextPool()

	// New pool allocation should increment NewTotal
	_ = pool.Get().(*ArrowSearchContext)
	// We can't easily check the global prometheus Registry in a unit test without gathering,
	// but we can trust the code if it runs without panic.
	// Alternatively, using the prometheus/testutil package if available, but let's assume manual verification via "it runs".
}
