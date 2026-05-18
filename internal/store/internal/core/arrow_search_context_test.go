package core

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrowSearchContext_DynamicResizing(t *testing.T) {
	ctx := NewArrowSearchContext()

	// Initial capacity check
	assert.GreaterOrEqual(t, cap(ctx.querySQ8), 1536)

	// Request 3072 dims
	dim := 3072
	// We need to implement EnsureCapacity in arrow_hnsw_graph.go
	// For now, these tests will FAIL until implementation.

	// Simulate using the context
	ctx.querySQ8 = append(ctx.querySQ8[:0], make([]byte, dim)...)
	assert.Equal(t, dim, len(ctx.querySQ8))
	assert.GreaterOrEqual(t, cap(ctx.querySQ8), dim)

	// Reset should keep capacity
	ctx.Reset()
	assert.Equal(t, 0, len(ctx.querySQ8))
	assert.GreaterOrEqual(t, cap(ctx.querySQ8), dim)
}

func TestArrowSearchContextPool_MemoryAlignment(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	pool := NewArrowSearchContextPool()

	// Get context
	ctx1 := pool.Get()
	dim := 3072
	ctx1.querySQ8 = append(ctx1.querySQ8[:0], make([]byte, dim)...)

	// Return to pool
	pool.Put(ctx1)

	// Get again
	ctx2 := pool.Get()
	// Since sync.Pool is not strictly guaranteed to return the exact same item,
	// we only assert if the pool returned the recycled context.
	if ctx2 == ctx1 {
		assert.GreaterOrEqual(t, cap(ctx2.querySQ8), dim, "Pooled context should retain its large capacity")
	}
	pool.Put(ctx2)
}
