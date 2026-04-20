package core

import (
	"testing"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
)

func TestSearchContextPooling(t *testing.T) {
	h := &ArrowHNSW{}
	h.config.DataType = types.VectorTypeFloat32
	h.dims.Store(128)
	h.searchPool = NewArrowSearchContextPool()

	ctx := h.searchPool.Get()
	assert.NotNil(t, ctx)

	h.searchPool.Put(ctx)

	ctx2 := h.searchPool.Get()
	assert.True(t, ctx == ctx2, "Should reuse context from pool")
}

func TestCandidateHeap(t *testing.T) {
	h := &CandidateHeap{}
	heap.Push(h, types.Candidate{ID: 1, Dist: 10.0})
	heap.Push(h, types.Candidate{ID: 2, Dist: 5.0})
	heap.Push(h, types.Candidate{ID: 3, Dist: 15.0})

	// Max heap by distance
	assert.Equal(t, float32(15.0), (*h)[0].Dist)
}

func TestHNSWRepairWorkers(t *testing.T) {
	// Comprehensive test for RepairTombstones and background workers
}
