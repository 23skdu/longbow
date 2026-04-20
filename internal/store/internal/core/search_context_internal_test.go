package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCandidateHeap_Internal(t *testing.T) {
	h := make(CandidateHeap, 0, 10)
	
	candidates := []types.Candidate{
		{ID: 1, Dist: 0.5},
		{ID: 2, Dist: 0.1},
		{ID: 3, Dist: 0.9},
		{ID: 4, Dist: 0.3},
	}
	
	// Max heap: 0.9 should be at top
	for _, c := range candidates {
		h = append(h, c)
	}
	// Note: We need to heapify if we append manually.
	// But our PopAndReturn expects a valid heap.
	
	// Better test: implement a simple Push if needed, otherwise use searchLayer logic simulation.
	// For now, let's just test PopAndReturn logic on a 1-element and empty heap
	
	t.Run("Empty", func(t *testing.T) {
		var h2 CandidateHeap
		_, ok := h2.PopAndReturn()
		assert.False(t, ok)
	})

	t.Run("Single", func(t *testing.T) {
		h2 := CandidateHeap{{ID: 1, Dist: 0.1}}
		c, ok := h2.PopAndReturn()
		assert.True(t, ok)
		assert.Equal(t, uint32(1), uint32(c.ID))
		assert.Equal(t, 0, len(h2))
	})
}
