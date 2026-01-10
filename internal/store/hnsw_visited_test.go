package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestArrowSearchContext_SparseClearing verifies that sparse clearing
// correctly resets the bitset using the visited history.
func TestArrowSearchContext_SparseClearing(t *testing.T) {
	// Create context with bitset size 1000
	ctx := NewArrowSearchContext()
	ctx.visited.Grow(1000)

	// Simulate a search visiting some nodes
	ids := []uint32{1, 50, 100, 999}
	for _, id := range ids {
		ctx.Visit(id)
	}

	// Verify bits are set
	for _, id := range ids {
		assert.True(t, ctx.visited.IsSet(id), "Bit %d should be set", id)
	}
	// Verify list is populated
	assert.Equal(t, len(ids), len(ctx.visitedList), "VisitedList should track IDs")

	// Perform Sparse Reset
	start := time.Now()
	ctx.ResetVisited()
	duration := time.Since(start)
	t.Logf("Sparse Reset took %v", duration)

	// Verify bits are cleared
	for _, id := range ids {
		assert.False(t, ctx.visited.IsSet(id), "Bit %d should be cleared", id)
	}
	// Verify list is cleared
	assert.Equal(t, 0, len(ctx.visitedList), "VisitedList should be empty after reset")

	// Verify cleanliness (sanity check random bit)
	assert.False(t, ctx.visited.IsSet(500), "Random bit should be clear")
}
