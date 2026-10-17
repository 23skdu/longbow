package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowBitset_FilterAndMarkUnvisited(t *testing.T) {
	bs := NewArrowBitset(1024)

	// Mark a few initial nodes
	bs.Set(5)
	bs.Set(100)
	bs.Set(250)

	candidates := []uint32{0, 5, 10, 100, 200, 250, 500, 1500} // 1500 tests auto-grow
	maxCommitted := int64(1000)

	var dst []uint32
	filtered := bs.FilterAndMarkUnvisited(candidates, maxCommitted, false, dst)

	// Nodes 5, 100, 250 were already visited
	// Node 1500 is >= maxCommitted (and allowUncommitted=false)
	// Expected unvisited and committed: [0, 10, 200, 500]
	expected := []uint32{0, 10, 200, 500}
	require.Equal(t, expected, filtered)

	// Now all these nodes must be marked as visited
	for _, id := range expected {
		assert.True(t, bs.IsSet(int(id)), "node %d should be visited", id)
	}

	// Calling FilterAndMarkUnvisited again on the same candidates should yield empty
	var dst2 []uint32
	filtered2 := bs.FilterAndMarkUnvisited(candidates, maxCommitted, false, dst2)
	assert.Empty(t, filtered2)

	// Allow uncommitted should include node 1500
	var dst3 []uint32
	filtered3 := bs.FilterAndMarkUnvisited([]uint32{1500}, maxCommitted, true, dst3)
	assert.Equal(t, []uint32{1500}, filtered3)
	assert.True(t, bs.IsSet(1500))
}
