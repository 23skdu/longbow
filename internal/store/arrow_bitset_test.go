package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrowBitset_FilterVisited(t *testing.T) {
	b := NewArrowBitset(100)

	// Pre-set some bits
	b.Set(10)
	b.Set(20)

	// Input with mix of visited and unvisited
	ids := []uint32{10, 11, 20, 21, 5, 10}

	filtered := b.FilterVisited(ids)

	// Should return only 11, 21, 5 (in order or arbitrary? In order usually)
	// 10 was set. 11 set during filter. 20 set. 21 set during filter. 5 set during. 10 set.
	// Wait, 10 appears twice. First time it's visited.
	// 11 is unvisited. It gets set.
	// 20 is visited.
	// 21 is unvisited. It gets set.
	// 5 is unvisited. It gets set.
	// 10 is visited.

	expected := []uint32{11, 21, 5}
	assert.Equal(t, expected, filtered)

	// Verify they are now set
	assert.True(t, b.IsSet(11))
	assert.True(t, b.IsSet(21))
	assert.True(t, b.IsSet(5))
}
