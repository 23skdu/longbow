package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCandidateHeap_Internal(t *testing.T) {
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
