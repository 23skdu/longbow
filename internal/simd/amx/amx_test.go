package amx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAMX(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	t.Run("Dot", func(t *testing.T) {
		res, err := DotAMX(a, b)
		assert.NoError(t, err)
		assert.Equal(t, float32(70), res)

		_, _ = DotAMX(nil, nil)
	})

	t.Run("L2", func(t *testing.T) {
		res, err := L2AMX(a, b)
		assert.NoError(t, err)
		assert.Equal(t, float32(64), res) // (1-5)^2 + (2-6)^2 + (3-7)^2 + (4-8)^2 = 16*4 = 64

		_, _ = L2AMX(nil, nil)
	})
}
