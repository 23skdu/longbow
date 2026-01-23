package simd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticDispatch_Euclidean(t *testing.T) {
	// Verify the global function variable is initialized and functioning
	assert.NotNil(t, DistFunc, "DistFunc should be initialized")

	a := []float32{1, 2, 3, 4}
	b := []float32{4, 3, 2, 1}

	// Expected: sqrt((1-4)^2 + (2-3)^2 + (3-2)^2 + (4-1)^2)
	// = sqrt(9 + 1 + 1 + 9) = sqrt(20) = 4.472136
	expected := float32(math.Sqrt(20))

	// Call via the static function pointer
	result, err := DistFunc(a, b)
	assert.NoError(t, err)

	assert.InDelta(t, expected, result, 0.0001, "Distance calculation should be correct")
}
