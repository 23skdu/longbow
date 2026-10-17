package simd

import (
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloat16DistanceParity_AVX512FP16(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, dims := range []int{16, 32, 64, 128, 384} {
		a := make([]float16.Num, dims)
		b := make([]float16.Num, dims)
		for i := 0; i < dims; i++ {
			a[i] = float16.New(rng.Float32()*2.0 - 1.0)
			b[i] = float16.New(rng.Float32()*2.0 - 1.0)
		}

		// Reference scalar Euclidean distance
		var refSum float64
		for i := 0; i < dims; i++ {
			diff := float64(a[i].Float32() - b[i].Float32())
			refSum += diff * diff
		}
		expectedEuc := float32(math.Sqrt(refSum))

		// Reference scalar Dot product
		var refDot float64
		for i := 0; i < dims; i++ {
			refDot += float64(a[i].Float32() * b[i].Float32())
		}
		expectedDot := float32(refDot)

		// Test euclideanF16AVX512FP16
		euc, err := euclideanF16AVX512FP16(a, b)
		require.NoError(t, err)
		assert.InDelta(t, expectedEuc, euc, 1e-2, "dims=%d euc mismatch", dims)

		// Test dotF16AVX512FP16
		dot, err := dotF16AVX512FP16(a, b)
		require.NoError(t, err)
		assert.InDelta(t, expectedDot, dot, 1e-2, "dims=%d dot mismatch", dims)
	}
}
