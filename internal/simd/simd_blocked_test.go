package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func float32EqualTol(a, b float32, tolerance float32) bool {
	return math.Abs(float64(a-b)) <= float64(tolerance)
}

func TestDimensionSpecificBlocked(t *testing.T) {
	dims := []int{768, 1024, 1536, 2048, 3072}

	for _, dim := range dims {
		t.Run(fmt.Sprintf("euclidean%d", dim), func(t *testing.T) {
			a := make([]float32, dim)
			b := make([]float32, dim)

			for i := 0; i < dim; i++ {
				a[i] = rand.Float32()
				b[i] = rand.Float32()
			}

			expected, err := euclideanGeneric(a, b)
			assert.NoError(t, err)

			var result float32
			var err2 error

			switch dim {
			case 768:
				result, err2 = euclidean768Blocked(a, b)
			case 1024:
				result, err2 = euclidean1024Blocked(a, b)
			case 1536:
				result, err2 = euclidean1536Blocked(a, b)
			case 2048:
				result, err2 = euclidean2048Blocked(a, b)
			case 3072:
				result, err2 = euclidean3072Blocked(a, b)
			}

			assert.NoError(t, err2)
			assert.True(t, float32EqualTol(expected, result, 0.1),
				fmt.Sprintf("dim=%d: expected=%v, got=%v", dim, expected, result))
		})
	}
}

func TestDotProductBlocked_Float32(t *testing.T) {
	// Test cases for blocked SIMD (Dimensions > 1024)
	dims := []int{1024, 1536, 2048, 3072, 4096, 4097} // 4097 to test odd trailing handling

	for _, dim := range dims {
		t.Run(fmt.Sprint(dim), func(t *testing.T) {
			a := make([]float32, dim)
			b := make([]float32, dim)

			var expected float32
			for i := 0; i < dim; i++ {
				a[i] = rand.Float32()
				b[i] = rand.Float32()
				expected += a[i] * b[i]
			}

			// Sanity check baseline
			baseline, err0 := DotProduct(a, b)
			assert.NoError(t, err0)
			assert.InDelta(t, expected, baseline, 0.01, "Baseline accuracy check")

			// Blocked version
			result, err := DotProductFloat32Blocked(a, b)
			assert.NoError(t, err)
			assert.InDelta(t, expected, result, 0.01, "Blocked implementation accuracy check")
		})
	}
}

func TestEuclideanBlocked_Float32(t *testing.T) {
	dims := []int{1024, 1536, 2048, 3072, 4097}

	for _, dim := range dims {
		t.Run(fmt.Sprint(dim), func(t *testing.T) {
			a := make([]float32, dim)
			b := make([]float32, dim)

			for i := 0; i < dim; i++ {
				a[i] = rand.Float32()
				b[i] = rand.Float32()
			}

			expected, err1 := EuclideanDistance(a, b)
			assert.NoError(t, err1)
			result, err2 := L2Float32Blocked(a, b)
			assert.NoError(t, err2)

			assert.InDelta(t, expected, result, 0.01, "Blocked L2 accuracy check")
		})
	}
}

// Benchmarks to prove optimizations
func BenchmarkDotProduct_3072(b *testing.B) {
	dim := 3072
	a := make([]float32, dim)
	bVec := make([]float32, dim)

	b.Run("Baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = DotProduct(a, bVec)
		}
	})

	b.Run("Blocked", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = DotProductFloat32Blocked(a, bVec)
		}
	})
}
