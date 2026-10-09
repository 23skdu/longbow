package simd

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDequantizeL2(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	testDims := []int{1, 3, 7, 8, 15, 16, 31, 32, 64, 128, 256, 512, 1024}
	for _, dim := range testDims {
		q := make([]float32, dim)
		v := make([]uint8, dim)
		for i := 0; i < dim; i++ {
			q[i] = rng.Float32()*10.0 - 5.0
			v[i] = uint8(rng.Intn(256))
		}

		minV := float32(-2.5)
		maxV := float32(7.5)
		scale := (maxV - minV) / 255.0

		// Compare generic with implementation
		expectedSum := dequantL2Float32Uint8Generic(q, v, minV, scale)
		gotSum := dequantL2Float32Uint8Impl(q, v, minV, scale)

		// Allow small floating-point difference due to FMA vs separate mul/add
		diff := math.Abs(float64(expectedSum - gotSum))
		relErr := diff / (float64(expectedSum) + 1e-6)
		require.Lessf(t, relErr, 1e-4, "Mismatch at dim %d: expected %f, got %f (diff %f)", dim, expectedSum, gotSum, diff)

		// Also check public functions
		d1 := DequantizeL2Float32Uint8(q, v, minV, scale)
		require.Greater(t, d1, float32(0))

		vi8 := make([]int8, dim)
		for i := range v {
			vi8[i] = int8(v[i])
		}
		d2 := DequantizeL2Float32Int8(q, vi8, minV, scale)
		require.Equal(t, d1, d2)

		// Test unquantized
		l2U8 := L2Float32Uint8(q, v)
		require.Greater(t, l2U8, float32(0))
		l2I8 := L2Float32Int8(q, vi8)
		require.Equal(t, l2U8, l2I8)

		// Test two uint8s
		v2 := make([]uint8, dim)
		for i := range v2 {
			v2[i] = uint8(rng.Intn(256))
		}
		dUint8 := DequantizeL2Uint8Uint8(v, v2, scale)
		require.GreaterOrEqual(t, dUint8, float32(0))
	}
}

func BenchmarkDequantizeL2(b *testing.B) {
	dim := 128
	q := make([]float32, dim)
	v := make([]uint8, dim)
	for i := 0; i < dim; i++ {
		q[i] = float32(i) * 0.1
		v[i] = uint8(i % 256)
	}
	minV := float32(-1.0)
	scale := float32(0.01)

	b.Run("Generic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dequantL2Float32Uint8Generic(q, v, minV, scale)
		}
	})

	b.Run("SIMD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dequantL2Float32Uint8Impl(q, v, minV, scale)
		}
	})
}
