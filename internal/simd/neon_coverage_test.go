//go:build arm64
// +build arm64

package simd

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestNEONComprehensive(t *testing.T) {
	initializeDispatch()

	t.Run("SpecializedEuclidean", func(t *testing.T) {
		dims := []int{128, 384, 768, 1024, 1536, 3072}
		for _, d := range dims {
			a := make([]float32, d)
			b := make([]float32, d)
			for i := range a {
				a[i] = float32(i)
				b[i] = float32(i + 1)
			}

			var dist float32
			var err error
			switch d {
			case 128:
				dist, err = euclidean128NEON(a, b)
			case 384:
				dist, err = euclidean384NEON(a, b)
			case 768:
				dist, err = euclidean768NEON(a, b)
			case 1024:
				dist, err = euclidean1024NEON(a, b)
			case 1536:
				dist, err = euclidean1536NEON(a, b)
			case 3072:
				dist, err = euclidean3072NEON(a, b)
			}
			assert.NoError(t, err)
			assert.InDelta(t, math.Sqrt(float64(d)), float64(dist), 1e-5)
		}
	})

	t.Run("MatchNeon_NonEmpty", func(t *testing.T) {
		ops := []CompareOp{CompareEq, CompareNeq, CompareGt, CompareGe, CompareLt, CompareLe}
		
		t.Run("Int64", func(t *testing.T) {
			src := []int64{1, 2, 3, 4, 1, 2, 3, 4}
			dst := make([]byte, len(src))
			for _, op := range ops {
				err := matchInt64Neon(src, 2, op, dst)
				assert.NoError(t, err)
			}
		})

		t.Run("Int32", func(t *testing.T) {
			src := []int32{1, 2, 3, 4, 1, 2, 3, 4}
			dst := make([]byte, len(src))
			for _, op := range ops {
				err := matchInt32Neon(src, 2, op, dst)
				assert.NoError(t, err)
			}
		})

		t.Run("Float32", func(t *testing.T) {
			src := []float32{1, 2, 3, 4, 1, 2, 3, 4}
			dst := make([]byte, len(src))
			for _, op := range ops {
				err := matchFloat32Neon(src, 2, op, dst)
				assert.NoError(t, err)
			}
		})

		t.Run("Float64", func(t *testing.T) {
			src := []float64{1, 2, 3, 4, 1, 2, 3, 4}
			dst := make([]byte, len(src))
			for _, op := range ops {
				err := matchFloat64Neon(src, 2, op, dst)
				assert.NoError(t, err)
			}
		})
	})

	t.Run("GenericMatchers_Extended", func(t *testing.T) {
		ops := []CompareOp{CompareEq, CompareNeq, CompareGt, CompareGe, CompareLt, CompareLe, CompareOp(999)}
		
		t.Run("Int64", func(t *testing.T) {
			src := []int64{1, 2}
			dst := make([]byte, 2)
			for _, op := range ops {
				_ = matchInt64Generic(src, 1, op, dst)
			}
		})

		t.Run("Int32", func(t *testing.T) {
			src := []int32{1, 2}
			dst := make([]byte, 2)
			for _, op := range ops {
				_ = matchInt32Generic(src, 1, op, dst)
			}
		})

		t.Run("Float32", func(t *testing.T) {
			src := []float32{1, 2}
			dst := make([]byte, 2)
			for _, op := range ops {
				_ = matchFloat32Generic(src, 1, op, dst)
			}
		})

		t.Run("Float64", func(t *testing.T) {
			src := []float64{1, 2}
			dst := make([]byte, 2)
			for _, op := range ops {
				_ = matchFloat64Generic(src, 1, op, dst)
			}
		})
	})

	t.Run("Cosine_ZeroVector", func(t *testing.T) {
		a := []float32{0, 0}
		b := []float32{1, 1}
		d, _ := cosineGeneric(a, b)
		assert.Equal(t, float32(1.0), d)
		
		d2, _ := CosineDistance(a, b)
		assert.Equal(t, float32(1.0), d2)
	})

	t.Run("MatchFloat64_Exhaustive", func(t *testing.T) {
		src := []float64{1.0, 2.0}
		dst := make([]byte, 2)
		ops := []CompareOp{CompareEq, CompareNeq, CompareGt, CompareGe, CompareLt, CompareLe}
		for _, op := range ops {
			_ = matchFloat64Generic(src, 1.0, op, dst)
			_ = matchFloat64Generic(src, 2.0, op, dst)
			_ = matchFloat64Generic(src, 0.0, op, dst)
			_ = matchFloat64Generic(src, 3.0, op, dst)
		}
	})

	t.Run("Activations_NonEmpty", func(t *testing.T) {
		src := []float32{0, 1, 2, 3, 4, 5, 6, 7}
		dst := make([]float32, len(src))
		
		sigmoidNEON(src, dst)
		expNEON(src, dst)
		logNEON(src, dst)
		softmaxNEON(src, dst)
	})

	t.Run("Haversine_Large", func(t *testing.T) {
		points := make([]lbcore.GeoPoint, 2048)
		results := make([]float32, 2048)
		HaversineBatch(0, 0, points, 6371.0, results)
	})

	t.Run("Bitwise_Errors", func(t *testing.T) {
		a := []byte{1}
		b := []byte{1, 2}
		assert.Error(t, AndBytes(a, b))
		assert.Error(t, OrBytes(a, b))
	})

	t.Run("UnrolledBaselines", func(t *testing.T) {
		a := make([]float32, 128)
		b := make([]float32, 128)
		_, _ = euclidean128Unrolled4x(a, b)
		_, _ = dot128Unrolled4x(a, b)
		
		a384 := make([]float32, 384)
		b384 := make([]float32, 384)
		_, _ = euclidean384Unrolled4x(a384, b384)
		
		a768 := make([]float32, 768)
		b768 := make([]float32, 768)
		_, _ = euclidean768Unrolled4x(a768, b768)
		
		a1536 := make([]float32, 1536)
		b1536 := make([]float32, 1536)
		_, _ = euclidean1536Unrolled4x(a1536, b1536)
	})

	t.Run("Reductions", func(t *testing.T) {
		src := []float32{1, 2, 3, 4}
		assert.Equal(t, float32(10), sumNEON(src))
		assert.Equal(t, float32(4), maxNEON(src))
		assert.Equal(t, float32(1), minNEON(src))
		assert.Equal(t, 3, argMaxNEON(src))
		assert.Equal(t, 0, argMinNEON(src))
	})

	t.Run("MatrixOps", func(t *testing.T) {
		a := []float32{1, 2, 3, 4}
		b := []float32{5, 6, 7, 8}
		dst := make([]float32, 4)
		matMulNEON(a, b, 2, 2, 2, dst)
	})

	t.Run("Distances", func(t *testing.T) {
		a := []float32{1, 2, 3, 4}
		b := []float32{5, 6, 7, 8}
		_, _ = manhattanNEON(a, b)
		_, _ = chebyshevNEON(a, b)
		_, _ = brayCurtisNEON(a, b)
		_, _ = dotFloat64NEON([]float64{1, 2}, []float64{3, 4})
	})

	t.Run("Scatter_Branches", func(t *testing.T) {
		dst := make([]float32, 10)
		targets := []uint32{1, 2, 3}
		weights := []float32{0.5, 0.5} // len(weights) < len(targets)
		accumulateWeightedScatterNEON(dst, targets, weights, 2.0)
		assert.Equal(t, float32(1.0), dst[1])
		assert.Equal(t, float32(1.0), dst[2])
		assert.Equal(t, float32(0.0), dst[3])
		
		accumulateWeightedScatterNEON(nil, nil, nil, 0)
	})

	t.Run("DotInt", func(t *testing.T) {
		a := []byte{0x12, 0x34}
		b := []byte{0x56, 0x78}
		_, _ = dotInt4Neon(a, b)
		_, _ = dotInt2Neon(a, b)
		_, _ = dotInt4Neon(nil, nil)
		_, _ = dotInt2Neon(nil, nil)
	})

	t.Run("F16Distances_Remainder", func(t *testing.T) {
		a := []float16.Num{float16.New(1.0), float16.New(2.0), float16.New(3.0), float16.New(4.0), float16.New(5.0)}
		b := []float16.Num{float16.New(4.0), float16.New(5.0), float16.New(6.0), float16.New(7.0), float16.New(8.0)}
		_, _ = euclideanF16Unrolled4x(a, b)
		_, _ = dotF16Unrolled4x(a, b)
		_, _ = cosineF16Unrolled4x(a, b)
		
		_, _ = euclideanF16Unrolled4x(nil, nil)
		_, _ = dotF16Unrolled4x(nil, nil)
		_, _ = cosineF16Unrolled4x(nil, nil)
	})
	
	t.Run("PublicMatchErrors", func(t *testing.T) {
		src := []int64{1}
		dst := make([]byte, 2) // Mismatch
		assert.Error(t, MatchInt64(src, 1, CompareEq, dst))
		
		src32 := []int32{1}
		assert.Error(t, MatchInt32(src32, 1, CompareEq, dst))
		
		srcf32 := []float32{1.0}
		assert.Error(t, MatchFloat32(srcf32, 1.0, CompareEq, dst))
		
		srcf64 := []float64{1.0}
		assert.Error(t, MatchFloat64(srcf64, 1.0, CompareEq, dst))
	})

	t.Run("Activations_Empty", func(t *testing.T) {
		sigmoidNEON(nil, nil)
		expNEON(nil, nil)
		logNEON(nil, nil)
	})
}
