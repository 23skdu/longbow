package simd

import (
	"testing"
	"github.com/stretchr/testify/assert"
	lbcore "github.com/23skdu/longbow/internal/core"
)

func TestBranchCoverage(t *testing.T) {
	initializeDispatch()

	t.Run("HaversineParallel", func(t *testing.T) {
		// Trigger parallel path (batchSize > 1024)
		points := make([]lbcore.GeoPoint, 2048)
		results := make([]float32, 2048)
		HaversineBatch(0, 0, points, 6371.0, results)
		assert.Equal(t, 2048, len(results))
	})

	t.Run("ScatterBranches", func(t *testing.T) {
		// Test various lengths to cover unrolling/remainder
		for _, n := range []int{1, 3, 4, 7, 8, 15} {
			dst := make([]float32, 100)
			targets := make([]uint32, n)
			weights := make([]float32, n)
			AccumulateWeightedScatter(dst, targets, weights, 2.0)
		}
	})

	t.Run("ComplexUnrolledRemainder", func(t *testing.T) {
		// Test remainders in complex unrolling
		for _, n := range []int{1, 2, 3, 5} {
			a := make([]complex128, n)
			b := make([]complex128, n)
			_, _ = euclideanComplex128Unrolled(a, b)
			_, _ = dotComplex128Unrolled(a, b)
			_, _ = cosineComplex128Unrolled(a, b)
			
			a64 := make([]complex64, n)
			b64 := make([]complex64, n)
			_, _ = euclideanComplex64Unrolled(a64, b64)
			_, _ = dotComplex64Unrolled(a64, b64)
			_, _ = cosineComplex64Unrolled(a64, b64)
		}
	})

	t.Run("TrigLengths", func(t *testing.T) {
		// Test remainders in trig functions
		for _, n := range []int{1, 3, 4, 7} {
			src := make([]float32, n)
			dst := make([]float32, n)
			sinFloat32Generic(src, dst)
			cosFloat32Generic(src, dst)
			atan2Float32Generic(src, src, dst)
		}
	})

	t.Run("CosineZeroNorms", func(t *testing.T) {
		// Cover the 'norm == 0' branches
		a := []float32{0, 0}
		b := []float32{1, 1}
		d, _ := cosineGeneric(a, b)
		assert.Equal(t, float32(1.0), d)
		
		d, _ = cosineGeneric(b, a)
		assert.Equal(t, float32(1.0), d)
	})

	t.Run("BrayCurtisZeroTotal", func(t *testing.T) {
		a := []float32{0, 0}
		b := []float32{0, 0}
		d, _ := BrayCurtisDistanceFloat32(a, b)
		assert.Equal(t, float32(0), d)
	})
	
	t.Run("MatchGenericRemainders", func(t *testing.T) {
		for _, n := range []int{1, 7, 8, 15} {
			dst := make([]byte, n)
			_ = matchInt64Generic(make([]int64, n), 0, CompareEq, dst)
			_ = matchInt32Generic(make([]int32, n), 0, CompareEq, dst)
			_ = matchFloat32Generic(make([]float32, n), 0, CompareEq, dst)
			_ = matchFloat64Generic(make([]float64, n), 0, CompareEq, dst)
		}
	})
}
