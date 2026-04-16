package simd

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/stretchr/testify/assert"
)

func TestCoverage_Float64Distances(t *testing.T) {
	a := []float64{1.0, 2.0, 3.0, 4.0}
	b := []float64{1.1, 2.1, 3.1, 4.1}

	t.Run("Euclidean", func(t *testing.T) {
		res, err := EuclideanDistanceFloat64(a, b)
		assert.NoError(t, err)
		assert.Greater(t, res, float32(0))
	})

	t.Run("Cosine", func(t *testing.T) {
		res, err := CosineDistanceFloat64(a, b)
		assert.NoError(t, err)
		assert.Greater(t, res, float32(0))
	})

	t.Run("Dot", func(t *testing.T) {
		// Test the newly added dotProductFloat64Impl
		res, err := dotProductFloat64Impl(a, b)
		assert.NoError(t, err)
		assert.Greater(t, res, float32(0))
	})
}

func TestCoverage_ComplexDistances(t *testing.T) {
	a := []complex64{1 + 1i, 2 + 2i}
	b := []complex64{1.1 + 1.1i, 2.1 + 2.1i}

	t.Run("Complex64", func(t *testing.T) {
		res, err := EuclideanDistanceComplex64(a, b)
		assert.NoError(t, err)
		assert.Greater(t, res, float32(0))
	})

	a128 := []complex128{1 + 1i, 2 + 2i}
	b128 := []complex128{1.1 + 1.1i, 2.1 + 2.1i}

	t.Run("Complex128", func(t *testing.T) {
		res, err := EuclideanDistanceComplex128(a128, b128)
		assert.NoError(t, err)
		assert.Greater(t, res, float32(0))
	})
}

func TestCoverage_Stubs(t *testing.T) {
	// Exercise stubs in simd_stubs.go (on non-amd64)
	src := []int64{1, 2, 3, 4}
	dst := make([]byte, 4)
	
	t.Run("MatchInt64AVX2_Stub", func(t *testing.T) {
		err := matchInt64AVX2(src, 2, CompareEq, dst)
		assert.NoError(t, err)
	})

	t.Run("MatchFloat32AVX512_Stub", func(t *testing.T) {
		fsrc := []float32{1.0, 2.0}
		err := matchFloat32AVX512(fsrc, 1.0, CompareEq, dst[:2])
		assert.NoError(t, err)
	})

	t.Run("L2SquaredAVX512_Stub", func(t *testing.T) {
		v1 := []float32{1, 2}
		v2 := []float32{3, 4}
		res, err := l2SquaredAVX512(v1, v2)
		assert.NoError(t, err)
		assert.Equal(t, float32(8.0), res)
	})
}

func TestCoverage_SpecializedWrappers(t *testing.T) {
	a := make([]float32, 16)
	b := make([]float32, 16)
	for i := range a {
		a[i] = float32(i)
		b[i] = float32(i + 1)
	}

	t.Run("Euclidean16AVX512", func(t *testing.T) {
		// On non-avx512 this might error or call stub
		_, _ = euclidean16AVX512Wrapper(a, b)
	})

	t.Run("Cosine16AVX512", func(t *testing.T) {
		_, _ = cosine16AVX512Wrapper(a, b)
	})
}

func TestCoverage_F16BatchStubs(t *testing.T) {
	q := make([]float16.Num, 8)
	vecs := [][]float16.Num{make([]float16.Num, 8)}
	res := make([]float32, 1)

	_ = euclideanF16BatchAVX2(q, vecs, res)
	_ = euclideanF16BatchAVX512(q, vecs, res)
}

func TestCoverage_Misc(t *testing.T) {
	dummy := make([]byte, 16)
	prefetchNTA(unsafe.Pointer(&dummy[0]))
	
	andBytesAVX2(dummy, dummy)
	orBytesAVX2(dummy, dummy)
	isAllZerosAVX2(dummy)
}
