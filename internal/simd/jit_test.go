package simd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJITEuclidean(t *testing.T) {
	err := initJIT()
	require.NoError(t, err)
	require.True(t, jitEnabled)

	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}

	// Expected: sqrt((1-4)^2 + (2-5)^2 + (3-6)^2) = sqrt(9 + 9 + 9) = sqrt(27) = 5.196152
	expected, err := euclideanGeneric(a, b)
	require.NoError(t, err)
	actual := jitRT.Euclidean(a, b)

	assert.InDelta(t, expected, actual, 0.0001)
}

func TestJITEuclideanBatch(t *testing.T) {
	err := initJIT()
	require.NoError(t, err)

	q := []float32{1.0, 1.0, 1.0}
	vecs := [][]float32{
		{1.0, 1.0, 1.0},  // dist = 0
		{2.0, 2.0, 2.0},  // dist = sqrt(1+1+1) = 1.732
		{4.0, 5.0, 13.0}, // dist = sqrt(9+16+144) = sqrt(169) = 13
	}

	res, err := jitRT.EuclideanBatch(q, vecs)
	require.NoError(t, err)
	require.Len(t, res, 3)

	assert.InDelta(t, float32(0.0), res[0], 0.0001)
	assert.InDelta(t, float32(1.73205), res[1], 0.0001)
	assert.InDelta(t, float32(13.0), res[2], 0.0001)
}

func BenchmarkJITEuclidean(b *testing.B) {
	_ = initJIT()
	a := make([]float32, 384)
	v := make([]float32, 384)
	for i := range a {
		a[i] = 1.0
		v[i] = 2.0
	}

	b.Run("JIT", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = jitRT.Euclidean(a, v)
		}
	})

	b.Run("Generic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = euclideanGeneric(a, v)
		}
	})

	b.Run("SIMD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistance(a, v)
		}
	})
}

func BenchmarkJITEuclideanBatch(b *testing.B) {
	_ = initJIT()
	dim := 384
	batchSize := 100
	q := make([]float32, dim)
	vecs := make([][]float32, batchSize)
	flatVecs := make([]float32, batchSize*dim)

	for i := 0; i < batchSize; i++ {
		vecs[i] = flatVecs[i*dim : (i+1)*dim]
		for j := 0; j < dim; j++ {
			vecs[i][j] = float32(j)
		}
	}

	b.Run("JIT_Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = jitRT.EuclideanBatch(q, vecs)
		}
	})

	b.Run("Native_Loop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < batchSize; j++ {
				_, _ = EuclideanDistance(q, vecs[j])
			}
		}
	})
}
