package simd

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEuclideanDistanceTiledBatch_Remainder(t *testing.T) {
	// Dimensions not multiple of 8 or 1024
	dims := 1030 
	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	vectors := make([][]float32, 2)
	vectors[0] = make([]float32, dims)
	vectors[1] = make([]float32, dims)

	for i := 0; i < dims; i++ {
		vectors[0][i] = float32(i + 1)
		vectors[1][i] = float32(i + 2)
	}

	results := make([]float32, 2)
	err := EuclideanDistanceTiledBatch(query, vectors, results)
	require.NoError(t, err)

	// Calculate expected
	expected := make([]float32, 2)
	for j := 0; j < 2; j++ {
		var sum float64
		for i := 0; i < dims; i++ {
			diff := float64(vectors[j][i] - query[i])
			sum += diff * diff
		}
		expected[j] = float32(math.Sqrt(sum))
	}

	require.InDeltaSlice(t, expected, results, 1e-5)
}

func TestDotProductTiledBatch_Remainder(t *testing.T) {
	dims := 1030
	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	vectors := make([][]float32, 2)
	vectors[0] = make([]float32, dims)
	vectors[1] = make([]float32, dims)

	for i := 0; i < dims; i++ {
		vectors[0][i] = float32(i + 1)
		vectors[1][i] = float32(i + 2)
	}

	results := make([]float32, 2)
	err := DotProductTiledBatch(query, vectors, results)
	require.NoError(t, err)

	// Calculate expected
	expected := make([]float32, 2)
	for j := 0; j < 2; j++ {
		var sum float64
		for i := 0; i < dims; i++ {
			sum += float64(vectors[j][i] * query[i])
		}
		expected[j] = float32(sum)
	}

	require.InDeltaSlice(t, expected, results, 512.0)
}
