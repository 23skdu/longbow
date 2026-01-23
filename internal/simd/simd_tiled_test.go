package simd

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEuclideanDistanceTiledBatch(t *testing.T) {
	dims := 3072
	numVecs := 64
	query := make([]float32, dims)
	vectors := make([][]float32, numVecs)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rand.Float32()
		}
	}
	for j := 0; j < dims; j++ {
		query[j] = rand.Float32()
	}

	expected := make([]float32, numVecs)
	EuclideanDistanceBatch(query, vectors, expected)

	results := make([]float32, numVecs)
	// We will implement this
	EuclideanDistanceTiledBatch(query, vectors, results)

	for i := range results {
		assert.InDelta(t, expected[i], results[i], 0.01, fmt.Sprintf("Vector %d mismatch", i))
	}
}

func BenchmarkTiledVsStandard_3072(b *testing.B) {
	dims := 3072
	numVecs := 64
	query := make([]float32, dims)
	vectors := make([][]float32, numVecs)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
	}
	results := make([]float32, numVecs)

	b.Run("Standard", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EuclideanDistanceBatch(query, vectors, results)
		}
	})

	b.Run("Tiled", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EuclideanDistanceTiledBatch(query, vectors, results)
		}
	})
}
