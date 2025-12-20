package simd

import (
	"math/rand"
	"testing"
	"time"
)

func TestEuclideanDistanceBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	dim := 128
	numVectors := 100

	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	results := make([]float32, numVectors)

	// This function doesn't exist yet, so this test will fail to compile
	EuclideanDistanceBatch(query, vectors, results)

	// Verify results against single-vector implementation
	for i, v := range vectors {
		expected := EuclideanDistance(query, v)
		if results[i] != expected {
			t.Errorf("Mismatch at index %d: expected %f, got %f", i, expected, results[i])
		}
	}
}

func BenchmarkEuclideanDistanceBatch(b *testing.B) {
	rng := rand.New(rand.NewSource(12345))
	dim := 128
	numVectors := 1000

	query := make([]float32, dim)
	for i := range query {
		query[i] = rng.Float32()
	}

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}

	results := make([]float32, numVectors)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EuclideanDistanceBatch(query, vectors, results)
	}
}
