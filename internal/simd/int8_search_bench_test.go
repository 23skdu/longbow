package simd

import (
	"math/rand"
	"testing"
)

func BenchmarkInt8DenseLoop_50k(b *testing.B) {
	dim := 128
	n := 50000

	dataset := make([]int8, n*dim)
	for i := range dataset {
		dataset[i] = int8(rand.Intn(256) - 128)
	}

	query := make([]int8, dim)
	for j := range query {
		query[j] = int8(rand.Intn(256) - 128)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		best := float32(1e30)
		for j := 0; j < n; j++ {
			d, _ := EuclideanDistanceInt8(query, dataset[j*dim:(j+1)*dim])
			if d < best {
				best = d
			}
		}
		_ = best
	}
}

func BenchmarkInt8DotProductLoop_50k(b *testing.B) {
	dim := 128
	n := 50000

	dataset := make([]int8, n*dim)
	for i := range dataset {
		dataset[i] = int8(rand.Intn(256) - 128)
	}

	query := make([]int8, dim)
	for j := range query {
		query[j] = int8(rand.Intn(256) - 128)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		best := float32(-1e30)
		for j := 0; j < n; j++ {
			d, _ := DotProductInt8(query, dataset[j*dim:(j+1)*dim])
			if d > best {
				best = d
			}
		}
		_ = best
	}
}

func BenchmarkFloat32Euclidean_128(b *testing.B) {
	dim := 128
	a := make([]float32, dim)
	bVec := make([]float32, dim)
	for i := range a {
		a[i] = rand.Float32()
		bVec[i] = rand.Float32()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EuclideanDistance(a, bVec)
	}
}
