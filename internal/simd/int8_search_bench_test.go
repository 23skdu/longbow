package simd

import (
	"math/rand"
	"testing"
)

// BenchmarkInt8DenseLoop_50k runs the benchmark int8 dense loop_50k operation.
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
	for b.Loop() {
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

// BenchmarkInt8DotProductLoop_50k runs the benchmark int8 dot product loop_50k operation.
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
	for b.Loop() {
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

// BenchmarkFloat32Euclidean_128 runs the benchmark float32 euclidean_128 operation.
func BenchmarkFloat32Euclidean_128(b *testing.B) {
	dim := 128
	a := make([]float32, dim)
	bVec := make([]float32, dim)
	for i := range a {
		a[i] = rand.Float32()
		bVec[i] = rand.Float32()
	}
	b.ResetTimer()
	for b.Loop() {
		_, _ = EuclideanDistance(a, bVec)
	}
}
