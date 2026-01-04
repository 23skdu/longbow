package simd_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
)

// euclidGenericCopy is a copy of the scalar fallback for comparison
func euclidGenericCopy(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		var sum float32
		for j := 0; j < len(query); j++ {
			d := query[j] - v[j]
			sum += d * d
		}
		results[i] = float32(math.Sqrt(float64(sum)))
	}
}

func Benchmark_Compare_EuclideanBatch(b *testing.B) {
	dims := 128
	batchSize := 1000

	query := make([]float32, dims)
	vectors := make([][]float32, batchSize)
	results := make([]float32, batchSize)

	r := rand.New(rand.NewSource(42))
	for i := 0; i < dims; i++ {
		query[i] = r.Float32()
	}
	for i := 0; i < batchSize; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = r.Float32()
		}
	}

	b.Run("Scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			euclidGenericCopy(query, vectors, results)
		}
	})

	b.Run("SIMD", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			simd.EuclideanDistanceBatch(query, vectors, results)
		}
	})
}
