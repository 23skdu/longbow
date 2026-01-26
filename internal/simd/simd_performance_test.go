package simd

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkSIMDDispatchOverhead benchmarks the overhead of function pointer dispatch
func BenchmarkSIMDDispatchOverhead(b *testing.B) {
	query := []float32{1.0, 2.0, 3.0, 4.0}
	vector := []float32{4.0, 3.0, 2.0, 1.0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This measures dispatch overhead + actual computation
		_, _ = EuclideanDistance(query, vector)
	}
}

// BenchmarkDirectFunctionCall benchmarks calling the implementation directly
func BenchmarkDirectFunctionCall(b *testing.B) {
	query := []float32{1.0, 2.0, 3.0, 4.0}
	vector := []float32{4.0, 3.0, 2.0, 1.0}

	// Call the dispatched function directly to measure just computation
	impl := euclideanDistanceImpl

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = impl(query, vector)
	}
}

// BenchmarkBatchOperations benchmarks batch distance computations
func BenchmarkBatchOperations(b *testing.B) {
	// Generate test data
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

	b.ResetTimer()
	b.Run("BatchEuclidean", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = EuclideanDistanceBatch(query, vectors, results)
		}
	})

	b.Run("BatchCosine", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = CosineDistanceBatch(query, vectors, results)
		}
	})

	b.Run("BatchDotProduct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = DotProductBatch(query, vectors, results)
		}
	})
}

// BenchmarkDifferentDimensions benchmarks performance across different vector dimensions
func BenchmarkDifferentDimensions(b *testing.B) {
	dimensions := []int{4, 8, 16, 32, 64, 128, 256, 512}

	for _, dims := range dimensions {
		b.Run(fmt.Sprintf("Euclidean_%d", dims), func(b *testing.B) {
			query := make([]float32, dims)
			vector := make([]float32, dims)

			r := rand.New(rand.NewSource(42))
			for i := 0; i < dims; i++ {
				query[i] = r.Float32()
				vector[i] = r.Float32()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = EuclideanDistance(query, vector)
			}
		})
	}
}

// BenchmarkImplementationComparison compares different SIMD implementations
func BenchmarkImplementationComparison(b *testing.B) {
	query := make([]float32, 128)
	vector := make([]float32, 128)

	r := rand.New(rand.NewSource(42))
	for i := 0; i < 128; i++ {
		query[i] = r.Float32()
		vector[i] = r.Float32()
	}

	impl := GetImplementation()
	b.Run(fmt.Sprintf("Euclidean_%s", impl), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistance(query, vector)
		}
	})
}

// BenchmarkMemoryAccessPatterns benchmarks different memory access patterns
func BenchmarkMemoryAccessPatterns(b *testing.B) {
	// Test contiguous vs non-contiguous memory access
	dims := 128
	numVectors := 1000

	// Contiguous: single large slice
	flatVectors := make([]float32, numVectors*dims)
	query := make([]float32, dims)

	r := rand.New(rand.NewSource(42))
	for i := 0; i < len(flatVectors); i++ {
		flatVectors[i] = r.Float32()
	}
	for i := 0; i < dims; i++ {
		query[i] = r.Float32()
	}

	results := make([]float32, numVectors)

	b.Run("FlatBatch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = EuclideanDistanceBatchFlat(query, flatVectors, numVectors, dims, results)
		}
	})

	// Non-contiguous: slice of slices
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = make([]float32, dims)
		copy(vectors[i], flatVectors[i*dims:(i+1)*dims])
	}

	b.Run("SliceOfSlices", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = EuclideanDistanceBatch(query, vectors, results)
		}
	})
}

// BenchmarkSIMDPrecisionValidation validates SIMD performance with consistent input
func BenchmarkSIMDPrecisionValidation(b *testing.B) {
	query := make([]float32, 128)
	vector := make([]float32, 128)

	r := rand.New(rand.NewSource(42))
	for i := 0; i < 128; i++ {
		query[i] = r.Float32()
		vector[i] = r.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := EuclideanDistance(query, vector)
		// Validate result is reasonable (positive distance)
		if result <= 0 {
			b.Errorf("Invalid distance result: %f", result)
		}
	}
}

// BenchmarkConcurrentSIMD tests SIMD performance under concurrent access
func BenchmarkConcurrentSIMD(b *testing.B) {

	query := make([]float32, 64)
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 64; i++ {
		query[i] = r.Float32()
	}

	b.RunParallel(func(pb *testing.PB) {
		localR := rand.New(rand.NewSource(r.Int63()))
		localQuery := make([]float32, 64)
		localVector := make([]float32, 64)

		copy(localQuery, query)

		for pb.Next() {
			// Generate random vector
			for i := 0; i < 64; i++ {
				localVector[i] = localR.Float32()
			}

			// Compute distance
			_, _ = EuclideanDistance(localQuery, localVector)
		}
	})
}
