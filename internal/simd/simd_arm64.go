//go:build arm64

package simd

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ARM64 NEON implementations
// Defined in simd_arm64.s

func euclideanNEON(a, b []float32) float32
func dotNEON(a, b []float32) float32

func euclideanF16NEON(a, b []float16.Num) float32 //nolint:unused
func dotF16NEON(a, b []float16.Num) float32       //nolint:unused
func cosineF16NEON(a, b []float16.Num) float32    //nolint:unused

// Optimized for 384 dimensions
func euclidean384NEON(a, b []float32) float32 {
	return euclideanNEON(a, b)
}

func euclidean128NEON(a, b []float32) float32 {
	return euclideanNEON(a, b)
}

func dot384NEON(a, b []float32) float32 {
	return dotNEON(a, b)
}

func dot128NEON(a, b []float32) float32 {
	return dotNEON(a, b)
}

// Cosine is still generic for now (or combine Dot / Norms later)
func cosineNEON(a, b []float32) float32 {
	if !features.HasNEON {
		return cosineGeneric(a, b)
	}

	var dot, normA, normB float32
	n := len(a)
	i := 0

	for ; i <= n-4; i += 4 {
		dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
		normA += a[i]*a[i] + a[i+1]*a[i+1] + a[i+2]*a[i+2] + a[i+3]*a[i+3]
		normB += b[i]*b[i] + b[i+1]*b[i+1] + b[i+2]*b[i+2] + b[i+3]*b[i+3]
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

func adcBatchNEON(table []float32, flatCodes []byte, m int, results []float32) {
	adcBatchGeneric(table, flatCodes, m, results)
}
func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = euclideanNEON(query, v)
	}
}

func dotBatchNEON(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = dotNEON(query, v)
	}
}

func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = cosineNEON(query, v)
	}
}

func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchNEON(query, vectors, results)
}
