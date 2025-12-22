//go:build arm64

package simd

import (
	"math"
	"unsafe"
)

// ARM64 NEON implementations
// Defined in simd_arm64.s

func euclideanNEON(a, b []float32) float32
func dotNEON(a, b []float32) float32

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

// AVX stubs for ARM64 (not available)
func euclideanAVX2(a, b []float32) float32   { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) float32 { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) float32      { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) float32    { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) float32         { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) float32       { return dotGeneric(a, b) }
func prefetchNTA(p unsafe.Pointer)           {}

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

func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}
func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	dotBatchGeneric(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	dotBatchGeneric(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	cosineBatchGeneric(query, vectors, results)
}
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	cosineBatchGeneric(query, vectors, results)
}
