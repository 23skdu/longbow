//go:build arm64

package simd

import (
	"math"
	"unsafe"
)

// ARM64 NEON implementations
// For now using optimized Go code; can be replaced with assembly later

func euclideanNEON(a, b []float32) float32 {
	if !features.HasNEON {
		return euclideanGeneric(a, b)
	}

	var sum float32
	n := len(a)
	i := 0

	// Process 4 elements at a time (NEON: 128-bit = 4 x float32)
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum += d0*d0 + d1*d1 + d2*d2 + d3*d3
	}

	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}

	return float32(math.Sqrt(float64(sum)))
}

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

func dotNEON(a, b []float32) float32 {
	if !features.HasNEON {
		return dotGeneric(a, b)
	}

	var sum float32
	n := len(a)
	i := 0

	for ; i <= n-4; i += 4 {
		sum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
	}

	for ; i < n; i++ {
		sum += a[i] * b[i]
	}

	return sum
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
