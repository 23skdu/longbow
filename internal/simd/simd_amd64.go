//go:build amd64

package simd

import (
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// AVX2 optimized Euclidean distance
// Processes 8 float32s at a time (256-bit registers)
func euclideanAVX2(a, b []float32) float32 {
	if !features.HasAVX2 {
		return euclideanGeneric(a, b)
	}

	var sum float32
	n := len(a)
	i := 0

	// Process 8 elements at a time (AVX2: 256-bit = 8 x float32)
	for ; i <= n-8; i += 8 {
		sum += euclidean8AVX2(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
	}

	// Handle remaining elements
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}

	return float32(math.Sqrt(float64(sum)))
}

// AVX512 optimized Euclidean distance
func euclideanAVX512(a, b []float32) float32 {
	if !features.HasAVX512 {
		return euclideanAVX2(a, b)
	}
	if len(a) == 0 {
		return 0
	}
	sum := l2SquaredAVX512(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
	return float32(math.Sqrt(float64(sum)))
}

// AVX512 optimized Euclidean distance for 384 dims
func euclidean384AVX512(a, b []float32) float32 {
	return float32(math.Sqrt(float64(euclidean384AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])))))
}

// AVX2 optimized Cosine distance
func cosineAVX2(a, b []float32) float32 {
	if !features.HasAVX2 {
		return cosineGeneric(a, b)
	}

	var dot, normA, normB float32
	n := len(a)
	i := 0

	// Process 8 elements at a time
	for ; i <= n-8; i += 8 {
		d, na, nb := cosine8AVX2(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
		dot += d
		normA += na
		normB += nb
	}

	// Handle remaining elements
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

// AVX512 optimized Cosine distance
func cosineAVX512(a, b []float32) float32 {
	if !features.HasAVX512 {
		return cosineAVX2(a, b)
	}
	if len(a) == 0 {
		return 1.0
	}
	dot, normA, normB := cosineDotAVX512(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
	if normA == 0 || normB == 0 {
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

// AVX2 optimized dot product
func dotAVX2(a, b []float32) float32 {
	if !features.HasAVX2 {
		return dotGeneric(a, b)
	}

	var sum float32
	n := len(a)
	i := 0

	for ; i <= n-8; i += 8 {
		sum += dot8AVX2(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
	}

	for ; i < n; i++ {
		sum += a[i] * b[i]
	}

	return sum
}

// AVX512 optimized dot product
func dotAVX512(a, b []float32) float32 {
	if !features.HasAVX512 {
		return dotAVX2(a, b)
	}

	// Legacy loop implementation for Dot (didn't optimize yet)
	var sum float32
	n := len(a)
	i := 0

	for ; i <= n-16; i += 16 {
		sum += dot16AVX512(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
	}

	for ; i <= n-8; i += 8 {
		sum += dot8AVX2(
			unsafe.Pointer(&a[i]),
			unsafe.Pointer(&b[i]),
		)
	}

	for ; i < n; i++ {
		sum += a[i] * b[i]
	}

	return sum
}

// AVX512 optimized dot product for 384 dims
func dot384AVX512(a, b []float32) float32 {
	return dot384AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]))
}

// AVX2 optimized Batch Euclidean distance
func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX2 {
		euclideanBatchGeneric(query, vectors, results)
		return
	}

	for idx, v := range vectors {
		if len(query) != len(v) {
			panic("simd: vector length mismatch")
		}

		var sum float32
		n := len(query)
		i := 0

		for ; i <= n-8; i += 8 {
			sum += euclidean8AVX2(
				unsafe.Pointer(&query[i]),
				unsafe.Pointer(&v[i]),
			)
		}

		for ; i < n; i++ {
			d := query[i] - v[i]
			sum += d * d
		}
		results[idx] = float32(math.Sqrt(float64(sum)))
	}
}

// AVX512 optimized Batch Euclidean distance
func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX512 {
		euclideanBatchAVX2(query, vectors, results)
		return
	}
	if len(query) == 0 {
		return
	}
	queryPtr := unsafe.Pointer(&query[0])
	qLen := len(query)

	for idx, v := range vectors {
		if len(v) != qLen {
			panic("simd: vector length mismatch")
		}
		if len(v) > 0 {
			sum := l2SquaredAVX512(queryPtr, unsafe.Pointer(&v[0]), qLen)
			results[idx] = float32(math.Sqrt(float64(sum)))
		} else {
			results[idx] = 0
		}
	}
}

// EuclideanDistanceVerticalBatch implementations
func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	// For now, use 4-way vertical batching if possible
	n := len(vectors)
	i := 0
	qPtr := unsafe.Pointer(&query[0])
	qLen := len(query)

	for ; i <= n-4; i += 4 {
		euclideanVertical4AVX2(
			qPtr,
			unsafe.Pointer(&vectors[i][0]),
			unsafe.Pointer(&vectors[i+1][0]),
			unsafe.Pointer(&vectors[i+2][0]),
			unsafe.Pointer(&vectors[i+3][0]),
			qLen,
			unsafe.Pointer(&results[i]),
		)
	}

	// Remainder
	for ; i < n; i++ {
		results[i] = euclideanAVX2(query, vectors[i])
	}
}

func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	n := len(vectors)
	i := 0
	qPtr := unsafe.Pointer(&query[0])
	qLen := len(query)

	for ; i <= n-4; i += 4 {
		euclideanVertical4AVX512(
			qPtr,
			unsafe.Pointer(&vectors[i][0]),
			unsafe.Pointer(&vectors[i+1][0]),
			unsafe.Pointer(&vectors[i+2][0]),
			unsafe.Pointer(&vectors[i+3][0]),
			qLen,
			unsafe.Pointer(&results[i]),
		)
	}

	// Remainder
	for ; i < n; i++ {
		results[i] = euclideanAVX512(query, vectors[i])
	}
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) {
	if len(results) == 0 {
		return
	}
	adcBatchAVX2Kernel(unsafe.Pointer(&table[0]), unsafe.Pointer(&flatCodes[0]), m, unsafe.Pointer(&results[0]), len(results))
}

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) {
	if !features.HasAVX512 {
		adcBatchAVX2(table, flatCodes, m, results)
		return
	}
	if len(results) == 0 {
		return
	}
	adcBatchAVX512Kernel(unsafe.Pointer(&table[0]), unsafe.Pointer(&flatCodes[0]), m, unsafe.Pointer(&results[0]), len(results))
}

// AVX2 optimized Batch Dot Product
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX2 {
		dotBatchGeneric(query, vectors, results)
		return
	}
	for idx, v := range vectors {
		results[idx] = dotAVX2(query, v)
	}
}

// AVX512 optimized Batch Dot Product
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX512 {
		dotBatchAVX2(query, vectors, results)
		return
	}
	for idx, v := range vectors {
		results[idx] = dotAVX512(query, v)
	}
}

// AVX2 optimized Batch Cosine distance
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX2 {
		cosineBatchGeneric(query, vectors, results)
		return
	}
	for idx, v := range vectors {
		results[idx] = cosineAVX2(query, v)
	}
}

// AVX512 optimized Batch Cosine distance
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	if !features.HasAVX512 {
		cosineBatchAVX2(query, vectors, results)
		return
	}
	if len(query) == 0 {
		for i := range results {
			results[i] = 1.0
		}
		return
	}
	queryPtr := unsafe.Pointer(&query[0])
	qLen := len(query)

	for idx, v := range vectors {
		if len(v) != qLen {
			panic("simd: vector length mismatch")
		}
		if len(v) > 0 {
			dot, normA, normB := cosineDotAVX512(queryPtr, unsafe.Pointer(&v[0]), qLen)
			if normA == 0 || normB == 0 {
				results[idx] = 1.0
			} else {
				results[idx] = 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
			}
		} else {
			results[idx] = 1.0
		}
	}
}

// NEON stubs for AMD64
func euclideanNEON(a, b []float32) float32 {
	return euclideanGeneric(a, b)
}

func cosineNEON(a, b []float32) float32 {
	return cosineGeneric(a, b)
}

func dotNEON(a, b []float32) float32 {
	return dotGeneric(a, b)
}

func euclidean384NEON(a, b []float32) float32 {
	return euclideanGeneric(a, b)
}

func dot384NEON(a, b []float32) float32 {
	return dotGeneric(a, b)
}

func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}

func dotBatchNEON(query []float32, vectors [][]float32, results []float32) {
	dotBatchGeneric(query, vectors, results)
}

func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) {
	cosineBatchGeneric(query, vectors, results)
}

func euclidean128NEON(a, b []float32) float32 {
	return euclidean128Unrolled4x(a, b)
}

func dot128NEON(a, b []float32) float32 {
	return dot128Unrolled4x(a, b)
}

func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}

func adcBatchNEON(table []float32, flatCodes []byte, m int, results []float32) {
	adcBatchGeneric(table, flatCodes, m, results)
}

func euclideanF16NEON(a, b []float16.Num) float32 {
	return euclideanF16Unrolled4x(a, b)
}

func dotF16NEON(a, b []float16.Num) float32 {
	return dotF16Unrolled4x(a, b)
}

func cosineF16NEON(a, b []float16.Num) float32 {
	return cosineF16Unrolled4x(a, b)
}

// Assembly function declarations
// New full-loop kernels
//
//go:noescape
func l2SquaredAVX512(a, b unsafe.Pointer, n int) float32

//go:noescape
func cosineDotAVX512(a, b unsafe.Pointer, n int) (dot, normA, normB float32)

//go:noescape
func euclideanVertical4AVX2(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)

//go:noescape
func euclideanVertical4AVX512(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)

// Existing partial-block definitions (kept for compatibility/Dot/legacy)
//
//go:noescape
func euclidean8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func euclidean16AVX512(a, b unsafe.Pointer) float32

//go:noescape
func cosine8AVX2(a, b unsafe.Pointer) (dot, normA, normB float32)

//go:noescape
func cosine16AVX512(a, b unsafe.Pointer) (dot, normA, normB float32)

//go:noescape
func dot8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func dot16AVX512(a, b unsafe.Pointer) float32

//go:noescape
func prefetchNTA(p unsafe.Pointer)

// =============================================================================
// Comparison Wrapper Functions
// =============================================================================

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) {
	if !features.HasAVX2 {
		matchInt64Generic(src, val, op, dst)
		return
	}
	if len(src) == 0 {
		return
	}
	matchInt64AVX2Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) {
	if !features.HasAVX2 {
		matchFloat32Generic(src, val, op, dst)
		return
	}
	if len(src) == 0 {
		return
	}
	matchFloat32AVX2Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) {
	if !features.HasAVX512 {
		matchInt64AVX2(src, val, op, dst)
		return
	}
	if len(src) == 0 {
		return
	}
	matchInt64AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) {
	if !features.HasAVX512 {
		matchFloat32AVX2(src, val, op, dst)
		return
	}
	if len(src) == 0 {
		return
	}
	matchFloat32AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
}

// NEON stubs for cross-platform link satisfaction (if referenced by simd.go)
func matchInt64NEON(src []int64, val int64, op CompareOp, dst []byte) {
	matchInt64Generic(src, val, op, dst)
}

func matchFloat32NEON(src []float32, val float32, op CompareOp, dst []byte) {
	matchFloat32Generic(src, val, op, dst)
}

// Kernel Declarations

//go:noescape
func matchInt64AVX2Kernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat32AVX2Kernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchInt64AVX512Kernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat32AVX512Kernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)

//go:noescape
func adcBatchAVX2Kernel(table unsafe.Pointer, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)

//go:noescape
func adcBatchAVX512Kernel(table unsafe.Pointer, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)

//go:noescape
func euclidean384AVX512Kernel(a, b unsafe.Pointer) float32

//go:noescape
func dot384AVX512Kernel(a, b unsafe.Pointer) float32

// FP16 AVX implementations
func euclideanF16AVX2(a, b []float16.Num) float32 {
	if !features.HasAVX2 {
		return euclideanF16Unrolled4x(a, b)
	}
	if len(a) == 0 {
		return 0
	}
	return euclideanF16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}

func euclideanF16AVX512(a, b []float16.Num) float32 {
	if !features.HasAVX512 {
		return euclideanF16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0
	}
	return euclideanF16AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}

func dotF16AVX2(a, b []float16.Num) float32 {
	if !features.HasAVX2 {
		return dotF16Unrolled4x(a, b)
	}
	if len(a) == 0 {
		return 0
	}
	return dotF16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}

func dotF16AVX512(a, b []float16.Num) float32 {
	if !features.HasAVX512 {
		return dotF16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0
	}
	return dotF16AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}

func cosineF16AVX2(a, b []float16.Num) float32 {
	dot := dotF16AVX2(a, b)
	normA := dotF16AVX2(a, a)
	normB := dotF16AVX2(b, b)
	if normA <= 0 || normB <= 0 {
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

func cosineF16AVX512(a, b []float16.Num) float32 {
	dot := dotF16AVX512(a, b)
	normA := dotF16AVX512(a, a)
	normB := dotF16AVX512(b, b)
	if normA <= 0 || normB <= 0 {
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

//go:noescape
func euclideanF16AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func euclideanF16AVX512Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotF16AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotF16AVX512Kernel(a, b unsafe.Pointer, n int) float32
