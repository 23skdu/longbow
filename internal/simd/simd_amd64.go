//go:build amd64

package simd

import (
	"errors"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// AVX2 optimized Euclidean distance
// Processes 8 float32s at a time (256-bit registers)
func euclideanAVX2(a, b []float32) (float32, error) {
	sum, err := l2SquaredAVX2(a, b)
	return float32(math.Sqrt(float64(sum))), err
}

// AVX2 optimized L2 Squared distance (no Sqrt)
func l2SquaredAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return L2SquaredFloat32(a, b)
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

	return sum, nil
}

// AVX2 optimized Euclidean distance for 384 dims - uses generic AVX2 SIMD kernel
func euclidean384AVX2(a, b []float32) (float32, error) {
	return euclideanAVX2(a, b)
}

// AVX2 optimized Euclidean distance for 768 dims.
// Uses scalar unrolled4x (faster than 8-float AVX2 loop for high dims on AVX2-only).
func euclidean768AVX2(a, b []float32) (float32, error) {
	return euclidean768Unrolled4x(a, b)
}

// AVX2 optimized Euclidean distance for 1536 dims.
// Uses scalar unrolled4x (faster than 8-float AVX2 loop for high dims on AVX2-only).
func euclidean1536AVX2(a, b []float32) (float32, error) {
	return euclidean1536Unrolled4x(a, b)
}

// AVX2 optimized Cosine distance
func cosineAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
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

	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

// AVX2 optimized dot product
func dotAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
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

	return sum, nil
}

// AVX2 optimized Batch Euclidean distance
func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX2 {
		return euclideanBatchGeneric(query, vectors, results)
	}

	for idx, v := range vectors {
		if v == nil || len(query) != len(v) {
			results[idx] = math.MaxFloat32
			continue
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
	return nil
}


func euclideanSQ8BatchAVX2(query []byte, vectors [][]byte, results []float32) error {
	if len(query) == 0 {
		return nil
	}
	qPtr := unsafe.Pointer(&query[0])
	qLen := len(query)
	for i, v := range vectors {
		if v == nil {
			continue
		}
		if len(v) != qLen {
			return errors.New("simd: vector length mismatch")
		}
		results[i] = float32(euclideanSQ8AVX2Kernel(qPtr, unsafe.Pointer(&v[0]), qLen))
	}
	return nil
}

func euclideanF16BatchAVX2(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	if len(query) == 0 {
		return nil
	}
	qPtr := unsafe.Pointer(&query[0])
	qLen := len(query)
	for i, v := range vectors {
		if v == nil {
			continue
		}
		if len(v) != qLen {
			return errors.New("simd: vector length mismatch")
		}
		results[i] = euclideanF16AVX2Kernel(qPtr, unsafe.Pointer(&v[0]), qLen)
	}
	return nil
}

// EuclideanDistanceVerticalBatch implementations
func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
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
		d, err := euclideanAVX2(query, vectors[i])
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) error {
	if len(results) == 0 {
		return nil
	}
	adcBatchAVX2Kernel(unsafe.Pointer(&table[0]), unsafe.Pointer(&flatCodes[0]), m, unsafe.Pointer(&results[0]), len(results))
	return nil
}

// AVX2 optimized Batch Dot Product
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX2 {
		return dotBatchGeneric(query, vectors, results)
	}
	for idx, v := range vectors {
		d, err := dotAVX2(query, v)
		if err != nil {
			return err
		}
		results[idx] = d
	}
	return nil
}

// AVX2 optimized Batch Cosine distance
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX2 {
		return cosineBatchGeneric(query, vectors, results)
	}
	for idx, v := range vectors {
		d, err := cosineAVX2(query, v)
		if err != nil {
			return err
		}
		results[idx] = d
	}
	return nil
}

// Assembly function declarations

//go:noescape
func euclidean8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func euclideanVertical4AVX2(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)

//go:noescape
func prefetchNTA(p unsafe.Pointer)

//go:noescape
func cosine8AVX2(a, b unsafe.Pointer) (dot, normA, normB float32)

//go:noescape
func dot8AVX2(a, b unsafe.Pointer) float32

//go:noescape
func matchInt64AVX2Kernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat32AVX2Kernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)

//go:noescape
func matchFloat64AVX2Kernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int)

//go:noescape
func euclideanFloat64AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func euclideanInt8AVX2Kernel(a, b unsafe.Pointer, n int) float32
func euclideanInt8Unrolled4xAVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func euclideanInt16AVX2Kernel(a, b unsafe.Pointer, n int) float32
func euclideanUint16AVX2Kernel(a, b unsafe.Pointer, n int) float32
func dotInt16AVX2Kernel(a, b unsafe.Pointer, n int) float32
func dotUint16AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotFloat64AVX2Kernel(a, b unsafe.Pointer, n int) float32

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return matchInt64Generic(src, val, op, dst)
	}
	if len(src) == 0 {
		return nil
	}
	matchInt64AVX2Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return matchFloat32Generic(src, val, op, dst)
	}
	if len(src) == 0 {
		return nil
	}
	matchFloat32AVX2Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

func matchFloat64AVX2(src []float64, val float64, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return matchFloat64Generic(src, val, op, dst)
	}
	if len(src) == 0 {
		return nil
	}
	matchFloat64AVX2Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

// FP16 AVX implementations
func euclideanF16AVX2(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return euclideanF16Unrolled4x(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < 8 {
		return euclideanF16Unrolled4x(a, b)
	}
	return euclideanF16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotF16AVX2(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return dotF16Unrolled4x(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < 8 {
		return dotF16Unrolled4x(a, b)
	}
	return dotF16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func cosineF16AVX2(a, b []float16.Num) (float32, error) {
	dot, err := dotF16AVX2(a, b)
	if err != nil {
		return 0, err
	}
	normA, err := dotF16AVX2(a, a)
	if err != nil {
		return 0, err
	}
	normB, err := dotF16AVX2(b, b)
	if err != nil {
		return 0, err
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

//go:noescape
func euclideanF16AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotF16AVX2Kernel(a, b unsafe.Pointer, n int) float32

// =============================================================================
// Float64 Implementations
// =============================================================================

func euclideanFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return 0, errors.New("avx2 not supported")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return float32(euclideanFloat64AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))), nil
}

func dotFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return dotFloat64Unrolled4x(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotFloat64AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

// =============================================================================
// PQ Kernel Implementation
// =============================================================================

//go:noescape
func adcBatchAVX2Kernel(table, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)

// =============================================================================
// Int8 Implementations
// =============================================================================

func euclideanInt8AVX2(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum)), nil
}

// =============================================================================
// Int16 Implementations
// =============================================================================

func euclideanInt16AVX2(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanInt16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func euclideanUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanUint16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotInt16AVX2(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotInt16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotUint16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotInt4AVX512(a, b []byte) (float32, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	var sum float32
	if n >= 32 {
		simdLen := (n / 32) * 32
		sum = dotInt4AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), simdLen)
		a = a[simdLen:]
		b = b[simdLen:]
	}
	if len(a) > 0 {
		tailSum, _ := dotInt4Generic(a, b)
		sum += tailSum
	}
	return sum, nil
}

func dotInt4AVX2(a, b []byte) (float32, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	var sum float32
	if n >= 16 {
		simdLen := (n / 16) * 16
		sum = dotInt4AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), simdLen)
		a = a[simdLen:]
		b = b[simdLen:]
	}
	if len(a) > 0 {
		tailSum, _ := dotInt4Generic(a, b)
		sum += tailSum
	}
	return sum, nil
}

func dotInt2AVX512(a, b []byte) (float32, error) {
	// Fallback to AVX2 or generic if not specialized
	return dotInt4AVX512(a, b) // Placeholder, should be specialized
}

func dotInt2AVX2(a, b []byte) (float32, error) {
	return dotInt4AVX2(a, b) // Placeholder
}

//go:noescape
func dotInt4AVX512Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotInt4AVX2Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotInt2AVX512Kernel(a, b unsafe.Pointer, n int) float32

//go:noescape
func dotInt2AVX2Kernel(a, b unsafe.Pointer, n int) float32
