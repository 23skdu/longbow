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
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX2 {
		return L2SquaredFloat32(a, b)
	}

	var sum float32
	l2SquaredAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
		uintptr(unsafe.Pointer(&sum)), // #nosec G103
	)

	return sum, nil
}

// AVX2 optimized Euclidean distance for 128 dims
func euclidean128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return euclideanAVX2(a, b)
	}
	return euclidean128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// AVX2 optimized Euclidean distance for 384 dims
func euclidean384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return euclideanAVX2(a, b)
	}
	return euclidean384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// AVX2 optimized Euclidean distance for 768 dims.
func euclidean768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return euclideanAVX2(a, b)
	}
	return euclidean768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// AVX2 optimized Euclidean distance for 1024 dims.
func euclidean1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return euclideanAVX2(a, b)
	}
	return euclidean1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// AVX2 optimized Euclidean distance for 1536 dims.
func euclidean1536AVX2(a, b []float32) (float32, error) {
	return euclideanAVX2(a, b)
}

// AVX2 optimized Euclidean distance for 3072 dims.
func euclidean3072AVX2(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return euclideanAVX2(a, b)
	}
	return euclidean3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// AVX2 optimized dot product for specific dimensions
func dot128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return dotAVX2(a, b)
	}
	return dot128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func dot384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return dotAVX2(a, b)
	}
	return dot384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func dot768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return dotAVX2(a, b)
	}
	return dot768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func dot1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return dotAVX2(a, b)
	}
	return dot1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func dot1536AVX2(a, b []float32) (float32, error) {
	return dotAVX2(a, b)
}

func dot3072AVX2(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return dotAVX2(a, b)
	}
	return dot3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

// L2Squared AVX2 dimension-specialized wrappers (no sqrt)
func l2Squared128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func l2Squared384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func l2Squared768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func l2Squared1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
}

func l2Squared3072AVX2(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil // #nosec G103
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
			uintptr(unsafe.Pointer(&a[i])), // #nosec G103
			uintptr(unsafe.Pointer(&b[i])), // #nosec G103
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

	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))), nil
}

// AVX2 optimized dot product
func dotAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX2 {
		return dotGeneric(a, b)
	}

	var sum float32
	dotAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
		uintptr(unsafe.Pointer(&sum)), // #nosec G103
	)

	return sum, nil
}

// Bray-Curtis uses the generic Go baseline; the avo stub kernel is not yet implemented.
func brayCurtisAVX2(a, b []float32) (float32, error) {
	return BrayCurtisDistanceFloat32(a, b)
}

// AVX2 optimized Batch Euclidean distance
func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanVerticalBatchAVX2(query, vectors, results)
}

func euclideanSQ8BatchAVX2(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}

func euclideanF16BatchAVX2(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	qLen := len(query)
	qPtr := uintptr(unsafe.Pointer(&query[0])) // #nosec G103 -- SIMD kernel requires uintptr
	for i, v := range vectors {
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		results[i] = euclideanF16AVX2Kernel(qPtr, uintptr(unsafe.Pointer(&v[0])), qLen) // #nosec G103 -- SIMD kernel requires uintptr
	}
	return nil
}

// EuclideanDistanceVerticalBatch implementations
func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	// For now, use 4-way vertical batching if possible
	n := len(vectors)
	if n == 0 {
		return nil
	}
	qLen := len(query)
	if qLen == 0 {
		return nil
	}
	qPtr := uintptr(unsafe.Pointer(&query[0])) // #nosec G103
	i := 0

	for ; i <= n-4; i += 4 {
		if len(vectors[i]) == 0 || len(vectors[i+1]) == 0 || len(vectors[i+2]) == 0 || len(vectors[i+3]) == 0 {
			break
		}
		euclideanVertical4AVX2(
			uintptr(qPtr),
			uintptr(unsafe.Pointer(&vectors[i][0])), // #nosec G103
			uintptr(unsafe.Pointer(&vectors[i+1][0])), // #nosec G103
			uintptr(unsafe.Pointer(&vectors[i+2][0])), // #nosec G103
			uintptr(unsafe.Pointer(&vectors[i+3][0])), // #nosec G103
			qLen,
			uintptr(unsafe.Pointer(&results[i])), // #nosec G103
		)
	}

	// Remainder
	for ; i < n; i++ {
		if len(vectors[i]) == 0 {
			continue
		}
		d, err := euclideanAVX2(query, vectors[i])
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

// AVX2 optimized Batch Dot Product
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX2 {
		return dotBatchGeneric(query, vectors, results)
	}
	n := len(vectors)
	if n == 0 {
		return nil
	}
	qLen := len(query)
	if qLen == 0 {
		return nil
	}
	qPtr := uintptr(unsafe.Pointer(&query[0])) // #nosec G103 -- SIMD kernel requires uintptr
	i := 0

	for ; i <= n-4; i += 4 {
		if len(vectors[i]) == 0 || len(vectors[i+1]) == 0 || len(vectors[i+2]) == 0 || len(vectors[i+3]) == 0 {
			break
		}
		dotVertical4AVX2(
			uintptr(qPtr),
			uintptr(unsafe.Pointer(&vectors[i][0])),   // #nosec G103 -- SIMD kernel requires uintptr
			uintptr(unsafe.Pointer(&vectors[i+1][0])), // #nosec G103 -- SIMD kernel requires uintptr
			uintptr(unsafe.Pointer(&vectors[i+2][0])), // #nosec G103 -- SIMD kernel requires uintptr
			uintptr(unsafe.Pointer(&vectors[i+3][0])), // #nosec G103 -- SIMD kernel requires uintptr
			qLen,
			uintptr(unsafe.Pointer(&results[i])), // #nosec G103 -- SIMD kernel requires uintptr
		)
	}

	for ; i < n; i++ {
		if len(vectors[i]) == 0 {
			continue
		}
		d, err := dotAVX2(query, vectors[i])
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// AVX2 optimized Batch Cosine distance
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchUnrolled4x(query, vectors, results)
}

// Assembly function declarations

// Assembly function declarations are now in all_kernels_stubs_amd64.go

// All *ToFloat32* functions use generic fallbacks in this file.
// The corresponding avo stub kernels are not yet implemented.
func int8ToFloat32AVX2(src []int8, dst []float32) {
	int8ToFloat32Generic(src, dst)
}

func uint8ToFloat32AVX2(src []uint8, dst []float32) {
	uint8ToFloat32Generic(src, dst)
}

func int16ToFloat32AVX2(src []int16, dst []float32) {
	int16ToFloat32Generic(src, dst)
}

func uint16ToFloat32AVX2(src []uint16, dst []float32) {
	uint16ToFloat32Generic(src, dst)
}

func int32ToFloat32AVX2(src []int32, dst []float32) {
	int32ToFloat32Generic(src, dst)
}

func uint32ToFloat32AVX2(src []uint32, dst []float32) {
	uint32ToFloat32Generic(src, dst)
}

func float16ToFloat32AVX2(src []float16.Num, dst []float32) {
	float16ToFloat32Generic(src, dst)
}

func int8ToFloat32AVX512(src []int8, dst []float32) {
	int8ToFloat32Generic(src, dst)
}

func uint8ToFloat32AVX512(src []uint8, dst []float32) {
	uint8ToFloat32Generic(src, dst)
}

func int16ToFloat32AVX512(src []int16, dst []float32) {
	int16ToFloat32Generic(src, dst)
}

func uint16ToFloat32AVX512(src []uint16, dst []float32) {
	uint16ToFloat32Generic(src, dst)
}

func int32ToFloat32AVX512(src []int32, dst []float32) {
	int32ToFloat32Generic(src, dst)
}

func uint32ToFloat32AVX512(src []uint32, dst []float32) {
	uint32ToFloat32Generic(src, dst)
}

func float16ToFloat32AVX512(src []float16.Num, dst []float32) {
	float16ToFloat32Generic(src, dst)
}

func sigmoidAVX2(src, dst []float32) {
	sigmoidGeneric(src, dst)
}

func softmaxAVX2(src, dst []float32) {
	softmaxGeneric(src, dst)
}

func expAVX2(src, dst []float32) {
	expGeneric(src, dst)
}

func logAVX2(src, dst []float32) {
	logGeneric(src, dst)
}

func sumAVX2(src []float32) float32 {
	return sumGeneric(src)
}

func maxAVX2(src []float32) float32 {
	return maxGeneric(src)
}

func minAVX2(src []float32) float32 {
	return minGeneric(src)
}

func sigmoidAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	sigmoidAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src)) // #nosec G103
}

func softmaxAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	softmaxAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src)) // #nosec G103
}

func expAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	expAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src)) // #nosec G103
}

func logAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	logAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src)) // #nosec G103
}

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}

func matchInt32AVX2(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32Generic(src, val, op, dst)
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}

func matchFloat64AVX2(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64Generic(src, val, op, dst)
}

// FP16 AVX implementations
func euclideanF16AVX2(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanF16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotF16AVX2(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotF16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
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
	return 1.0 - dot/(float32(math.Sqrt(float64(normA)))*float32(math.Sqrt(float64(normB)))), nil
}

// F16 kernels are in stubs

// =============================================================================
// Float64 Implementations
// =============================================================================

func euclideanFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanFloat64AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotFloat64AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func l2SquaredFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX2 {
		return l2SquaredFloat64Unrolled4x(a, b)
	}
	return l2SquaredFloat64AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func cosineFloat64AVX2(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	if !features.HasAVX2 {
		return cosineFloat64Unrolled4x(a, b)
	}
	dot, normA, normB := cosineFloat64AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
	)
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))), nil
}

// =============================================================================
// PQ Kernel Implementation
// =============================================================================

// adcBatchAVX2Kernel is in stubs

// =============================================================================
// Int8 Implementations
// =============================================================================

func euclideanInt8AVX512Kernel(a, b uintptr, n int) float32

func euclideanInt8AVX2(a, b []int8) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanInt8AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

// =============================================================================
// Int16 Implementations — AVX2 native (VPMOVSXWD + VPMULLD / VPMADDWD)
// =============================================================================

//go:noescape
func euclideanInt16AVX2Kernel(a, b uintptr, n int) float32

//go:noescape
func euclideanUint16AVX2Kernel(a, b uintptr, n int) float32

//go:noescape
func dotInt16AVX2Kernel(a, b uintptr, n int) float32

//go:noescape
func dotUint16AVX2Kernel(a, b uintptr, n int) float32

func euclideanInt16AVX2(a, b []int16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanInt16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func euclideanUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanUint16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotInt16AVX2(a, b []int16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotInt16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotUint16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotInt8AVX2(a, b []int8) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotInt8AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func euclideanUint8AVX2(a, b []uint8) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanUint8AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotUint8AVX2(a, b []uint8) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotUint8AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotInt4AVX512(a, b []byte) (float32, error) {
	return dotInt4Generic(a, b)
}

func dotInt4AVX2(a, b []byte) (float32, error) {
	return dotInt4Generic(a, b)
}

func dotInt2AVX512(a, b []byte) (float32, error) {
	return dotInt2Generic(a, b)
}

func dotInt2AVX2(a, b []byte) (float32, error) {
	return dotInt2Generic(a, b)
}

func sinAVX2(src, dst []float32) {
	sinFloat32Generic(src, dst)
}

func cosAVX2(src, dst []float32) {
	cosFloat32Generic(src, dst)
}

func sincosAVX2(src, sinDst, cosDst []float32) {
	sincosFloat32Generic(src, sinDst, cosDst)
}

func sqrtAVX2(src, dst []float32) {
	n := len(src)
	if n != len(dst) || n == 0 {
		sqrtFloat32Generic(src, dst)
		return
	}
	sqrtFloat32AVX2Kernel(
		unsafe.Pointer(&src[0]), // #nosec G103
		unsafe.Pointer(&dst[0]), // #nosec G103
		n,
	)
}

func atan2AVX2(y, x, dst []float32) {
	atan2Float32Generic(y, x, dst)
}

func argMaxAVX2(src []float32) int {
	n := len(src)
	if n == 0 {
		return -1
	}
	if n < 8 {
		return argMaxGeneric(src)
	}
	simdLen := (n / 8) * 8
	val, idx := argMaxAVX2Kernel(uintptr(unsafe.Pointer(&src[0])), simdLen) // #nosec G103
	if n > simdLen {
		for i := simdLen; i < n; i++ {
			if src[i] > val {
				val = src[i]
				idx = i
			}
		}
	}
	return idx
}

func argMinAVX2(src []float32) int {
	n := len(src)
	if n == 0 {
		return -1
	}
	if n < 8 {
		return argMinGeneric(src)
	}
	simdLen := (n / 8) * 8
	val, idx := argMinAVX2Kernel(uintptr(unsafe.Pointer(&src[0])), simdLen) // #nosec G103
	if n > simdLen {
		for i := simdLen; i < n; i++ {
			if src[i] < val {
				val = src[i]
				idx = i
			}
		}
	}
	return idx
}

func matMulAVX2(a, b []float32, m, n, k int, dst []float32) {
	if len(a) < m*k || len(b) < k*n || len(dst) < m*n {
		matMulGeneric(a, b, m, n, k, dst)
		return
	}
	// Our SIMD kernel assumes n is a multiple of 8
	if n%8 != 0 {
		matMulGeneric(a, b, m, n, k, dst)
		return
	}

	matMulAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		uintptr(unsafe.Pointer(&dst[0])), // #nosec G103
		m, n, k,
	)
}

//go:noescape
func pause()

var _ = func() {
	if false {
		pause()
		_, _ = dotInt4AVX2(nil, nil)
		_, _ = dotInt2AVX2(nil, nil)
		sinAVX2(nil, nil)
		cosAVX2(nil, nil)
		atan2AVX2(nil, nil, nil)
	}
}

func l2SquaredBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		if v == nil || len(v) == 0 {
			continue
		}
		d, err := l2SquaredAVX2(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}
