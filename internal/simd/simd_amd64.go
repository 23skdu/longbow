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
	l2SquaredAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a),
		uintptr(unsafe.Pointer(&sum)),
	)

	return sum, nil
}

// AVX2 optimized Euclidean distance for 128 dims
func euclidean128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return euclideanAVX2(a, b)
	}
	return euclidean128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// AVX2 optimized Euclidean distance for 384 dims
func euclidean384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return euclideanAVX2(a, b)
	}
	return euclidean384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// AVX2 optimized Euclidean distance for 768 dims.
func euclidean768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return euclideanAVX2(a, b)
	}
	return euclidean768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// AVX2 optimized Euclidean distance for 1024 dims.
func euclidean1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return euclideanAVX2(a, b)
	}
	return euclidean1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
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
	return euclidean3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// AVX2 optimized dot product for specific dimensions
func dot128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return dotAVX2(a, b)
	}
	return dot128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return dotAVX2(a, b)
	}
	return dot384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return dotAVX2(a, b)
	}
	return dot768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return dotAVX2(a, b)
	}
	return dot1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot1536AVX2(a, b []float32) (float32, error) {
	return dotAVX2(a, b)
}

func dot3072AVX2(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return dotAVX2(a, b)
	}
	return dot3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// L2Squared AVX2 dimension-specialized wrappers (no sqrt)
func l2Squared128AVX2(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared128AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared384AVX2(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared384AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared768AVX2(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared768AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared1024AVX2(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared1024AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared3072AVX2(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared3072AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
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
			uintptr(unsafe.Pointer(&a[i])),
			uintptr(unsafe.Pointer(&b[i])),
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
	if !features.HasAVX2 {
		return dotGeneric(a, b)
	}

	var sum float32
	dotAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a),
		uintptr(unsafe.Pointer(&sum)),
	)

	return sum, nil
}

// AVX2 optimized Bray-Curtis distance
func brayCurtisAVX2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX2 {
		return BrayCurtisDistanceFloat32(a, b)
	}

	return brayCurtisAVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a),
	), nil
}

// AVX2 optimized Batch Euclidean distance
func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func euclideanSQ8BatchAVX2(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}

func euclideanF16BatchAVX2(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchGeneric(query, vectors, results)
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
	if len(query) == 0 {
		return nil
	}
	qPtr := uintptr(unsafe.Pointer(&query[0]))
	i := 0

	for ; i <= n-4; i += 4 {
		euclideanVertical4AVX2(
			uintptr(qPtr),
			uintptr(unsafe.Pointer(&vectors[i][0])),
			uintptr(unsafe.Pointer(&vectors[i+1][0])),
			uintptr(unsafe.Pointer(&vectors[i+2][0])),
			uintptr(unsafe.Pointer(&vectors[i+3][0])),
			qLen,
			uintptr(unsafe.Pointer(&results[i])),
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
	return adcBatchGeneric(table, flatCodes, m, results)
}

// AVX2 optimized Batch Dot Product
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}

// AVX2 optimized Batch Cosine distance
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX2 {
		return cosineBatchGeneric(query, vectors, results)
	}
	n := len(vectors)
	if n == 0 {
		return nil
	}
	qLen := len(query)
	if qLen == 0 {
		return nil
	}
	if len(query) == 0 {
		return nil
	}
	qPtr := uintptr(unsafe.Pointer(&query[0]))
	i := 0

	for ; i <= n-4; i += 4 {
		cosineVertical4AVX2(
			uintptr(qPtr),
			uintptr(unsafe.Pointer(&vectors[i][0])),
			uintptr(unsafe.Pointer(&vectors[i+1][0])),
			uintptr(unsafe.Pointer(&vectors[i+2][0])),
			uintptr(unsafe.Pointer(&vectors[i+3][0])),
			qLen,
			uintptr(unsafe.Pointer(&results[i])),
		)
	}

	for ; i < n; i++ {
		d, err := cosineAVX2(query, vectors[i])
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// Assembly function declarations

// Assembly function declarations are now in all_kernels_stubs_amd64.go

func int8ToFloat32AVX2(src []int8, dst []float32) {
	if len(src) == 0 {
		return
	}
	int8ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint8ToFloat32AVX2(src []uint8, dst []float32) {
	if len(src) == 0 {
		return
	}
	uint8ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func int16ToFloat32AVX2(src []int16, dst []float32) {
	if len(src) == 0 {
		return
	}
	int16ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint16ToFloat32AVX2(src []uint16, dst []float32) {
	if len(src) == 0 {
		return
	}
	uint16ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func int32ToFloat32AVX2(src []int32, dst []float32) {
	if len(src) == 0 {
		return
	}
	int32ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint32ToFloat32AVX2(src []uint32, dst []float32) {
	uint32ToFloat32Generic(src, dst) // uint32 -> f32 is tricky on AVX2, fallback to generic
}

func float16ToFloat32AVX2(src []float16.Num, dst []float32) {
	if len(src) == 0 {
		return
	}
	// We already have VCVTPH2PS in AVX2
	float16ToFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func int8ToFloat32AVX512(src []int8, dst []float32) {
	if len(src) == 0 {
		return
	}
	int8ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint8ToFloat32AVX512(src []uint8, dst []float32) {
	if len(src) == 0 {
		return
	}
	uint8ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func int16ToFloat32AVX512(src []int16, dst []float32) {
	if len(src) == 0 {
		return
	}
	int16ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint16ToFloat32AVX512(src []uint16, dst []float32) {
	if len(src) == 0 {
		return
	}
	uint16ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func int32ToFloat32AVX512(src []int32, dst []float32) {
	if len(src) == 0 {
		return
	}
	int32ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func uint32ToFloat32AVX512(src []uint32, dst []float32) {
	if len(src) == 0 {
		return
	}
	uint32ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func float16ToFloat32AVX512(src []float16.Num, dst []float32) {
	if len(src) == 0 {
		return
	}
	float16ToFloat32AVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
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
	sigmoidAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func softmaxAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	softmaxAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func expAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	expAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

func logAVX512(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	logAVX512Kernel(uintptr(unsafe.Pointer(&src[0])), uintptr(unsafe.Pointer(&dst[0])), len(src))
}

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
	matchInt64AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), val, int(op), uintptr(unsafe.Pointer(&dst[0])), len(src))
	return nil
}

func matchInt32AVX2(src []int32, val int32, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	if !features.HasAVX2 {
		return matchInt32Generic(src, val, op, dst)
	}
	if len(src) == 0 {
		return nil
	}
	matchInt32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), int64(val), int(op), uintptr(unsafe.Pointer(&dst[0])), len(src))
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
	matchFloat32AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), int64(math.Float32bits(val)), int(op), uintptr(unsafe.Pointer(&dst[0])), len(src))
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
	matchFloat64AVX2Kernel(uintptr(unsafe.Pointer(&src[0])), int64(math.Float64bits(val)), int(op), uintptr(unsafe.Pointer(&dst[0])), len(src))
	return nil
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
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
	return 1.0 - (dot/float32(math.Sqrt(float64(normA))))*float32(math.Sqrt(float64(normB))), nil
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a)), nil
}

func l2SquaredFloat64AVX2(a, b []float64) (float32, error) {
	val, err := euclideanFloat64AVX2(a, b)
	if err != nil {
		return 0, err
	}
	return val * val, nil
}

// =============================================================================
// PQ Kernel Implementation
// =============================================================================

// adcBatchAVX2Kernel is in stubs

// =============================================================================
// Int8 Implementations
// =============================================================================

func euclideanInt8AVX2(a, b []int8) (float32, error) {
	return euclideanInt8Unrolled4x(a, b)
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a)), nil
}

func euclideanUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanUint16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a)), nil
}

func dotInt16AVX2(a, b []int16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotInt16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a)), nil
}

func dotUint16AVX2(a, b []uint16) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return dotUint16AVX2Kernel(
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		len(a)), nil
}

func dotInt4AVX512(a, b []byte) (float32, error) {
	n := len(a)
	if n == 0 {
		return 0, nil
	}
	var sum float32
	if n >= 64 {
		simdLen := (n / 64) * 64
		sum = dotInt4AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])), simdLen)
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
	if n >= 32 {
		simdLen := (n / 32) * 32
		sum = dotInt4AVX2Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0])), simdLen)
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

func atan2AVX2(y, x, dst []float32) {
	atan2Float32Generic(y, x, dst)
}

func argMaxAVX2(src []float32) int {
	if len(src) < 8 {
		return argMaxGeneric(src)
	}
	_, idx := argMaxAVX2Kernel(uintptr(unsafe.Pointer(&src[0])), len(src))
	return idx
}

func argMinAVX2(src []float32) int {
	if len(src) < 8 {
		return argMinGeneric(src)
	}
	_, idx := argMinAVX2Kernel(uintptr(unsafe.Pointer(&src[0])), len(src))
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
		uintptr(unsafe.Pointer(&a[0])),
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(unsafe.Pointer(&dst[0])),
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
		if v == nil {
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

