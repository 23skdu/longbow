//go:build amd64 && avx512
// +build amd64
// +build avx512

package simd

import (
	"errors"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// =============================================================================
// Distance Functions
// =============================================================================

// AVX512 optimized Euclidean distance
func euclideanAVX512(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return euclideanAVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	sum := l2SquaredAVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
	return float32(math.Sqrt(float64(sum))), nil
}

// AVX512 optimized L2 Squared distance (no Sqrt)
func l2SquaredAVX512(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return l2SquaredAVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return l2SquaredAVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

// AVX512 optimized dot product
func dotAVX512(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return dotAVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotAVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

// AVX512 optimized Cosine distance
func cosineAVX512(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return cosineAVX2(a, b)
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	dot, normA, normB := cosineDotAVX512(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

// =============================================================================
// Batch Functions
// =============================================================================

func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX512 {
		return euclideanBatchAVX2(query, vectors, results)
	}
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}

	n := len(vectors)
	qLen := len(query)
	qPtr := unsafe.Pointer(&query[0])
	i := 0

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

	for ; i < n; i++ {
		v := vectors[i]
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		sum := l2SquaredAVX512Kernel(qPtr, unsafe.Pointer(&v[0]), qLen)
		results[i] = float32(math.Sqrt(float64(sum)))
	}
	return nil
}

func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX512 {
		return dotBatchAVX2(query, vectors, results)
	}
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}

	n := len(vectors)
	qLen := len(query)
	qPtr := unsafe.Pointer(&query[0])
	i := 0

	for ; i <= n-4; i += 4 {
		dotVertical4AVX512(
			qPtr,
			unsafe.Pointer(&vectors[i][0]),
			unsafe.Pointer(&vectors[i+1][0]),
			unsafe.Pointer(&vectors[i+2][0]),
			unsafe.Pointer(&vectors[i+3][0]),
			qLen,
			unsafe.Pointer(&results[i]),
		)
	}

	for ; i < n; i++ {
		v := vectors[i]
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		results[i] = dotAVX512Kernel(qPtr, unsafe.Pointer(&v[0]), qLen)
	}
	return nil
}

func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX512 {
		return cosineBatchAVX2(query, vectors, results)
	}
	if len(query) == 0 || len(vectors) == 0 {
		return nil
	}

	n := len(vectors)
	qLen := len(query)
	qPtr := unsafe.Pointer(&query[0])
	i := 0

	for ; i <= n-4; i += 4 {
		cosineVertical4AVX512(
			qPtr,
			unsafe.Pointer(&vectors[i][0]),
			unsafe.Pointer(&vectors[i+1][0]),
			unsafe.Pointer(&vectors[i+2][0]),
			unsafe.Pointer(&vectors[i+3][0]),
			qLen,
			unsafe.Pointer(&results[i]),
		)
	}

	for ; i < n; i++ {
		v := vectors[i]
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		dot, normA, normB := cosineDotAVX512(qPtr, unsafe.Pointer(&v[0]), qLen)
		if normA == 0 || normB == 0 {
			results[i] = 1.0
		} else {
			results[i] = 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
		}
	}
	return nil
}

func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	if !features.HasAVX512 {
		return euclideanVerticalBatchAVX2(query, vectors, results)
	}
	
	n := len(vectors)
	qLen := len(query)
	queryPtr := unsafe.Pointer(&query[0])
	
	i := 0
	for ; i <= n-4; i += 4 {
		euclideanVertical4AVX512(
			queryPtr,
			unsafe.Pointer(&vectors[i][0]),
			unsafe.Pointer(&vectors[i+1][0]),
			unsafe.Pointer(&vectors[i+2][0]),
			unsafe.Pointer(&vectors[i+3][0]),
			qLen,
			unsafe.Pointer(&results[i]),
		)
	}
	
	for ; i < n; i++ {
		d, err := euclideanAVX512(query, vectors[i])
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func euclideanSQ8BatchAVX512(query []byte, vectors [][]byte, results []float32) error {
	if !features.HasAVX512 {
		return euclideanSQ8BatchAVX2(query, vectors, results)
	}
	
	qLen := len(query)
	qPtr := unsafe.Pointer(&query[0])
	
	for i, v := range vectors {
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		results[i] = float32(euclideanSQ8AVX512Kernel(qPtr, unsafe.Pointer(&v[0]), qLen))
	}
	return nil
}

func euclideanF16BatchAVX512(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	if !features.HasAVX512 {
		return euclideanF16BatchAVX2(query, vectors, results)
	}
	
	qLen := len(query)
	qPtr := unsafe.Pointer(&query[0])
	
	for i, v := range vectors {
		if len(v) != qLen {
			return errors.New("simd: batch dimension mismatch")
		}
		results[i] = euclideanF16AVX512Kernel(qPtr, unsafe.Pointer(&v[0]), qLen)
	}
	return nil
}

// =============================================================================
// Comparison Functions
// =============================================================================

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	if !features.HasAVX512 {
		return matchInt64AVX2(src, val, op, dst)
	}
	matchInt64AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

func matchInt32AVX512(src []int32, val int32, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	if !features.HasAVX512 {
		return matchInt32AVX2(src, val, op, dst)
	}
	matchInt32AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	if !features.HasAVX512 {
		return matchFloat32AVX2(src, val, op, dst)
	}
	matchFloat32AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

func matchFloat64AVX512(src []float64, val float64, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	if !features.HasAVX512 {
		return matchFloat64AVX2(src, val, op, dst)
	}
	matchFloat64AVX512Kernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src))
	return nil
}

// =============================================================================
// Floating Point Variants
// =============================================================================

func euclideanFloat64AVX512(a, b []float64) (float32, error) {
	return euclideanFloat64Unrolled4x(a, b)
}

func dotFloat64AVX512(a, b []float64) (float32, error) {
	return dotFloat64Unrolled4x(a, b)
}

func l2SquaredFloat64AVX512(a, b []float64) (float32, error) {
	return l2SquaredFloat64Unrolled4x(a, b)
}


func euclideanF16AVX512(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return euclideanF16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanF16AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotF16AVX512(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return dotF16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotF16AVX512Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func cosineF16AVX512(a, b []float16.Num) (float32, error) {
	dot, err := dotF16AVX512(a, b)
	if err != nil {
		return 0, err
	}
	// Simplified norm calculation for F16
	var normA, normB float32
	for i := range a {
		fa := a[i].Float32()
		fb := b[i].Float32()
		normA += fa * fa
		normB += fb * fb
	}
	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

// =============================================================================
// L2Squared Fixed Dimension Specializations (no sqrt)
// =============================================================================

func l2Squared128AVX512(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return l2SquaredAVX512(a, b)
	}
	if !features.HasAVX512 {
		return l2Squared128AVX2(a, b)
	}
	return l2Squared128AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot128AVX512(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return dotAVX512(a, b)
	}
	if !features.HasAVX512 {
		return dot128AVX2(a, b)
	}
	return dot128AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared384AVX512(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return l2SquaredAVX512(a, b)
	}
	if !features.HasAVX512 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared384AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared768AVX512(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return l2SquaredAVX512(a, b)
	}
	if !features.HasAVX512 {
		return l2SquaredAVX2(a, b)
	}
	return l2Squared768AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared1024AVX512(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return l2SquaredAVX512(a, b)
	}
	if !features.HasAVX512 {
		return l2Squared1024AVX2(a, b)
	}
	return l2Squared1024AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot1024AVX512(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return dotAVX512(a, b)
	}
	if !features.HasAVX512 {
		return dot1024AVX2(a, b)
	}
	return dot1024AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func l2Squared3072AVX512(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return l2SquaredAVX512(a, b)
	}
	if !features.HasAVX512 {
		return l2Squared3072AVX2(a, b)
	}
	return l2Squared3072AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot3072AVX512(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return dotAVX512(a, b)
	}
	if !features.HasAVX512 {
		return dot3072AVX2(a, b)
	}
	return dot3072AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

// =============================================================================
// Fixed Dimension Specializations
// =============================================================================

func euclidean384AVX512(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return 0, errors.New("simd: length must be 384")
	}
	if !features.HasAVX512 {
		return euclidean384AVX2(a, b)
	}
	return euclidean384AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}


func euclidean768AVX512(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return 0, errors.New("simd: length must be 768")
	}
	if !features.HasAVX512 {
		return euclidean768AVX2(a, b)
	}
	return euclidean768AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}


func euclidean1536AVX512(a, b []float32) (float32, error) {
	if len(a) != 1536 || len(b) != 1536 {
		return 0, errors.New("simd: length must be 1536")
	}
	if !features.HasAVX512 {
		return euclidean1536AVX2(a, b)
	}
	// No specialized 1536 kernel yet, use general AVX512
	return euclideanAVX512(a, b)
}


func dot384AVX512(a, b []float32) (float32, error) {
	if len(a) != 384 || len(b) != 384 {
		return 0, errors.New("simd: length must be 384")
	}
	if !features.HasAVX512 {
		return dotGeneric(a, b)
	}
	return dot384AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot768AVX512(a, b []float32) (float32, error) {
	if len(a) != 768 || len(b) != 768 {
		return 0, errors.New("simd: length must be 768")
	}
	if !features.HasAVX512 {
		return dotGeneric(a, b)
	}
	return dot768AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func dot1536AVX512(a, b []float32) (float32, error) {
	if len(a) != 1536 || len(b) != 1536 {
		return 0, errors.New("simd: length must be 1536")
	}
	if !features.HasAVX512 {
		return dotGeneric(a, b)
	}
	return dot1536AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}

func euclidean128AVX512(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return euclideanAVX512(a, b)
	}
	if !features.HasAVX512 {
		return euclidean128AVX2(a, b)
	}
	return euclidean128AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}


func euclidean1024AVX512(a, b []float32) (float32, error) {
	if len(a) != 1024 || len(b) != 1024 {
		return euclideanAVX512(a, b)
	}
	if !features.HasAVX512 {
		return euclidean1024AVX2(a, b)
	}
	return euclidean1024AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}


func euclidean3072AVX512(a, b []float32) (float32, error) {
	if len(a) != 3072 || len(b) != 3072 {
		return euclideanAVX512(a, b)
	}
	if !features.HasAVX512 {
		return euclidean3072AVX2(a, b)
	}
	return euclidean3072AVX512Kernel(uintptr(unsafe.Pointer(&a[0])), uintptr(unsafe.Pointer(&b[0]))), nil
}


func euclidean16AVX512Wrapper(a, b []float32) (float32, error) {
	if len(a) != 16 || len(b) != 16 {
		return 0, errors.New("simd: expected dimension 16")
	}
	if !features.HasAVX512 {
		return euclideanGeneric(a, b)
	}
	return euclidean16AVX512(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])), nil
}

func cosine16AVX512Wrapper(a, b []float32) (float32, error) {
	if len(a) != 16 || len(b) != 16 {
		return 0, errors.New("simd: expected dimension 16")
	}
	if !features.HasAVX512 {
		return cosineGeneric(a, b)
	}
	dot, normA, normB := cosine16AVX512(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]))
	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

// =============================================================================
// PQ Functions
// =============================================================================

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) error {
	if !features.HasAVX512 {
		return adcBatchAVX2(table, flatCodes, m, results)
	}
	adcBatchAVX512Kernel(unsafe.Pointer(&table[0]), unsafe.Pointer(&flatCodes[0]), m, unsafe.Pointer(&results[0]), len(results))
	return nil
}

func adcBatchVNNI(table []float32, flatCodes []byte, m int, results []float32) error {
	if !features.HasVNNI {
		return adcBatchAVX512(table, flatCodes, m, results)
	}
	// For VNNI, we might need a different table format, but for now we dispatch to the kernel.
	adcBatchVNNIKernel(unsafe.Pointer(&table[0]), unsafe.Pointer(&flatCodes[0]), m, unsafe.Pointer(&results[0]), len(results))
	return nil
}

func euclideanPQVNNI(query []byte, centroids []byte, subDim int, k int, results []float32) error {
	if !features.HasVNNI {
		return errors.New("simd: VNNI not supported")
	}
	qPtr := unsafe.Pointer(&query[0])
	cPtr := unsafe.Pointer(&centroids[0])
	rPtr := unsafe.Pointer(&results[0])
	euclideanPQVNNIKernel(qPtr, cPtr, subDim, k, rPtr)
	return nil
}

func dotInt16AVX512(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return dotInt16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotInt16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func dotUint16AVX512(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return dotUint16AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotUint16AVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)), nil
}

func andBytesAVX512(dst, src []byte) {
	if !features.HasAVX512 {
		andBytesAVX2(dst, src)
		return
	}
	andBytesAVX2(dst, src)
}

func orBytesAVX512(dst, src []byte) {
	if !features.HasAVX512 {
		orBytesAVX2(dst, src)
		return
	}
	orBytesAVX2(dst, src)
}

func euclideanInt8AVX512(a, b []int8) (float32, error) {
	return euclideanInt8Unrolled4x(a, b)
}

func euclideanInt16AVX512(a, b []int16) (float32, error) {
	if !features.HasAVX512 {
		return euclideanInt16AVX2(a, b)
	}
	return euclideanInt16AVX2(a, b)
}

func euclideanUint16AVX512(a, b []uint16) (float32, error) {
	if !features.HasAVX512 {
		return euclideanUint16AVX2(a, b)
	}
	return euclideanUint16AVX2(a, b)
}

func isAllZerosAVX512(data []byte) bool {
	if !features.HasAVX512 {
		return isAllZerosAVX2(data)
	}
	return isAllZerosAVX2(data)
}

