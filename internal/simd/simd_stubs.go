//go:build !amd64

package simd

import (
	"errors"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Stubs for non-AMD64 architectures to satisfy simd.go references

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}

func matchFloat64AVX2(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64Generic(src, val, op, dst)
}

func matchFloat64AVX512(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64Generic(src, val, op, dst)
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

func adcBatchVNNI(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

func euclideanAVX2(a, b []float32) (float32, error)       { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) (float32, error)     { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) (float32, error)          { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) (float32, error)        { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) (float32, error)             { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) (float32, error)           { return dotGeneric(a, b) }
func euclidean384AVX512(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean768AVX512(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean1536AVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }

func euclidean384AVX2(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean768AVX2(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean1536AVX2(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }

func dot384AVX512(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot768AVX512(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot1536AVX512(a, b []float32) (float32, error) { return dotGeneric(a, b) }

func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchGeneric(query, vectors, results)
}
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func prefetchNTA(p unsafe.Pointer) {}

func euclideanFloat64AVX2(a, b []float64) (float32, error) {
	return 0, errors.New("avx2 not supported")
}
func euclideanFloat64AVX512(a, b []float64) (float32, error) {
	return 0, errors.New("avx512 not supported")
}
func euclideanInt8AVX2(a, b []int8) (float32, error)   { return 0, errors.New("avx2 not supported") }
func euclideanInt16AVX2(a, b []int16) (float32, error) { return 0, errors.New("avx2 not supported") }
func euclideanUint16AVX2(a, b []uint16) (float32, error) { return 0, errors.New("avx2 not supported") }
func dotInt16AVX2(a, b []int16) (float32, error) { return 0, errors.New("avx2 not supported") }
func dotUint16AVX2(a, b []uint16) (float32, error) { return 0, errors.New("avx2 not supported") }

func l2SquaredAVX2(a, b []float32) (float32, error)   { return L2SquaredFloat32(a, b) }
func l2SquaredAVX512(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }

func euclideanSQ8BatchAVX2(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}

func euclideanSQ8BatchAVX512(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}

func euclideanF16BatchAVX2(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchGeneric(query, vectors, results)
}

func euclideanF16BatchAVX512(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchGeneric(query, vectors, results)
}

func matchInt32AVX2(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32Generic(src, val, op, dst)
}

func matchInt32AVX512(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32Generic(src, val, op, dst)
}

func andBytesAVX2(dst, src []byte) { andBytesGeneric(dst, src) }
func orBytesAVX2(dst, src []byte)  { orBytesGeneric(dst, src) }
func isAllZerosAVX2(src []byte) bool { return isAllZerosGeneric(src) }

func dotFloat64AVX2(a, b []float64) (float32, error)   { return dotFloat64Unrolled4x(a, b) }
func dotFloat64AVX512(a, b []float64) (float32, error) { return dotFloat64Unrolled4x(a, b) }

func dotInt4AVX512(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt4AVX2(a, b []byte) (float32, error)   { return dotInt4Generic(a, b) }
func dotInt2AVX512(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }
func dotInt2AVX2(a, b []byte) (float32, error)   { return dotInt2Generic(a, b) }

func euclidean16AVX512Wrapper(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func cosine16AVX512Wrapper(a, b []float32) (float32, error)    { return cosineGeneric(a, b) }
