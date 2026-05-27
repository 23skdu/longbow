//go:build amd64 && !avx512

package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Stubs for AVX512 functions on AMD64 systems without avx512 build tag

func euclideanAVX512(a, b []float32) (float32, error) { return euclideanAVX2(a, b) }
func l2SquaredAVX512(a, b []float32) (float32, error) { return l2SquaredAVX2(a, b) }
func dotAVX512(a, b []float32) (float32, error)       { return dotAVX2(a, b) }
func cosineAVX512(a, b []float32) (float32, error)    { return cosineAVX2(a, b) }

func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchAVX2(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchAVX2(query, vectors, results)
}
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchAVX2(query, vectors, results)
}
func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanVerticalBatchAVX2(query, vectors, results)
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64AVX2(src, val, op, dst)
}
func matchInt32AVX512(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32AVX2(src, val, op, dst)
}
func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32AVX2(src, val, op, dst)
}
func matchFloat64AVX512(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64AVX2(src, val, op, dst)
}

func euclidean128AVX512(a, b []float32) (float32, error)  { return euclidean128AVX2(a, b) }
func euclidean384AVX512(a, b []float32) (float32, error)  { return euclidean384AVX2(a, b) }
func euclidean768AVX512(a, b []float32) (float32, error)  { return euclidean768AVX2(a, b) }
func euclidean1024AVX512(a, b []float32) (float32, error) { return euclidean1024AVX2(a, b) }
func euclidean1536AVX512(a, b []float32) (float32, error) { return euclidean1536AVX2(a, b) }
func euclidean3072AVX512(a, b []float32) (float32, error) { return euclidean3072AVX2(a, b) }

func dot128AVX512(a, b []float32) (float32, error)  { return dot128AVX2(a, b) }
func dot384AVX512(a, b []float32) (float32, error)  { return dot384AVX2(a, b) }
func dot768AVX512(a, b []float32) (float32, error)  { return dot768AVX2(a, b) }
func dot1024AVX512(a, b []float32) (float32, error) { return dot1024AVX2(a, b) }
func dot1536AVX512(a, b []float32) (float32, error) { return dot1536AVX2(a, b) }
func dot3072AVX512(a, b []float32) (float32, error) { return dot3072AVX2(a, b) }

func l2Squared128AVX512(a, b []float32) (float32, error)  { return l2Squared128AVX2(a, b) }
func l2Squared384AVX512(a, b []float32) (float32, error)  { return l2Squared384AVX2(a, b) }
func l2Squared768AVX512(a, b []float32) (float32, error)  { return l2Squared768AVX2(a, b) }
func l2Squared1024AVX512(a, b []float32) (float32, error) { return l2Squared1024AVX2(a, b) }
func l2Squared3072AVX512(a, b []float32) (float32, error) { return l2Squared3072AVX2(a, b) }

func euclideanFloat64AVX512(a, b []float64) (float32, error) { return euclideanFloat64AVX2(a, b) }
func l2SquaredFloat64AVX512(a, b []float64) (float32, error) { return l2SquaredFloat64AVX2(a, b) }
func dotFloat64AVX512(a, b []float64) (float32, error)       { return dotFloat64AVX2(a, b) }

func euclideanSQ8BatchAVX512(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchAVX2(query, vectors, results)
}

func euclideanF16AVX512(a, b []float16.Num) (float32, error) { return euclideanF16AVX2(a, b) }
func dotF16AVX512(a, b []float16.Num) (float32, error)       { return dotF16AVX2(a, b) }
func cosineF16AVX512(a, b []float16.Num) (float32, error)    { return cosineF16AVX2(a, b) }
func euclideanF16BatchAVX512(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchAVX2(query, vectors, results)
}

func andBytesAVX512(dst, src []byte)    { andBytesAVX2(dst, src) }
func orBytesAVX512(dst, src []byte)     { orBytesAVX2(dst, src) }
func isAllZerosAVX512(data []byte) bool { return isAllZerosAVX2(data) }

func UnpackTQ2AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ2AVX2(src, dst, scale, bias)
}
func UnpackTQ4AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ4AVX2(src, dst, scale, bias)
}
func UnpackTQ8AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ8AVX2(src, dst, scale, bias)
}
func PackTQ2AVX512(src []float32, dst []byte) { PackTQ2AVX2(src, dst) }
func PackTQ4AVX512(src []float32, dst []byte) { PackTQ4AVX2(src, dst) }
func PackTQ8AVX512(src []float32, dst []byte) { PackTQ8AVX2(src, dst) }

func UnpackTQ2AVX512VBMI(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ2AVX2(src, dst, scale, bias)
}
func PackTQ2AVX512VBMI(src []float32, dst []byte) { PackTQ2AVX2(src, dst) }

func euclideanInt8AVX512(a, b []int8) (float32, error)     { return euclideanInt8Unrolled4x(a, b) }
func euclideanInt16AVX512(a, b []int16) (float32, error)   { return euclideanInt16AVX2(a, b) }
func euclideanUint16AVX512(a, b []uint16) (float32, error) { return euclideanUint16AVX2(a, b) }
func dotInt16AVX512(a, b []int16) (float32, error)         { return dotInt16AVX2(a, b) }
func dotUint16AVX512(a, b []uint16) (float32, error)       { return dotUint16AVX2(a, b) }

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchAVX2(table, flatCodes, m, results)
}

func adcBatchVNNI(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchAVX2(table, flatCodes, m, results)
}

func euclidean16AVX512Wrapper(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func cosine16AVX512Wrapper(a, b []float32) (float32, error)    { return cosineGeneric(a, b) }

func l2SquaredBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return l2SquaredBatchAVX2(query, vectors, results)
}
