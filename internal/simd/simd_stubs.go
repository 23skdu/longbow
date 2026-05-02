//go:build !amd64

package simd

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow/float16"
	lbcore "github.com/23skdu/longbow/internal/core"
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

func prefetchNTA(p uintptr) {}

func dotFloat64AVX512(a, b []float64) (float32, error) {
	return 0, errors.New("avx512 not supported")
}

func haversineBatchAVX2(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	haversineBatchGeneric(centerLat, centerLon, points, earthRadius, results)
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
func euclideanFloat64AVX2(a, b []float64) (float32, error) { return euclideanFloat64Unrolled4x(a, b) }

func dotInt4AVX512(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt4AVX2(a, b []byte) (float32, error)   { return dotInt4Generic(a, b) }
func dotInt2AVX512(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }
func dotInt2AVX2(a, b []byte) (float32, error)   { return dotInt2Generic(a, b) }

func euclidean16AVX512Wrapper(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func cosine16AVX512Wrapper(a, b []float32) (float32, error)    { return cosineGeneric(a, b) }

func andBytesAVX512(dst, src []byte) { andBytesGeneric(dst, src) }
func orBytesAVX512(dst, src []byte)  { orBytesGeneric(dst, src) }
func isAllZerosAVX512(src []byte) bool { return isAllZerosGeneric(src) }
func euclideanInt8AVX512(a, b []int8) (float32, error) { return euclideanInt8Unrolled4x(a, b) }
func euclideanInt16AVX512(a, b []int16) (float32, error) { return 0, errors.New("avx512 not supported") }
func euclideanUint16AVX512(a, b []uint16) (float32, error) { return 0, errors.New("avx512 not supported") }
func dotInt16AVX512(a, b []int16) (float32, error) { return 0, errors.New("avx512 not supported") }
func dotUint16AVX512(a, b []uint16) (float32, error) { return 0, errors.New("avx512 not supported") }

func int8ToFloat32AVX2(src []int8, dst []float32) { int8ToFloat32Generic(src, dst) }
func uint8ToFloat32AVX2(src []uint8, dst []float32) { uint8ToFloat32Generic(src, dst) }
func int16ToFloat32AVX2(src []int16, dst []float32) { int16ToFloat32Generic(src, dst) }
func uint16ToFloat32AVX2(src []uint16, dst []float32) { uint16ToFloat32Generic(src, dst) }
func int32ToFloat32AVX2(src []int32, dst []float32) { int32ToFloat32Generic(src, dst) }
func uint32ToFloat32AVX2(src []uint32, dst []float32) { uint32ToFloat32Generic(src, dst) }
func float16ToFloat32AVX2(src []float16.Num, dst []float32) { float16ToFloat32Generic(src, dst) }

func int8ToFloat32AVX512(src []int8, dst []float32) { int8ToFloat32Generic(src, dst) }
func uint8ToFloat32AVX512(src []uint8, dst []float32) { uint8ToFloat32Generic(src, dst) }
func int16ToFloat32AVX512(src []int16, dst []float32) { int16ToFloat32Generic(src, dst) }
func uint16ToFloat32AVX512(src []uint16, dst []float32) { uint16ToFloat32Generic(src, dst) }
func int32ToFloat32AVX512(src []int32, dst []float32) { int32ToFloat32Generic(src, dst) }
func uint32ToFloat32AVX512(src []uint32, dst []float32) { uint32ToFloat32Generic(src, dst) }
func float16ToFloat32AVX512(src []float16.Num, dst []float32) { float16ToFloat32Generic(src, dst) }

func sigmoidAVX2(src, dst []float32) { sigmoidGeneric(src, dst) }
func softmaxAVX2(src, dst []float32) { softmaxGeneric(src, dst) }
func expAVX2(src, dst []float32) { expGeneric(src, dst) }
func logAVX2(src, dst []float32) { logGeneric(src, dst) }

func sigmoidAVX512(src, dst []float32) { sigmoidGeneric(src, dst) }
func softmaxAVX512(src, dst []float32) { softmaxGeneric(src, dst) }
func expAVX512(src, dst []float32) { expGeneric(src, dst) }
func logAVX512(src, dst []float32) { logGeneric(src, dst) }

func sumAVX2(src []float32) float32 { return sumGeneric(src) }
func maxAVX2(src []float32) float32 { return maxGeneric(src) }
func minAVX2(src []float32) float32 { return minGeneric(src) }

func sinAVX2(src, dst []float32)   { sinFloat32Generic(src, dst) }
func cosAVX2(src, dst []float32)   { cosFloat32Generic(src, dst) }
func atan2AVX2(y, x, dst []float32) { atan2Float32Generic(y, x, dst) }

func matMulAVX2_Go(a, b []float32, m, n, k int, dst []float32) {
	matMulGeneric(a, b, m, n, k, dst)
}

func ManhattanDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return 0, errors.New("simd: AVX2 not supported on this architecture")
}

func ChebyshevDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return 0, errors.New("simd: AVX2 not supported on this architecture")
}

func BrayCurtisDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return 0, errors.New("simd: AVX2 not supported on this architecture")
}
