//go:build !amd64

package simd

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow/float16"
	lbcore "github.com/23skdu/longbow/internal/core"
)

// Stubs for non-x86 architectures to satisfy references in dispatch.go

func euclideanAVX2(a, b []float32) (float32, error)   { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) (float32, error)      { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) (float32, error)    { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) (float32, error)         { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) (float32, error)       { return dotGeneric(a, b) }

func euclidean128AVX2(a, b []float32) (float32, error)   { return euclideanGeneric(a, b) }
func euclidean384AVX2(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean768AVX2(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean1024AVX2(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func euclidean1536AVX2(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func euclidean3072AVX2(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }

func euclidean128AVX512(a, b []float32) (float32, error)   { return euclideanGeneric(a, b) }
func euclidean384AVX512(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean768AVX512(a, b []float32) (float32, error)  { return euclideanGeneric(a, b) }
func euclidean1024AVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func euclidean1536AVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func euclidean3072AVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }

func dot128AVX2(a, b []float32) (float32, error)   { return dotGeneric(a, b) }
func dot384AVX2(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot768AVX2(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot1024AVX2(a, b []float32) (float32, error) { return dotGeneric(a, b) }
func dot1536AVX2(a, b []float32) (float32, error) { return dotGeneric(a, b) }
func dot3072AVX2(a, b []float32) (float32, error) { return dotGeneric(a, b) }

func dot128AVX512(a, b []float32) (float32, error)   { return dotGeneric(a, b) }
func dot384AVX512(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot768AVX512(a, b []float32) (float32, error)  { return dotGeneric(a, b) }
func dot1024AVX512(a, b []float32) (float32, error) { return dotGeneric(a, b) }
func dot1536AVX512(a, b []float32) (float32, error) { return dotGeneric(a, b) }
func dot3072AVX512(a, b []float32) (float32, error) { return dotGeneric(a, b) }

func l2Squared128AVX2(a, b []float32) (float32, error)   { return L2SquaredFloat32(a, b) }
func l2Squared384AVX2(a, b []float32) (float32, error)  { return L2SquaredFloat32(a, b) }
func l2Squared768AVX2(a, b []float32) (float32, error)  { return L2SquaredFloat32(a, b) }
func l2Squared1024AVX2(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared3072AVX2(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }

func l2Squared128AVX512(a, b []float32) (float32, error)   { return L2SquaredFloat32(a, b) }
func l2Squared384AVX512(a, b []float32) (float32, error)  { return L2SquaredFloat32(a, b) }
func l2Squared768AVX512(a, b []float32) (float32, error)  { return L2SquaredFloat32(a, b) }
func l2Squared1024AVX512(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared3072AVX512(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }

func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchGeneric(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}

func l2SquaredAVX2(a, b []float32) (float32, error)   { return L2SquaredFloat32(a, b) }
func l2SquaredAVX512(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func prefetchNTA(p uintptr) {}

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

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}
func matchInt32AVX512(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32Generic(src, val, op, dst)
}
func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}
func matchFloat64AVX512(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64Generic(src, val, op, dst)
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

func matMulAVX2(a, b []float32, m, n, k int, dst []float32) { matMulGeneric(a, b, m, n, k, dst) }
func argMaxAVX2(src []float32) int { return argMaxGeneric(src) }
func argMinAVX2(src []float32) int { return argMinGeneric(src) }

func sinAVX2(src, dst []float32) { sinFloat32Generic(src, dst) }
func cosAVX2(src, dst []float32) { cosFloat32Generic(src, dst) }
func atan2AVX2(y, x, dst []float32) { atan2Float32Generic(y, x, dst) }

func haversineBatchAVX2(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	haversineBatchGeneric(centerLat, centerLon, points, earthRadius, results)
}

func dotFloat64AVX2(a, b []float64) (float32, error) { return dotFloat64Unrolled4x(a, b) }
func euclideanFloat64AVX2(a, b []float64) (float32, error) { return euclideanFloat64Unrolled4x(a, b) }

func dotInt4AVX2(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt2AVX2(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }

func andBytesAVX2(dst, src []byte) { andBytesGeneric(dst, src) }
func orBytesAVX2(dst, src []byte)  { orBytesGeneric(dst, src) }
func isAllZerosAVX2(src []byte) bool { return isAllZerosGeneric(src) }

func euclideanSQ8BatchAVX512(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}
func euclideanF16BatchAVX512(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchGeneric(query, vectors, results)
}
func andBytesAVX512(dst, src []byte) { andBytesGeneric(dst, src) }
func orBytesAVX512(dst, src []byte)  { orBytesGeneric(dst, src) }
func isAllZerosAVX512(src []byte) bool { return isAllZerosGeneric(src) }

func euclideanFloat64AVX512(a, b []float64) (float32, error) { return euclideanFloat64Unrolled4x(a, b) }
func dotFloat64AVX512(a, b []float64) (float32, error)      { return dotFloat64Unrolled4x(a, b) }

func euclideanInt8AVX512(a, b []int8) (float32, error) { return euclideanInt8Unrolled4x(a, b) }
func euclideanInt16AVX512(a, b []int16) (float32, error) { return 0, errors.New("avx512 not supported") }
func euclideanUint16AVX512(a, b []uint16) (float32, error) { return 0, errors.New("avx512 not supported") }
func dotInt16AVX512(a, b []int16) (float32, error) { return 0, errors.New("avx512 not supported") }
func dotUint16AVX512(a, b []uint16) (float32, error) { return 0, errors.New("avx512 not supported") }

func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
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

func euclideanInt8AVX2(_, _ []int8) (float32, error)   { return 0, errors.New("avx2 not supported") }
func euclideanInt16AVX2(_, _ []int16) (float32, error) { return 0, errors.New("avx2 not supported") }
func euclideanUint16AVX2(_, _ []uint16) (float32, error) { return 0, errors.New("avx2 not supported") }
func dotInt16AVX2(_, _ []int16) (float32, error) { return 0, errors.New("avx2 not supported") }
func dotUint16AVX2(_, _ []uint16) (float32, error) { return 0, errors.New("avx2 not supported") }

func euclideanSQ8BatchAVX2(query []byte, vectors [][]byte, results []float32) error {
	return euclideanSQ8BatchGeneric(query, vectors, results)
}
func euclideanF16BatchAVX2(query []float16.Num, vectors [][]float16.Num, results []float32) error {
	return euclideanF16BatchGeneric(query, vectors, results)
}

func euclidean16AVX512Wrapper(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func cosine16AVX512Wrapper(a, b []float32) (float32, error)    { return cosineGeneric(a, b) }

func dotInt4AVX512(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt2AVX512(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }

func matMulAVX2Go(a, b []float32, m, n, k int, dst []float32) {
	matMulGeneric(a, b, m, n, k, dst)
}
// ManhattanDistanceFloat32AVX2 is a stub for non-AMD64 architectures.
func ManhattanDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return ManhattanDistanceFloat32(a, b)
}

// ChebyshevDistanceFloat32AVX2 is a stub for non-AMD64 architectures.
func ChebyshevDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return ChebyshevDistanceFloat32(a, b)
}

// BrayCurtisDistanceFloat32AVX2 is a stub for non-AMD64 architectures.
func BrayCurtisDistanceFloat32AVX2(a, b []float32) (float32, error) {
	return BrayCurtisDistanceFloat32(a, b)
}

func UnpackTQ2AVX2(src []byte, dst []float32, scale, bias float32) { UnpackTQ2Generic(src, dst, scale, bias) }
func UnpackTQ4AVX2(src []byte, dst []float32, scale, bias float32) { UnpackTQ4Generic(src, dst, scale, bias) }
func UnpackTQ8AVX2(src []byte, dst []float32, scale, bias float32) { UnpackTQ8Generic(src, dst, scale, bias) }
func PackTQ2AVX2(src []float32, dst []byte) { PackTQ2Generic(src, dst) }
func PackTQ4AVX2(src []float32, dst []byte) { PackTQ4Generic(src, dst) }
func PackTQ8AVX2(src []float32, dst []byte) { PackTQ8Generic(src, dst) }

func UnpackTQ2AVX512(src []byte, dst []float32, scale, bias float32) { UnpackTQ2Generic(src, dst, scale, bias) }
func UnpackTQ4AVX512(src []byte, dst []float32, scale, bias float32) { UnpackTQ4Generic(src, dst, scale, bias) }
func UnpackTQ8AVX512(src []byte, dst []float32, scale, bias float32) { UnpackTQ8Generic(src, dst, scale, bias) }
func PackTQ2AVX512(src []float32, dst []byte) { PackTQ2Generic(src, dst) }
func PackTQ4AVX512(src []float32, dst []byte) { PackTQ4Generic(src, dst) }
func PackTQ8AVX512(src []float32, dst []byte) { PackTQ8Generic(src, dst) }
