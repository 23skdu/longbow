//go:build !arm64

package simd

import (
	"errors"
	"unsafe"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func euclideanNEON(a, b []float32) (float32, error)     { return euclideanUnrolled4x(a, b) }
func euclidean384NEON(a, b []float32) (float32, error)  { return euclideanUnrolled4x(a, b) }
func euclidean768NEON(a, b []float32) (float32, error)  { return euclideanUnrolled4x(a, b) }
func euclidean1536NEON(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func euclidean128NEON(a, b []float32) (float32, error)  { return euclidean128Unrolled4x(a, b) }
func cosineNEON(a, b []float32) (float32, error)        { return cosineUnrolled4x(a, b) }
func dotNEON(a, b []float32) (float32, error)           { return dotUnrolled4x(a, b) }
func dot384NEON(a, b []float32) (float32, error)        { return dotUnrolled4x(a, b) }
func dot768NEON(a, b []float32) (float32, error)        { return dotUnrolled4x(a, b) }
func dot1536NEON(a, b []float32) (float32, error)       { return dotUnrolled4x(a, b) }
func dot128NEON(a, b []float32) (float32, error)        { return dot128Unrolled4x(a, b) }

func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchUnrolled4x(query, vectors, results)
}
func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchUnrolled4x(query, vectors, results)
}
func dotBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchUnrolled4x(query, vectors, results)
}
func adcBatchNEON(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}
func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchUnrolled4x(query, vectors, results)
}

func euclideanF16NEON(a, b []float16.Num) (float32, error) { return euclideanF16Unrolled4x(a, b) }
func cosineF16NEON(a, b []float16.Num) (float32, error)    { return cosineF16Unrolled4x(a, b) }
func dotF16NEON(a, b []float16.Num) (float32, error)       { return dotF16Unrolled4x(a, b) }

func l2SquaredNEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared128NEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared384NEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared768NEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared1024NEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
func l2Squared3072NEON(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }

// FastWalshHadamardTransform32NEON is a stub for non-ARM64 platforms.
func FastWalshHadamardTransform32NEON(a []float32) error { return fastWalshHadamardTransform32Generic(a) }
// RandomRotationNEON is a stub for non-ARM64 platforms.
func RandomRotationNEON(a []float32, seed int64) error  { return randomRotationGeneric(a, seed) }

func dotInt4Neon(a, b []byte) (float32, error) { return dotInt4Generic(a, b) }
func dotInt2Neon(a, b []byte) (float32, error) { return dotInt2Generic(a, b) }

func matchInt64Neon(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}
func matchInt32Neon(src []int32, val int32, op CompareOp, dst []byte) error {
	return matchInt32Generic(src, val, op, dst)
}
func matchFloat32Neon(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}
func matchFloat64Neon(src []float64, val float64, op CompareOp, dst []byte) error {
	return matchFloat64Generic(src, val, op, dst)
}

func euclidean1024NEON(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func euclidean3072NEON(a, b []float32) (float32, error) { return euclideanUnrolled4x(a, b) }
func dot1024NEON(a, b []float32) (float32, error)       { return dotUnrolled4x(a, b) }
func dot3072NEON(a, b []float32) (float32, error)       { return dotUnrolled4x(a, b) }
func euclideanFloat64NEON(a, b []float64) (float32, error) { return euclideanFloat64Unrolled4x(a, b) }

func int8ToFloat32NEON(src []int8, dst []float32) { int8ToFloat32Generic(src, dst) }
func uint8ToFloat32NEON(src []uint8, dst []float32) { uint8ToFloat32Generic(src, dst) }
func int16ToFloat32NEON(src []int16, dst []float32) { int16ToFloat32Generic(src, dst) }
func uint16ToFloat32NEON(src []uint16, dst []float32) { uint16ToFloat32Generic(src, dst) }
func int32ToFloat32NEON(src []int32, dst []float32) { int32ToFloat32Generic(src, dst) }
func uint32ToFloat32NEON(src []uint32, dst []float32) { uint32ToFloat32Generic(src, dst) }
func float16ToFloat32NEON(src []float16.Num, dst []float32) { float16ToFloat32Generic(src, dst) }

func sigmoidNEON(src, dst []float32) { sigmoidGeneric(src, dst) }
func softmaxNEON(src, dst []float32) { softmaxGeneric(src, dst) }
func expNEON(src, dst []float32) { expGeneric(src, dst) }
func logNEON(src, dst []float32) { logGeneric(src, dst) }

func sumNEON(src []float32) float32 { return sumGeneric(src) }
func maxNEON(src []float32) float32 { return maxGeneric(src) }
func minNEON(src []float32) float32 { return minGeneric(src) }

func manhattanNEON(a, b []float32) (float32, error) {
	_, _ = a, b
	return 0, errors.New("simd: NEON not supported on this architecture")
}

func chebyshevNEON(a, b []float32) (float32, error) {
	_, _ = a, b
	return 0, errors.New("simd: NEON not supported on this architecture")
}

func brayCurtisNEON(a, b []float32) (float32, error) {
	_, _ = a, b
	return 0, errors.New("simd: NEON not supported on this architecture")
}

func memcpyNEON(dst, src unsafe.Pointer, n int) { memcpyGeneric(dst, src, n) }

func accumulateWeightedScatterNEON(dst []float32, targets []uint32, weights []float32, factor float32) {
	accumulateWeightedScatterGeneric(dst, targets, weights, factor)
}

func matMulNEON(a, b []float32, m, n, k int, dst []float32) { matMulGeneric(a, b, m, n, k, dst) }
func argMaxNEON(src []float32) int { return argMaxGeneric(src) }
func argMinNEON(src []float32) int { return argMinGeneric(src) }

func andBitVectorsNEON(a, b []uint64) { AndBitVectorsGeneric(a, b) }
func countBitVectorNEON(src []uint64) int {
	return CountBitVectorGeneric(src)
}
func hammingNEON(a, b []uint64) (float32, error) {
	return float32(HammingDistanceGeneric(a, b)), nil
}

// Static assertion to keep Go functions "used" even if not in the active dispatch path.
var _ = func() {
	if false {
		_ = adcBatchNEON(nil, nil, 0, nil)
		_ = euclideanVerticalBatchNEON(nil, nil, nil)
		_, _ = manhattanNEON(nil, nil)
		_, _ = chebyshevNEON(nil, nil)
		_, _ = brayCurtisNEON(nil, nil)
	}
}


