//go:build !arm64

package simd

import (
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

func FastWalshHadamardTransform32NEON(a []float32) error { return fastWalshHadamardTransform32Generic(a) }
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


