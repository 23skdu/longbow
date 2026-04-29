//go:build arm64

package simd

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ARM64 NEON implementations
// Using generic unrolled fallbacks for stability while assembly kernels are refined.

func euclideanNEON(a, b []float32) (float32, error) {
	return euclideanUnrolled4x(a, b)
}

func dotNEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func cosineNEON(a, b []float32) (float32, error) {
	return cosineUnrolled4x(a, b)
}

func l2SquaredNEON(a, b []float32) (float32, error) {
	d, err := euclideanUnrolled4x(a, b)
	if err != nil {
		return 0, err
	}
	return d * d, nil
}

func euclidean128NEON(a, b []float32) (float32, error) {
	return euclidean128Unrolled4x(a, b)
}

func euclidean384NEON(a, b []float32) (float32, error) {
	return euclidean384Unrolled4x(a, b)
}

func euclidean768NEON(a, b []float32) (float32, error) {
	return euclidean768Unrolled4x(a, b)
}

func euclidean1024NEON(a, b []float32) (float32, error) {
	return euclideanUnrolled4x(a, b)
}

func euclidean1536NEON(a, b []float32) (float32, error) {
	return euclidean1536Unrolled4x(a, b)
}

func euclidean3072NEON(a, b []float32) (float32, error) {
	return euclideanUnrolled4x(a, b)
}

func dot128NEON(a, b []float32) (float32, error) {
	return dot128Unrolled4x(a, b)
}

func dot384NEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func dot768NEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func dot1024NEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func dot1536NEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func dot3072NEON(a, b []float32) (float32, error) {
	return dotUnrolled4x(a, b)
}

func euclideanF16NEON(a, b []float16.Num) (float32, error) {
	return euclideanF16Unrolled4x(a, b)
}

func dotF16NEON(a, b []float16.Num) (float32, error) {
	return dotF16Unrolled4x(a, b)
}

func cosineF16NEON(a, b []float16.Num) (float32, error) {
	return cosineF16Unrolled4x(a, b)
}

func dotInt4Neon(a, b []byte) (float32, error) {
	return dotInt4Generic(a, b)
}

func dotInt2Neon(a, b []byte) (float32, error) {
	return dotInt2Generic(a, b)
}

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

func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchUnrolled4x(query, vectors, results)
}

func dotBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchUnrolled4x(query, vectors, results)
}

func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchUnrolled4x(query, vectors, results)
}

func FastWalshHadamardTransform32NEON(a []float32) error {
	return fastWalshHadamardTransform32Generic(a)
}

func RandomRotationNEON(a []float32, seed int64) error {
	return randomRotationGeneric(a, seed)
}

func euclideanFloat64NEON(a, b []float64) (float32, error) {
	return euclideanFloat64Unrolled4x(a, b)
}

func cosineFloat64NEON(a, b []float64) (float32, error) {
	return cosineFloat64Unrolled4x(a, b)
}

func dotFloat64NEON(a, b []float64) (float32, error) {
	return dotFloat64Unrolled4x(a, b)
}

// Internal assembly kernels (must have Go declarations to satisfy go vet)
//go:noescape
func euclideanNEONKernel(a, b []float32) float32
//go:noescape
func euclideanHighDimNEONKernel(a, b []float32) float32
//go:noescape
func dotNEONKernel(a, b []float32) float32
//go:noescape
func dotHighDimNEONKernel(a, b []float32) float32
//go:noescape
func cosineNEONKernel(a, b []float32) float32
//go:noescape
func cosineHighDimNEONKernel(a, b []float32) float32
//go:noescape
func dotInt4NeonKernel(a, b unsafe.Pointer, n int) float32
//go:noescape
func dotInt2NeonKernel(a, b unsafe.Pointer, n int) float32
//go:noescape
func l2SquaredNEONKernel(a, b []float32) float32
//go:noescape
func euclideanF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func dotF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func cosineF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func randomSignFlipNEONKernel(a []float32, seed int64)
//go:noescape
func fastWalshHadamardTransform32NEONKernel(a []float32)
//go:noescape
func vectorButterflyNEONKernel(a, b []float32)
//go:noescape
func vectorButterfly16NEONKernel(a, b []float32)
//go:noescape
func dot128NEONKernel(a, b []float32) float32
//go:noescape
func dot384NEONKernel(a, b []float32) float32
//go:noescape
func dot768NEONKernel(a, b []float32) float32
//go:noescape
func dot1024NEONKernel(a, b []float32) float32
//go:noescape
func dot1536NEONKernel(a, b []float32) float32
//go:noescape
func dot3072NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared128NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared384NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared768NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared1024NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared1536NEONKernel(a, b []float32) float32
//go:noescape
func l2Squared3072NEONKernel(a, b []float32) float32
