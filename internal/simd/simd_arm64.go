//go:build arm64

package simd

import (
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ARM64 NEON implementations
// Using generic unrolled fallbacks for stability while assembly kernels are refined.

func euclideanNEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func dotNEON(a, b []float32) (float32, error) {
	return dotNEONKernel(a, b), nil
}

func cosineNEON(a, b []float32) (float32, error) {
	return cosineNEONKernel(a, b), nil
}

func l2SquaredNEON(a, b []float32) (float32, error) {
	return l2SquaredNEONKernel(a, b), nil
}

func euclidean128NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func euclidean384NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func euclidean768NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func euclidean1024NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func euclidean1536NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func euclidean3072NEON(a, b []float32) (float32, error) {
	return euclideanNEONKernel(a, b), nil
}

func dot128NEON(a, b []float32) (float32, error) {
	return dot128NEONKernel(a, b), nil
}

func dot384NEON(a, b []float32) (float32, error) {
	return dot384NEONKernel(a, b), nil
}

func dot768NEON(a, b []float32) (float32, error) {
	return dot768NEONKernel(a, b), nil
}

func dot1024NEON(a, b []float32) (float32, error) {
	return dot1024NEONKernel(a, b), nil
}

func dot1536NEON(a, b []float32) (float32, error) {
	return dot1536NEONKernel(a, b), nil
}

func dot3072NEON(a, b []float32) (float32, error) {
	return dot3072NEONKernel(a, b), nil
}

func euclideanF16NEON(a, b []float16.Num) (float32, error) {
	return euclideanF16NEONKernel(a, b), nil
}

func dotF16NEON(a, b []float16.Num) (float32, error) {
	return dotF16NEONKernel(a, b), nil
}

func cosineF16NEON(a, b []float16.Num) (float32, error) {
	return cosineF16NEONKernel(a, b), nil
}

func dotInt4Neon(a, b []byte) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return float32(dotInt4NeonKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))), nil // #nosec G103
}

func dotInt2Neon(a, b []byte) (float32, error) {
	if len(a) == 0 {
		return 0, nil
	}
	return float32(dotInt2NeonKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))), nil // #nosec G103
}

func matchInt64Neon(src []int64, val int64, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	matchInt64NeonKernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
	return nil
}

func matchInt32Neon(src []int32, val int32, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	matchInt32NeonKernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
	return nil
}

func matchFloat32Neon(src []float32, val float32, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	matchFloat32NeonKernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
	return nil
}

func matchFloat64Neon(src []float64, val float64, op CompareOp, dst []byte) error {
	if len(src) == 0 {
		return nil
	}
	matchFloat64NeonKernel(unsafe.Pointer(&src[0]), val, int(op), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
	return nil
}

func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, vec := range vectors {
		sum := l2SquaredNEONKernel(query, vec)
		results[i] = float32(math.Sqrt(float64(sum)))
	}
	return nil
}

func dotBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, vec := range vectors {
		results[i] = dotNEONKernel(query, vec)
	}
	return nil
}

func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, vec := range vectors {
		results[i] = cosineNEONKernel(query, vec)
	}
	return nil
}

func FastWalshHadamardTransform32NEON(a []float32) error {
	if len(a) == 32 {
		fastWalshHadamardTransform32NEONKernel(a)
		return nil
	}
	return fastWalshHadamardTransform32Generic(a)
}

func RandomRotationNEON(a []float32, seed int64) error {
	return randomRotationGeneric(a, seed)
}

func int8ToFloat32NEON(src []int8, dst []float32) {
	int8ToFloat32Generic(src, dst)
}

func uint8ToFloat32NEON(src []uint8, dst []float32) {
	uint8ToFloat32Generic(src, dst)
}

func int16ToFloat32NEON(src []int16, dst []float32) {
	int16ToFloat32Generic(src, dst)
}

func uint16ToFloat32NEON(src []uint16, dst []float32) {
	uint16ToFloat32Generic(src, dst)
}

func int32ToFloat32NEON(src []int32, dst []float32) {
	int32ToFloat32Generic(src, dst)
}

func uint32ToFloat32NEON(src []uint32, dst []float32) {
	uint32ToFloat32Generic(src, dst)
}

func float16ToFloat32NEON(src []float16.Num, dst []float32) {
	float16ToFloat32Generic(src, dst) // Use generic for now, f16 conversion is tricky in asm
}

func sigmoidNEON(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	sigmoidNEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

func expNEON(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	expNEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

func logNEON(src, dst []float32) {
	if len(src) == 0 {
		return
	}
	logNEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

func softmaxNEON(src, dst []float32) {
	softmaxGeneric(src, dst)
}

func memcpyNEON(dst, src unsafe.Pointer, n int) {
	memcpyNTA(dst, src, n)
}

func euclideanFloat64NEON(a, b []float64) (float32, error) {
	return euclideanFloat64Unrolled4x(a, b)
}

func dotFloat64NEON(a, b []float64) (float32, error) {
	return dotFloat64Unrolled4x(a, b)
}

func sumNEON(src []float32) float32 {
	return sumGeneric(src)
}

func maxNEON(src []float32) float32 {
	return maxGeneric(src)
}

func minNEON(src []float32) float32 {
	return minGeneric(src)
}

func argMaxNEON(src []float32) int {
	return argMaxGeneric(src)
}

func argMinNEON(src []float32) int {
	return argMinGeneric(src)
}

func matMulNEON(a, b []float32, m, n, k int, dst []float32) {
	matMulGeneric(a, b, m, n, k, dst)
}

// Internal assembly kernels (must have Go declarations to satisfy go vet)
//go:noescape
func expNEONKernel(src, dst unsafe.Pointer, n int)
//go:noescape
func logNEONKernel(src, dst unsafe.Pointer, n int)
//go:noescape
func softmaxNEONKernel(src, dst unsafe.Pointer, n int)
//go:noescape
func sigmoidNEONKernel(src, dst unsafe.Pointer, n int)
//go:noescape
func sumNEONKernel(src unsafe.Pointer, n int) float32
//go:noescape
func maxNEONKernel(src unsafe.Pointer, n int) float32
//go:noescape
func minNEONKernel(src unsafe.Pointer, n int) float32
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
func l2SquaredNEONKernel(a, b []float32) float32
//go:noescape
func dotF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func randomSignFlipNEONKernel(a []float32, seed int64)
//go:noescape
func fastWalshHadamardTransform32NEONKernel(a []float32)
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
//go:noescape
func euclideanF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func cosineF16NEONKernel(a, b []float16.Num) float32
//go:noescape
func vectorButterflyNEONKernel(a, b []float32)
//go:noescape
func vectorButterfly16NEONKernel(a, b []float32)
//go:noescape
func dotInt4NeonKernel(a, b unsafe.Pointer, n int) int32
//go:noescape
func dotInt2NeonKernel(a, b unsafe.Pointer, n int) int32

func manhattanNEON(a, b []float32) (float32, error) {
	return ManhattanDistanceFloat32(a, b)
}

func chebyshevNEON(a, b []float32) (float32, error) {
	return ChebyshevDistanceFloat32(a, b)
}

func brayCurtisNEON(a, b []float32) (float32, error) {
	return BrayCurtisDistanceFloat32(a, b)
}

//go:noescape
func pause()

//go:noescape
func memcpyNTA(dst, src unsafe.Pointer, n int)

// Static assertion to keep Go assembly kernels "used" even if not in the active dispatch path.
// This prevents gopls from reporting them as unused while keeping them available for debugging.
var _ = func() {
	if false {
		pause()
		// Activation kernels (NEON stubs; AVX-512 path used on amd64)
		expNEONKernel(nil, nil, 0)
		logNEONKernel(nil, nil, 0)
		softmaxNEONKernel(nil, nil, 0)
		sigmoidNEONKernel(nil, nil, 0)
		// Reduction kernels
		_ = sumNEONKernel(nil, 0)
		_ = maxNEONKernel(nil, 0)
		_ = minNEONKernel(nil, 0)
		// Distance kernels kept for debugging / future dispatch
		_, _ = dotFloat64NEON(nil, nil)
		_ = dotNEONKernel(nil, nil)
		_ = dotHighDimNEONKernel(nil, nil)
		_ = cosineNEONKernel(nil, nil)
		_ = cosineHighDimNEONKernel(nil, nil)
		_ = l2SquaredNEONKernel(nil, nil)
		_ = dotF16NEONKernel(nil, nil)
		randomSignFlipNEONKernel(nil, 0)
		_ = dot128NEONKernel(nil, nil)
		_ = dot384NEONKernel(nil, nil)
		_ = dot768NEONKernel(nil, nil)
		_ = dot1024NEONKernel(nil, nil)
		_ = dot1536NEONKernel(nil, nil)
		_ = dot3072NEONKernel(nil, nil)
		_ = l2Squared128NEONKernel(nil, nil)
		_ = l2Squared768NEONKernel(nil, nil)
		_ = l2Squared1024NEONKernel(nil, nil)
		_ = l2Squared1536NEONKernel(nil, nil)
		_ = l2Squared3072NEONKernel(nil, nil)
		// Already-guarded kernels
		_ = dotInt4NeonKernel(nil, nil, 0)
		_ = dotInt2NeonKernel(nil, nil, 0)
		_ = euclideanF16NEONKernel(nil, nil)
		_ = cosineF16NEONKernel(nil, nil)
		vectorButterflyNEONKernel(nil, nil)
		vectorButterfly16NEONKernel(nil, nil)
		_ = l2Squared384NEONKernel(nil, nil)
		accumulateWeightedScatterNEON(nil, nil, nil, 0)
		accumulateWeightedScatterNEONKernel(nil, nil, nil, 0, 0)
	}
}

func accumulateWeightedScatterNEON(dst []float32, targets []uint32, weights []float32, factor float32) {
	if len(targets) == 0 {
		return
	}
	n := len(targets)
	if len(weights) < n {
		n = len(weights)
	}
	// Note: We don't check dst bounds here to match baseline behavior and maximize performance.
	// The caller (GraphStore) ensures targets are within range.
	accumulateWeightedScatterNEONKernel(unsafe.Pointer(&dst[0]), unsafe.Pointer(&targets[0]), unsafe.Pointer(&weights[0]), factor, n) // #nosec G103
}

//go:noescape
func accumulateWeightedScatterNEONKernel(dst, targets, weights unsafe.Pointer, factor float32, n int)
