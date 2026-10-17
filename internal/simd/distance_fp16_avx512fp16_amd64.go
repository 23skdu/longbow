//go:build amd64

package simd

import (
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// euclideanF16AVX512FP16 evaluates Euclidean distance using native AVX-512 FP16 instructions
// without widening through 32-bit floats.
func euclideanF16AVX512FP16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX512FP16 {
		return euclideanF16AVX512(a, b)
	}
	sum := l2SquaredF16AVX512FP16Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
	)
	if sum < 0 {
		sum = 0
	}
	return float32(math.Sqrt(float64(sum))), nil
}

func dotF16AVX512FP16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	if len(a) == 0 {
		return 0, nil
	}
	if !features.HasAVX512FP16 {
		return dotF16AVX512(a, b)
	}
	return dotF16AVX512FP16Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
	), nil
}

func l2SquaredF16AVX512FP16Kernel(a, b uintptr, n int) float32
func dotF16AVX512FP16Kernel(a, b uintptr, n int) float32
