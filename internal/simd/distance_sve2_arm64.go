//go:build arm64

package simd

import (
	"math"
)

// SVE2 kernels: when CPU has SVE2 (Graviton 3/4, Grace Hopper), SVE2 predicate-driven
// vector loops process 256-bit or 512-bit vectors per iteration.
func euclideanSVE2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	sum := l2SquaredSVE2Kernel(a, b)
	return float32(math.Sqrt(float64(sum))), nil
}

func l2SquaredSVE2(a, b []float32) float32 {
	return l2SquaredSVE2Kernel(a, b)
}

func dotSVE2(a, b []float32) float32 {
	return dotSVE2Kernel(a, b)
}

func cosineSVE2(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, ErrDimensionMismatch
	}
	dot := dotSVE2Kernel(a, b)
	normA := dotSVE2Kernel(a, a)
	normB := dotSVE2Kernel(b, b)
	denom := float32(math.Sqrt(float64(normA * normB)))
	if denom == 0 {
		return 0, nil
	}
	cos := dot / denom
	return 1.0 - cos, nil
}

func l2SquaredSVE2Kernel(a, b []float32) float32
func dotSVE2Kernel(a, b []float32) float32
