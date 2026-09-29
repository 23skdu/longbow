//go:build amd64

package simd

import (
	"errors"
	"math"
	"unsafe" // #nosec G103
)

// The AVX-512 float64 wrappers are compiled into every amd64 build. The
// kernels they call contain AVX-512 instructions, so each wrapper checks the
// runtime capability flag first and falls back to the AVX2 path when the
// instruction set is unavailable.

func euclideanFloat64AVX512(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return euclideanFloat64AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanFloat64AVX512Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func dotFloat64AVX512(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return dotFloat64AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotFloat64AVX512Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}

func cosineFloat64AVX512(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return cosineFloat64AVX2(a, b)
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	dot, normA, normB := cosineFloat64AVX512Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a),
	)
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))), nil
}

func l2SquaredFloat64AVX512(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if !features.HasAVX512 {
		return l2SquaredFloat64AVX2(a, b)
	}
	if len(a) == 0 {
		return 0, nil
	}
	return l2SquaredFloat64AVX512Kernel(
		uintptr(unsafe.Pointer(&a[0])), // #nosec G103
		uintptr(unsafe.Pointer(&b[0])), // #nosec G103
		len(a)), nil
}
