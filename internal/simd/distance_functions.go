package simd

import (
	"errors"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// =============================================================================
// Individual Distance Functions
// =============================================================================

// EuclideanDistance calculates the Euclidean distance between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func EuclideanDistance(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) == 384 {
		return euclideanDistance384Impl(a, b)
	}
	if len(a) == 128 {
		return euclideanDistance128Impl(a, b)
	}
	return euclideanDistanceImpl(a, b)
}

// L2Squared calculates the squared Euclidean distance between two vectors.
// Optimized for PQ training and ADC table construction.
func L2Squared(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return l2SquaredImpl(a, b)
}

// CosineDistance calculates the cosine distance (1 - similarity) between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func CosineDistance(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	return cosineDistanceImpl(a, b)
}

// DotProduct calculates the dot product of two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func DotProduct(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) == 384 {
		return dotProduct384Impl(a, b)
	}
	if len(a) == 128 {
		return dotProduct128Impl(a, b)
	}
	return dotProductImpl(a, b)
}

// EuclideanDistanceF16 calculates the Euclidean distance between two FP16 vectors.
func EuclideanDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("euclidean", implementation).Inc()
	return euclideanDistanceF16Impl(a, b)
}

// CosineDistanceF16 calculates the cosine distance between two FP16 vectors.
func CosineDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("cosine", implementation).Inc()
	return cosineDistanceF16Impl(a, b)
}

// DotProductF16 calculates the dot product of two FP16 vectors.
func DotProductF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("dot", implementation).Inc()
	return dotProductF16Impl(a, b)
}

// EuclideanDistanceFloat64 calculates Euclidean distance for Float64 vectors.
// Returns float32 distance for consistency with other metrics.
func EuclideanDistanceFloat64(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanDistanceFloat64Impl(a, b)
}

// EuclideanDistanceComplex64 calculates Euclidean distance for Complex64 vectors.
func EuclideanDistanceComplex64(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	// Unsafe cast to []float32
	vfA := unsafe.Slice((*float32)(unsafe.Pointer(&a[0])), len(a)*2)
	vfB := unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)*2)

	return EuclideanDistance(vfA, vfB)
}

// EuclideanDistanceComplex128 calculates Euclidean distance for Complex128 vectors.
func EuclideanDistanceComplex128(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	// Optimization: complex128 is just 2x float64s in memory.
	// We can treat them as float64 vectors of 2*N length and use the optimized
	// AVX/AVX512 kernels for float64.
	//
	// Euclidean distance is sqrt(sum((real_diff^2 + imag_diff^2))), which is
	// exactly what 2N float64 euclidean distance calculates.

	// Unsafe cast to []float64
	vfA := unsafe.Slice((*float64)(unsafe.Pointer(&a[0])), len(a)*2)
	vfB := unsafe.Slice((*float64)(unsafe.Pointer(&b[0])), len(b)*2)

	return EuclideanDistanceFloat64(vfA, vfB)
}

// DotProductF64 calculates the dot product of two Float64 vectors.
func DotProductF64(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotFloat64Unrolled4x(a, b)
}

// DotProductComplex64 calculates the real part of the dot product of two Complex64 vectors.
func DotProductComplex64(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotComplex64Unrolled(a, b)
}

// DotProductComplex128 calculates the real part of the dot product of two Complex128 vectors.
func DotProductComplex128(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotComplex128Unrolled(a, b)
}

// L2SquaredFloat32 calculates the squared Euclidean distance using a generic implementation.
// This is used for PQ training where we need precise control over the implementation.
func L2SquaredFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0

	// Unrolled loop for better performance
	for ; i < n-3; i += 4 {
		diff0 := a[i] - b[i]
		diff1 := a[i+1] - b[i+1]
		diff2 := a[i+2] - b[i+2]
		diff3 := a[i+3] - b[i+3]

		sum0 += diff0 * diff0
		sum1 += diff1 * diff1
		sum2 += diff2 * diff2
		sum3 += diff3 * diff3
	}

	// Handle remaining elements
	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum0 += diff * diff
	}

	return sum0 + sum1 + sum2 + sum3, nil
}

// Prefetch hints to the CPU to fetch data into cache for future use.
// It uses the PREFETCHNTA instruction on x86 for non-temporal access.
func Prefetch(p unsafe.Pointer) {
	prefetchImpl(p)
}
