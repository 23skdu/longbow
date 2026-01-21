package simd

import (
	"errors"
	"math"
)

// =============================================================================
// Integer Baseline Kernels (Unrolled 4x)
// Using float32 accumulators to prevent overflow during squaring.
// =============================================================================

func euclideanInt8Unrolled4x(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float32(a[i]) - float32(b[i])
		d1 := float32(a[i+1]) - float32(b[i+1])
		d2 := float32(a[i+2]) - float32(b[i+2])
		d3 := float32(a[i+3]) - float32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float32(a[i]) - float32(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func dotInt8Unrolled4x(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float32(a[i]) * float32(b[i])
		sum1 += float32(a[i+1]) * float32(b[i+1])
		sum2 += float32(a[i+2]) * float32(b[i+2])
		sum3 += float32(a[i+3]) * float32(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float32(a[i]) * float32(b[i])
	}
	return sum0 + sum1 + sum2 + sum3, nil
}

// ... Repeat for Int16, Int32, Int64 and Uint equivalents ...
// Note: Int64 might need float64 for better precision if values are huge.

func euclideanInt16Unrolled4x(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float32(a[i]) - float32(b[i])
		d1 := float32(a[i+1]) - float32(b[i+1])
		d2 := float32(a[i+2]) - float32(b[i+2])
		d3 := float32(a[i+3]) - float32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float32(a[i]) - float32(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func dotInt16Unrolled4x(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float32(a[i]) * float32(b[i])
		sum1 += float32(a[i+1]) * float32(b[i+1])
		sum2 += float32(a[i+2]) * float32(b[i+2])
		sum3 += float32(a[i+3]) * float32(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float32(a[i]) * float32(b[i])
	}
	return sum0 + sum1 + sum2 + sum3, nil
}

// Int32 Baseline
func euclideanInt32Unrolled4x(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float32(a[i]) - float32(b[i])
		d1 := float32(a[i+1]) - float32(b[i+1])
		d2 := float32(a[i+2]) - float32(b[i+2])
		d3 := float32(a[i+3]) - float32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float32(a[i]) - float32(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

// Int64 Baseline
func euclideanInt64Unrolled4x(a, b []int64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float64(a[i]) - float64(b[i])
		d1 := float64(a[i+1]) - float64(b[i+1])
		d2 := float64(a[i+2]) - float64(b[i+2])
		d3 := float64(a[i+3]) - float64(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
}

// Complex64 Baseline
func euclideanComplex64Unrolled(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]

		sum0 += real(d0)*real(d0) + imag(d0)*imag(d0)
		sum1 += real(d1)*real(d1) + imag(d1)*imag(d1)
		sum2 += real(d2)*real(d2) + imag(d2)*imag(d2)
		sum3 += real(d3)*real(d3) + imag(d3)*imag(d3)
	}
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += real(d)*real(d) + imag(d)*imag(d)
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func dotComplex64Unrolled(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var dot0, dot1, dot2, dot3 complex64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		dot0 += a[i] * b[i]
		dot1 += a[i+1] * b[i+1]
		dot2 += a[i+2] * b[i+2]
		dot3 += a[i+3] * b[i+3]
	}
	for ; i < n; i++ {
		dot0 += a[i] * b[i]
	}
	return real(dot0 + dot1 + dot2 + dot3), nil
}

// Uint Baseline (Mapping to float32 to prevent overflow)
func euclideanUint8Unrolled4x(a, b []uint8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float32(a[i]) - float32(b[i])
		d1 := float32(a[i+1]) - float32(b[i+1])
		d2 := float32(a[i+2]) - float32(b[i+2])
		d3 := float32(a[i+3]) - float32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float32(a[i]) - float32(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func euclideanUint16Unrolled4x(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float32(a[i]) - float32(b[i])
		d1 := float32(a[i+1]) - float32(b[i+1])
		d2 := float32(a[i+2]) - float32(b[i+2])
		d3 := float32(a[i+3]) - float32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float32(a[i]) - float32(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func euclideanUint32Unrolled4x(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float64(a[i]) - float64(b[i])
		d1 := float64(a[i+1]) - float64(b[i+1])
		d2 := float64(a[i+2]) - float64(b[i+2])
		d3 := float64(a[i+3]) - float64(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
}

func euclideanUint64Unrolled4x(a, b []uint64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := float64(a[i]) - float64(b[i])
		d1 := float64(a[i+1]) - float64(b[i+1])
		d2 := float64(a[i+2]) - float64(b[i+2])
		d3 := float64(a[i+3]) - float64(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		sum0 += d * d
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
}

// Complex128 Baseline
func euclideanComplex128Unrolled(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]

		sum0 += real(d0)*real(d0) + imag(d0)*imag(d0)
		sum1 += real(d1)*real(d1) + imag(d1)*imag(d1)
		sum2 += real(d2)*real(d2) + imag(d2)*imag(d2)
		sum3 += real(d3)*real(d3) + imag(d3)*imag(d3)
	}
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += real(d)*real(d) + imag(d)*imag(d)
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
}

// Float64 Kernels
func euclideanFloat64Unrolled4x(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += d * d
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
}

func dotFloat64Unrolled4x(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += a[i] * b[i]
		sum1 += a[i+1] * b[i+1]
		sum2 += a[i+2] * b[i+2]
		sum3 += a[i+3] * b[i+3]
	}
	for ; i < n; i++ {
		sum0 += a[i] * b[i] // FIXing potential bug here too, it should be a[i]*b[i]
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}
