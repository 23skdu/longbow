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

func dotInt32Unrolled4x(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 int64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += int64(a[i]) * int64(b[i])
		sum1 += int64(a[i+1]) * int64(b[i+1])
		sum2 += int64(a[i+2]) * int64(b[i+2])
		sum3 += int64(a[i+3]) * int64(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += int64(a[i]) * int64(b[i])
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
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
		sum0 += a[i] * b[i]
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func dotUint16Unrolled4x(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float64(a[i]) * float64(b[i])
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func dotUint32Unrolled4x(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float64(a[i]) * float64(b[i])
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func dotInt64Unrolled4x(a, b []int64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float64(a[i]) * float64(b[i])
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func dotUint64Unrolled4x(a, b []uint64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += float64(a[i]) * float64(b[i])
		sum1 += float64(a[i+1]) * float64(b[i+1])
		sum2 += float64(a[i+2]) * float64(b[i+2])
		sum3 += float64(a[i+3]) * float64(b[i+3])
	}
	for ; i < n; i++ {
		sum0 += float64(a[i]) * float64(b[i])
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func dotComplex128Unrolled(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var dotR0, dotR1, dotR2, dotR3 float64
	var dotI0, dotI1, dotI2, dotI3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		v0, v1, v2, v3 := a[i], a[i+1], a[i+2], a[i+3]
		w0, w1, w2, w3 := b[i], b[i+1], b[i+2], b[i+3]
		dotR0 += real(v0) * real(w0)
		dotR1 += real(v1) * real(w1)
		dotR2 += real(v2) * real(w2)
		dotR3 += real(v3) * real(w3)
		dotI0 += imag(v0) * imag(w0)
		dotI1 += imag(v1) * imag(w1)
		dotI2 += imag(v2) * imag(w2)
		dotI3 += imag(v3) * imag(w3)
	}
	for ; i < n; i++ {
		dotR0 += real(a[i]) * real(b[i])
		dotI0 += imag(a[i]) * imag(b[i])
	}
	return float32((dotR0 + dotR1 + dotR2 + dotR3) - (dotI0 + dotI1 + dotI2 + dotI3)), nil
}

// cosineFloat64Unrolled4x calculates cosine distance for Float64 vectors using generic implementation.
func cosineFloat64Unrolled4x(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}

	var dotSum, normASum, normBSum float64
	n := len(a)
	i := 0

	// Unrolled loop for better performance
	for ; i <= n-4; i += 4 {
		dotSum += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
		normASum += a[i]*a[i] + a[i+1]*a[i+1] + a[i+2]*a[i+2] + a[i+3]*a[i+3]
		normBSum += b[i]*b[i] + b[i+1]*b[i+1] + b[i+2]*b[i+2] + b[i+3]*b[i+3]
	}

	// Handle remaining elements
	for ; i < n; i++ {
		dotSum += a[i] * b[i]
		normASum += a[i] * a[i]
		normBSum += b[i] * b[i]
	}

	// Calculate cosine similarity
	if normASum == 0 || normBSum == 0 {
		return 1.0, nil // Cosine distance is 1 for zero vectors
	}

	similarity := dotSum / (math.Sqrt(normASum) * math.Sqrt(normBSum))
	// Clamp to [-1, 1] to handle numerical errors
	if similarity > 1.0 {
		similarity = 1.0
	} else if similarity < -1.0 {
		similarity = -1.0
	}

	// Cosine distance = 1 - similarity
	return float32(1.0 - similarity), nil
}
