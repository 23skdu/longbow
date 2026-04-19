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

func dotInt8Unrolled4x(a, b []int8) (float32, error) {
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

// ... Repeat for Int16, Int32, Int64 and Uint equivalents ...
// Note: Int64 might need float64 for better precision if values are huge.

func euclideanInt16Unrolled4x(a, b []int16) (float32, error) {
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

func dotInt16Unrolled4x(a, b []int16) (float32, error) {
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

// Int32 Baseline
func euclideanInt32Unrolled4x(a, b []int32) (float32, error) {
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

func dotInt32Unrolled4x(a, b []int32) (float32, error) {
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
	var sum0, sum1, sum2, sum3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0, d1, d2, d3 := a[i]-b[i], a[i+1]-b[i+1], a[i+2]-b[i+2], a[i+3]-b[i+3]
		sum0 += float64(real(d0)*real(d0) + imag(d0)*imag(d0))
		sum1 += float64(real(d1)*real(d1) + imag(d1)*imag(d1))
		sum2 += float64(real(d2)*real(d2) + imag(d2)*imag(d2))
		sum3 += float64(real(d3)*real(d3) + imag(d3)*imag(d3))
	}
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += float64(real(d)*real(d) + imag(d)*imag(d))
	}
	return float32(math.Sqrt(sum0 + sum1 + sum2 + sum3)), nil
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

func euclideanUint16Unrolled4x(a, b []uint16) (float32, error) {
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
func dotUint8Unrolled4x(a, b []uint8) (float32, error) {
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

// =============================================================================
// Generic Cosine Distance Helper
// =============================================================================

func cosineDistanceInt8Unrolled4x(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot += va0*vb0 + va1*vb1 + va2*vb2 + va3*vb3
		normA += va0*va0 + va1*va1 + va2*va2 + va3*va3
		normB += vb0*vb0 + vb1*vb1 + vb2*vb2 + vb3*vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
}

func cosineDistanceUint8Unrolled4x(a, b []uint8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot += va0*vb0 + va1*vb1 + va2*vb2 + va3*vb3
		normA += va0*va0 + va1*va1 + va2*va2 + va3*va3
		normB += vb0*vb0 + vb1*vb1 + vb2*vb2 + vb3*vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
}

func cosineDistanceInt16Unrolled4x(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineDistanceUint16Unrolled4x(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineDistanceInt32Unrolled4x(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineDistanceUint32Unrolled4x(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineDistanceInt64Unrolled4x(a, b []int64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineDistanceUint64Unrolled4x(a, b []uint64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot, normA, normB float64
	for i := range a {
		va, vb := float64(a[i]), float64(b[i])
		dot += va * vb
		normA += va * va
		normB += vb * vb
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineComplex64Unrolled(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dotR, dotI, normA, normB float64
	for i := range a {
		va_r, va_i := float64(real(a[i])), float64(imag(a[i]))
		vb_r, vb_i := float64(real(b[i])), float64(imag(b[i]))
		
		// dot(a, b) = sum(a[i] * conj(b[i]))
		// (ar + i*ai) * (br - i*bi) = (ar*br + ai*bi) + i*(ai*br - ar*bi)
		dotR += va_r*vb_r + va_i*vb_i
		dotI += va_i*vb_r - va_r*vb_i
		
		normA += va_r*va_r + va_i*va_i
		normB += vb_r*vb_r + vb_i*vb_i
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	// We use the real part for cosine similarity in most vdb contexts
	similarity := dotR / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

func cosineComplex128Unrolled(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dotR, dotI, normA, normB float64
	for i := range a {
		va_r, va_i := real(a[i]), imag(a[i])
		vb_r, vb_i := real(b[i]), imag(b[i])
		
		dotR += va_r*vb_r + va_i*vb_i
		dotI += va_i*vb_r - va_r*vb_i
		
		normA += va_r*va_r + va_i*va_i
		normB += vb_r*vb_r + vb_i*vb_i
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dotR / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}
