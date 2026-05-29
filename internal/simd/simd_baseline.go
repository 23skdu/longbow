package simd

import (
	"errors"
	"math"
	"runtime"
	"sync"

	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow/float16"
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
func l2SquaredInt8Unrolled4x(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 int32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := int32(a[i]) - int32(b[i])
		d1 := int32(a[i+1]) - int32(b[i+1])
		d2 := int32(a[i+2]) - int32(b[i+2])
		d3 := int32(a[i+3]) - int32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum0 += d * d
	}
	return float32(sum0 + sum1 + sum2 + sum3), nil
}

func l2SquaredUint8Unrolled4x(a, b []uint8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 int32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := int32(a[i]) - int32(b[i])
		d1 := int32(a[i+1]) - int32(b[i+1])
		d2 := int32(a[i+2]) - int32(b[i+2])
		d3 := int32(a[i+3]) - int32(b[i+3])
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum0 += d * d
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

func l2SquaredFloat64Unrolled4x(a, b []float64) (float32, error) {
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
	return float32(sum0 + sum1 + sum2 + sum3), nil
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
	var dot0, dot1, dot2, dot3 float64
	var normA0, normA1, normA2, normA3 float64
	var normB0, normB1, normB2, normB3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot0 += va0 * vb0
		normA0 += va0 * va0
		normB0 += vb0 * vb0
		dot1 += va1 * vb1
		normA1 += va1 * va1
		normB1 += vb1 * vb1
		dot2 += va2 * vb2
		normA2 += va2 * va2
		normB2 += vb2 * vb2
		dot3 += va3 * vb3
		normA3 += va3 * va3
		normB3 += vb3 * vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot0 += va * vb
		normA0 += va * va
		normB0 += vb * vb
	}
	totalDot := dot0 + dot1 + dot2 + dot3
	totalNormA := normA0 + normA1 + normA2 + normA3
	totalNormB := normB0 + normB1 + normB2 + normB3
	if totalNormA <= 0 || totalNormB <= 0 {
		return 1.0, nil
	}
	similarity := totalDot / (math.Sqrt(totalNormA) * math.Sqrt(totalNormB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
}

func cosineDistanceUint16Unrolled4x(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot0, dot1, dot2, dot3 float64
	var normA0, normA1, normA2, normA3 float64
	var normB0, normB1, normB2, normB3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot0 += va0 * vb0
		normA0 += va0 * va0
		normB0 += vb0 * vb0
		dot1 += va1 * vb1
		normA1 += va1 * va1
		normB1 += vb1 * vb1
		dot2 += va2 * vb2
		normA2 += va2 * va2
		normB2 += vb2 * vb2
		dot3 += va3 * vb3
		normA3 += va3 * va3
		normB3 += vb3 * vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot0 += va * vb
		normA0 += va * va
		normB0 += vb * vb
	}
	totalDot := dot0 + dot1 + dot2 + dot3
	totalNormA := normA0 + normA1 + normA2 + normA3
	totalNormB := normB0 + normB1 + normB2 + normB3
	if totalNormA <= 0 || totalNormB <= 0 {
		return 1.0, nil
	}
	similarity := totalDot / (math.Sqrt(totalNormA) * math.Sqrt(totalNormB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
}

func cosineDistanceInt32Unrolled4x(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	// Use float64 accumulators: int32 products can exceed int64 range at large dims.
	var dot0, dot1, dot2, dot3 float64
	var normA0, normA1, normA2, normA3 float64
	var normB0, normB1, normB2, normB3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot0 += va0 * vb0
		normA0 += va0 * va0
		normB0 += vb0 * vb0
		dot1 += va1 * vb1
		normA1 += va1 * va1
		normB1 += vb1 * vb1
		dot2 += va2 * vb2
		normA2 += va2 * va2
		normB2 += vb2 * vb2
		dot3 += va3 * vb3
		normA3 += va3 * va3
		normB3 += vb3 * vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot0 += va * vb
		normA0 += va * va
		normB0 += vb * vb
	}
	totalDot := dot0 + dot1 + dot2 + dot3
	totalNormA := normA0 + normA1 + normA2 + normA3
	totalNormB := normB0 + normB1 + normB2 + normB3
	if totalNormA <= 0 || totalNormB <= 0 {
		return 1.0, nil
	}
	similarity := totalDot / (math.Sqrt(totalNormA) * math.Sqrt(totalNormB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
}

func cosineDistanceUint32Unrolled4x(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	var dot0, dot1, dot2, dot3 float64
	var normA0, normA1, normA2, normA3 float64
	var normB0, normB1, normB2, normB3 float64
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, vb0 := float64(a[i]), float64(b[i])
		va1, vb1 := float64(a[i+1]), float64(b[i+1])
		va2, vb2 := float64(a[i+2]), float64(b[i+2])
		va3, vb3 := float64(a[i+3]), float64(b[i+3])
		dot0 += va0 * vb0
		normA0 += va0 * va0
		normB0 += vb0 * vb0
		dot1 += va1 * vb1
		normA1 += va1 * va1
		normB1 += vb1 * vb1
		dot2 += va2 * vb2
		normA2 += va2 * va2
		normB2 += vb2 * vb2
		dot3 += va3 * vb3
		normA3 += va3 * va3
		normB3 += vb3 * vb3
	}
	for ; i < n; i++ {
		va, vb := float64(a[i]), float64(b[i])
		dot0 += va * vb
		normA0 += va * va
		normB0 += vb * vb
	}
	totalDot := dot0 + dot1 + dot2 + dot3
	totalNormA := normA0 + normA1 + normA2 + normA3
	totalNormB := normB0 + normB1 + normB2 + normB3
	if totalNormA <= 0 || totalNormB <= 0 {
		return 1.0, nil
	}
	similarity := totalDot / (math.Sqrt(totalNormA) * math.Sqrt(totalNormB))
	return float32(math.Max(0, math.Min(2, 1.0-similarity))), nil
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
		vaR, vaI := float64(real(a[i])), float64(imag(a[i]))
		vbR, vbI := float64(real(b[i])), float64(imag(b[i]))

		// dot(a, b) = sum(a[i] * conj(b[i]))
		// (ar + i*ai) * (br - i*bi) = (ar*br + ai*bi) + i*(ai*br - ar*bi)
		dotR += vaR*vbR + vaI*vbI
		dotI += vaI*vbR - vaR*vbI

		normA += vaR*vaR + vaI*vaI
		normB += vbR*vbR + vbI*vbI
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
		vaR, vaI := real(a[i]), imag(a[i])
		vbR, vbI := real(b[i]), imag(b[i])

		dotR += vaR*vbR + vaI*vbI
		dotI += vaI*vbR - vaR*vbI

		normA += vaR*vaR + vaI*vaI
		normB += vbR*vbR + vbI*vbI
	}
	if normA <= 0 || normB <= 0 {
		return 1.0, nil
	}
	similarity := dotR / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1.0 - similarity), nil
}

// ManhattanDistanceFloat32 calculates the L1 distance between two float32 vectors.
func ManhattanDistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		if d < 0 {
			sum -= d
		} else {
			sum += d
		}
	}
	return sum, nil
}

// ChebyshevDistanceFloat32 calculates the L-infinity distance between two float32 vectors.
func ChebyshevDistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	var max float32
	for i := range a {
		d := a[i] - b[i]
		if d < 0 {
			d = -d
		}
		if d > max {
			max = d
		}
	}
	return max, nil
}

// BrayCurtisDistanceFloat32 calculates the Bray-Curtis distance between two float32 vectors.
func BrayCurtisDistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sumAbsDiff, sumAbsTotal float32
	for i := range a {
		d := a[i] - b[i]
		if d < 0 {
			sumAbsDiff -= d
		} else {
			sumAbsDiff += d
		}
		s := a[i] + b[i]
		if s < 0 {
			sumAbsTotal -= s
		} else {
			sumAbsTotal += s
		}
	}
	if sumAbsTotal == 0 {
		return 0, nil
	}
	return sumAbsDiff / sumAbsTotal, nil
}

// ManhattanDistanceF16 calculates the L1 distance between two float16 vectors.
func ManhattanDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum float32
	for i := range a {
		d := a[i].Float32() - b[i].Float32()
		if d < 0 {
			sum -= d
		} else {
			sum += d
		}
	}
	return sum, nil
}

// ChebyshevDistanceF16 calculates the L-infinity distance between two float16 vectors.
func ChebyshevDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	var max float32
	for i := range a {
		d := a[i].Float32() - b[i].Float32()
		if d < 0 {
			d = -d
		}
		if d > max {
			max = d
		}
	}
	return max, nil
}

// BrayCurtisDistanceF16 calculates the Bray-Curtis distance between two float16 vectors.
func BrayCurtisDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sumAbsDiff, sumAbsTotal float32
	for i := range a {
		va, vb := a[i].Float32(), b[i].Float32()
		d := va - vb
		if d < 0 {
			sumAbsDiff -= d
		} else {
			sumAbsDiff += d
		}
		s := va + vb
		if s < 0 {
			sumAbsTotal -= s
		} else {
			sumAbsTotal += s
		}
	}
	if sumAbsTotal == 0 {
		return 0, nil
	}
	return sumAbsDiff / sumAbsTotal, nil
}

// AccumulateWeightedScatterFloat32 adds weighted values to a destination slice using scatter indices.
// dst[targets[i]] += weights[i] * factor
func AccumulateWeightedScatterFloat32(dst []float32, targets []uint32, weights []float32, factor float32) {
	n := len(targets)
	if len(weights) < n {
		n = len(weights)
	}

	// Unrolled 4x for better performance
	i := 0
	for ; i <= n-4; i += 4 {
		t0, t1, t2, t3 := targets[i], targets[i+1], targets[i+2], targets[i+3]
		w0, w1, w2, w3 := weights[i], weights[i+1], weights[i+2], weights[i+3]

		dst[t0] += w0 * factor
		dst[t1] += w1 * factor
		dst[t2] += w2 * factor
		dst[t3] += w3 * factor
	}

	for ; i < n; i++ {
		dst[targets[i]] += weights[i] * factor
	}
}

func sinFloat32Generic(src, dst []float32) {
	for i, v := range src {
		dst[i] = float32(math.Sin(float64(v)))
	}
}

func cosFloat32Generic(src, dst []float32) {
	for i, v := range src {
		dst[i] = float32(math.Cos(float64(v)))
	}
}

func atan2Float32Generic(y, x, dst []float32) {
	for i := range y {
		dst[i] = float32(math.Atan2(float64(y[i]), float64(x[i])))
	}
}

func haversineBatchGeneric(centerLat, centerLon float64, points []lbcore.GeoPoint, earthRadius float64, results []float32) {
	lat1 := centerLat * math.Pi / 180.0
	lon1 := centerLon * math.Pi / 180.0
	cosLat1 := math.Cos(lat1)

	// Parallelize for large batches
	if len(points) < 1024 {
		for i, p := range points {
			lat2 := p.Lat * math.Pi / 180.0
			lon2 := p.Lon * math.Pi / 180.0
			dLat := lat2 - lat1
			dLon := lon2 - lon1
			a := math.Sin(dLat/2)*math.Sin(dLat/2) + cosLat1*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
			c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
			results[i] = float32(earthRadius * c)
		}
		return
	}

	numCPUs := runtime.NumCPU()
	if numCPUs < 1 {
		numCPUs = 1
	}
	chunkSize := (len(points) + numCPUs - 1) / numCPUs

	var wg sync.WaitGroup
	for i := 0; i < len(points); i += chunkSize {
		start := i
		end := i + chunkSize
		if end > len(points) {
			end = len(points)
		}

		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for j := s; j < e; j++ {
				p := points[j]
				lat2 := p.Lat * math.Pi / 180.0
				lon2 := p.Lon * math.Pi / 180.0
				dLat := lat2 - lat1
				dLon := lon2 - lon1
				a := math.Sin(dLat/2)*math.Sin(dLat/2) + cosLat1*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
				c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
				results[j] = float32(earthRadius * c)
			}
		}(start, end)
	}
	wg.Wait()
}
