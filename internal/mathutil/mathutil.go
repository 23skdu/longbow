//go:build !emlgo

package mathutil

import (
	"math"
	"sync/atomic"
)

// Backend identifies which math kernel backend is active.
type Backend int32

const (
	// BackendStandard routes calculations to Go's standard "math" library.
	BackendStandard Backend = 0
	// BackendEML routes calculations to the high-performance EMLGo SIMD & fastmath library.
	BackendEML Backend = 1
)

var currentBackend int32 = int32(BackendStandard)

// Part 7: float64 exclusion stub — no-op without emlgo build tag
func SetFloat64Excluded(excluded bool) {}
func IsFloat64Excluded() bool          { return false }

// SetBackend changes the active math backend globally.
// Without the "emlgo" build tag, switching to BackendEML is a no-op.
func SetBackend(b Backend) {
	if b != BackendEML {
		atomic.StoreInt32(&currentBackend, int32(b))
	}
}

// GetBackend returns the currently active math backend.
func GetBackend() Backend {
	return Backend(atomic.LoadInt32(&currentBackend))
}

// IsEML returns true if the EMLGo backend is active.
// Without the "emlgo" build tag, this always returns false.
func IsEML() bool {
	return false
}

// =============================================================================
// Scalar Operations (standard math)
// =============================================================================

func Sqrt(x float64) float64             { return math.Sqrt(x) }
func FMA(x, y, z float64) float64        { return math.FMA(x, y, z) }
func Exp(x float64) float64              { return math.Exp(x) }
func Log(x float64) float64              { return math.Log(x) }
func Sin(x float64) float64              { return math.Sin(x) }
func Cos(x float64) float64              { return math.Cos(x) }
func Tan(x float64) float64              { return math.Tan(x) }
func Pow(x, y float64) float64           { return math.Pow(x, y) }
func Sinh(x float64) float64             { return math.Sinh(x) }
func Cosh(x float64) float64             { return math.Cosh(x) }
func Tanh(x float64) float64             { return math.Tanh(x) }
func Asin(x float64) float64             { return math.Asin(x) }
func Acos(x float64) float64             { return math.Acos(x) }
func Atan(x float64) float64             { return math.Atan(x) }

// =============================================================================
// Vector / Batch Operations (Float64) — standard math loops
// =============================================================================

func ExpBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Exp(v)
	}
	return res
}

func LogBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Log(v)
	}
	return res
}

func SinBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sin(v)
	}
	return res
}

func CosBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cos(v)
	}
	return res
}

func TanBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tan(v)
	}
	return res
}

func SinhBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sinh(v)
	}
	return res
}

func CoshBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cosh(v)
	}
	return res
}

func TanhBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tanh(v)
	}
	return res
}

func SqrtBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sqrt(v)
	}
	return res
}

func AddBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

func SubBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] - b[i]
	}
	return res
}

func MulBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}

func DivBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] / b[i]
	}
	return res
}

func NegBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = -v
	}
	return res
}

func PowBatch(x []float64, y float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Pow(v, y)
	}
	return res
}

// =============================================================================
// Vector / Batch Operations (Float32) — standard math loops
// =============================================================================

func ExpBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Exp(float64(v)))
	}
	return res
}

func SinBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Sin(float64(v)))
	}
	return res
}

func CosBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Cos(float64(v)))
	}
	return res
}

func AddBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

func MulBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}
