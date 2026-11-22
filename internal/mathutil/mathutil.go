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

// SetFloat64Excluded 7: float64 exclusion stub — no-op without emlgo build tag
func SetFloat64Excluded(excluded bool) {}

// IsFloat64Excluded reports whether the condition holds.
func IsFloat64Excluded() bool { return false }

// PushStandard is a no-op without the emlgo build tag (backend is always standard).
func PushStandard() (restore func()) {
	return func() {}
}

// IsForceStandard is always false without the emlgo build tag.
func IsForceStandard() bool { return false }

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

// Sqrt runs the sqrt operation.
func Sqrt(x float64) float64 { return math.Sqrt(x) }

// FMA runs the fma operation.
func FMA(x, y, z float64) float64 { return math.FMA(x, y, z) }

// Exp runs the exp operation.
func Exp(x float64) float64 { return math.Exp(x) }

// Log runs the log operation.
func Log(x float64) float64 { return math.Log(x) }

// Sin runs the sin operation.
func Sin(x float64) float64 { return math.Sin(x) }

// Cos runs the cos operation.
func Cos(x float64) float64 { return math.Cos(x) }

// Tan runs the tan operation.
func Tan(x float64) float64 { return math.Tan(x) }

// Pow runs the pow operation.
func Pow(x, y float64) float64 { return math.Pow(x, y) }

// Sinh runs the sinh operation.
func Sinh(x float64) float64 { return math.Sinh(x) }

// Cosh runs the cosh operation.
func Cosh(x float64) float64 { return math.Cosh(x) }

// Tanh runs the tanh operation.
func Tanh(x float64) float64 { return math.Tanh(x) }

// Asin runs the asin operation.
func Asin(x float64) float64 { return math.Asin(x) }

// Acos runs the acos operation.
func Acos(x float64) float64 { return math.Acos(x) }

// Atan runs the atan operation.
func Atan(x float64) float64 { return math.Atan(x) }

// =============================================================================
// Vector / Batch Operations (Float64) — standard math loops
// =============================================================================

// ExpBatch runs the exp batch operation.
func ExpBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Exp(v)
	}
	return res
}

// LogBatch runs the log batch operation.
func LogBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Log(v)
	}
	return res
}

// SinBatch runs the sin batch operation.
func SinBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sin(v)
	}
	return res
}

// CosBatch runs the cos batch operation.
func CosBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cos(v)
	}
	return res
}

// TanBatch runs the tan batch operation.
func TanBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tan(v)
	}
	return res
}

// SinhBatch runs the sinh batch operation.
func SinhBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sinh(v)
	}
	return res
}

// CoshBatch runs the cosh batch operation.
func CoshBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cosh(v)
	}
	return res
}

// TanhBatch runs the tanh batch operation.
func TanhBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tanh(v)
	}
	return res
}

// SqrtBatch runs the sqrt batch operation.
func SqrtBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sqrt(v)
	}
	return res
}

// AddBatch runs the add batch operation.
func AddBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

// SubBatch runs the sub batch operation.
func SubBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] - b[i]
	}
	return res
}

// MulBatch runs the mul batch operation.
func MulBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}

// DivBatch runs the div batch operation.
func DivBatch(a, b []float64) []float64 {
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] / b[i]
	}
	return res
}

// NegBatch runs the neg batch operation.
func NegBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = -v
	}
	return res
}

// PowBatch runs the pow batch operation.
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

// ExpBatchF32 runs the exp batch f32 operation.
func ExpBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Exp(float64(v)))
	}
	return res
}

// SinBatchF32 runs the sin batch f32 operation.
func SinBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Sin(float64(v)))
	}
	return res
}

// CosBatchF32 runs the cos batch f32 operation.
func CosBatchF32(x []float32) []float32 {
	res := make([]float32, len(x))
	for i, v := range x {
		res[i] = float32(math.Cos(float64(v)))
	}
	return res
}

// AddBatchF32 runs the add batch f32 operation.
func AddBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

// MulBatchF32 runs the mul batch f32 operation.
func MulBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}
