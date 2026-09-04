package mathutil

import (
	"math"
	"sync/atomic"

	"github.com/emlgo/eml/pkg/arithmetic"
	"github.com/emlgo/eml/pkg/fastmath"
	"github.com/emlgo/eml/pkg/hyper"
	"github.com/emlgo/eml/pkg/logexp"
	"github.com/emlgo/eml/pkg/trig"
)

// Backend identifies which math kernel backend is active.
type Backend int32

const (
	// BackendStandard routes calculations to Go's standard "math" library.
	BackendStandard Backend = 0
	// BackendEML routes calculations to the high-performance EMLGo SIMD & fastmath library.
	BackendEML Backend = 1
)

var currentBackend int32 = int32(BackendEML)

// SetBackend changes the active math backend globally.
func SetBackend(b Backend) {
	atomic.StoreInt32(&currentBackend, int32(b))
}

// GetBackend returns the currently active math backend.
func GetBackend() Backend {
	return Backend(atomic.LoadInt32(&currentBackend))
}

// IsEML returns true if the EMLGo backend is active.
func IsEML() bool {
	return GetBackend() == BackendEML
}

// =============================================================================
// Scalar Operations
// =============================================================================

// Sqrt computes the square root of x.
func Sqrt(x float64) float64 {
	if IsEML() {
		return fastmath.Sqrt(x)
	}
	return math.Sqrt(x)
}

// FMA computes x*y + z in a single cycle with one rounding.
func FMA(x, y, z float64) float64 {
	if IsEML() {
		return fastmath.FMA(x, y, z)
	}
	return math.FMA(x, y, z)
}

// Exp computes e^x.
func Exp(x float64) float64 {
	if IsEML() {
		return fastmath.Exp(x)
	}
	return math.Exp(x)
}

// Log computes the natural logarithm ln(x).
func Log(x float64) float64 {
	if IsEML() {
		return logexp.Log(x)
	}
	return math.Log(x)
}

// Sin computes the sine of x.
func Sin(x float64) float64 {
	if IsEML() {
		return fastmath.Sin(x)
	}
	return math.Sin(x)
}

// Cos computes the cosine of x.
func Cos(x float64) float64 {
	if IsEML() {
		return fastmath.Cos(x)
	}
	return math.Cos(x)
}

// Tan computes the tangent of x.
func Tan(x float64) float64 {
	if IsEML() {
		return trig.Tan(x)
	}
	return math.Tan(x)
}

// Pow computes x^y.
func Pow(x, y float64) float64 {
	if IsEML() {
		return arithmetic.Pow(x, y)
	}
	return math.Pow(x, y)
}

// Sinh computes the hyperbolic sine of x.
func Sinh(x float64) float64 {
	if IsEML() {
		return hyper.Sinh(x)
	}
	return math.Sinh(x)
}

// Cosh computes the hyperbolic cosine of x.
func Cosh(x float64) float64 {
	if IsEML() {
		return hyper.Cosh(x)
	}
	return math.Cosh(x)
}

// Tanh computes the hyperbolic tangent of x.
func Tanh(x float64) float64 {
	if IsEML() {
		return hyper.Tanh(x)
	}
	return math.Tanh(x)
}

// Asin computes arcsine.
func Asin(x float64) float64 {
	if IsEML() {
		return trig.Asin(x)
	}
	return math.Asin(x)
}

// Acos computes arccosine.
func Acos(x float64) float64 {
	if IsEML() {
		return trig.Acos(x)
	}
	return math.Acos(x)
}

// Atan computes arctangent.
func Atan(x float64) float64 {
	if IsEML() {
		return trig.Atan(x)
	}
	return math.Atan(x)
}

// =============================================================================
// Vector / Batch Operations (Float64)
// =============================================================================

// ExpBatch computes element-wise exp for float64 slices.
func ExpBatch(x []float64) []float64 {
	if IsEML() {
		return logexp.ExpBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Exp(v)
	}
	return res
}

// LogBatch computes element-wise natural log for float64 slices.
func LogBatch(x []float64) []float64 {
	if IsEML() {
		return logexp.LogBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Log(v)
	}
	return res
}

// SinBatch computes element-wise sin for float64 slices.
func SinBatch(x []float64) []float64 {
	if IsEML() {
		return trig.SinBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sin(v)
	}
	return res
}

// CosBatch computes element-wise cos for float64 slices.
func CosBatch(x []float64) []float64 {
	if IsEML() {
		return trig.CosBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cos(v)
	}
	return res
}

// TanBatch computes element-wise tan for float64 slices.
func TanBatch(x []float64) []float64 {
	if IsEML() {
		return trig.TanBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tan(v)
	}
	return res
}

// SinhBatch computes element-wise sinh for float64 slices.
func SinhBatch(x []float64) []float64 {
	if IsEML() {
		return hyper.SinhBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Sinh(v)
	}
	return res
}

// CoshBatch computes element-wise cosh for float64 slices.
func CoshBatch(x []float64) []float64 {
	if IsEML() {
		return hyper.CoshBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Cosh(v)
	}
	return res
}

// TanhBatch computes element-wise tanh for float64 slices.
func TanhBatch(x []float64) []float64 {
	if IsEML() {
		return hyper.TanhBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Tanh(v)
	}
	return res
}

// SqrtBatch computes element-wise sqrt for float64 slices.
func SqrtBatch(x []float64) []float64 {
	res := make([]float64, len(x))
	if IsEML() {
		for i, v := range x {
			res[i] = fastmath.Sqrt(v)
		}
		return res
	}
	for i, v := range x {
		res[i] = math.Sqrt(v)
	}
	return res
}

// AddBatch computes element-wise a + b for float64 slices.
func AddBatch(a, b []float64) []float64 {
	if IsEML() {
		return arithmetic.AddBatch(a, b)
	}
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

// SubBatch computes element-wise a - b for float64 slices.
func SubBatch(a, b []float64) []float64 {
	if IsEML() {
		return arithmetic.SubBatch(a, b)
	}
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] - b[i]
	}
	return res
}

// MulBatch computes element-wise a * b for float64 slices.
func MulBatch(a, b []float64) []float64 {
	if IsEML() {
		return arithmetic.MulBatch(a, b)
	}
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}

// DivBatch computes element-wise a / b for float64 slices.
func DivBatch(a, b []float64) []float64 {
	if IsEML() {
		return arithmetic.DivBatch(a, b)
	}
	n := min(len(a), len(b))
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = a[i] / b[i]
	}
	return res
}

// NegBatch computes element-wise -x for float64 slices.
func NegBatch(x []float64) []float64 {
	if IsEML() {
		return arithmetic.NegBatch(x)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = -v
	}
	return res
}

// PowBatch computes element-wise x^y for float64 slices.
func PowBatch(x []float64, y float64) []float64 {
	if IsEML() {
		return arithmetic.PowBatch(x, y)
	}
	res := make([]float64, len(x))
	for i, v := range x {
		res[i] = math.Pow(v, y)
	}
	return res
}

// =============================================================================
// Vector / Batch Operations (Float32)
// =============================================================================

// ExpBatchF32 computes element-wise exp for float32 slices using SIMD when EML is enabled.
func ExpBatchF32(x []float32) []float32 {
	n := len(x)
	res := make([]float32, n)
	if IsEML() && n >= 64 {
		// Convert to f64, batch compute, convert back
		f64Buf := make([]float64, n)
		for i, v := range x {
			f64Buf[i] = float64(v)
		}
		out64 := logexp.ExpBatch(f64Buf)
		for i, v := range out64 {
			res[i] = float32(v)
		}
		return res
	}
	for i, v := range x {
		res[i] = float32(Exp(float64(v)))
	}
	return res
}

// SinBatchF32 computes element-wise sin for float32 slices.
func SinBatchF32(x []float32) []float32 {
	n := len(x)
	res := make([]float32, n)
	if IsEML() && n >= 64 {
		f64Buf := make([]float64, n)
		for i, v := range x {
			f64Buf[i] = float64(v)
		}
		out64 := trig.SinBatch(f64Buf)
		for i, v := range out64 {
			res[i] = float32(v)
		}
		return res
	}
	for i, v := range x {
		res[i] = float32(Sin(float64(v)))
	}
	return res
}

// CosBatchF32 computes element-wise cos for float32 slices.
func CosBatchF32(x []float32) []float32 {
	n := len(x)
	res := make([]float32, n)
	if IsEML() && n >= 64 {
		f64Buf := make([]float64, n)
		for i, v := range x {
			f64Buf[i] = float64(v)
		}
		out64 := trig.CosBatch(f64Buf)
		for i, v := range out64 {
			res[i] = float32(v)
		}
		return res
	}
	for i, v := range x {
		res[i] = float32(Cos(float64(v)))
	}
	return res
}

// AddBatchF32 computes element-wise a + b for float32 slices.
func AddBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	if IsEML() && n >= 128 {
		f64A := make([]float64, n)
		f64B := make([]float64, n)
		for i := 0; i < n; i++ {
			f64A[i] = float64(a[i])
			f64B[i] = float64(b[i])
		}
		out64 := arithmetic.AddBatch(f64A, f64B)
		for i, v := range out64 {
			res[i] = float32(v)
		}
		return res
	}
	for i := 0; i < n; i++ {
		res[i] = a[i] + b[i]
	}
	return res
}

// MulBatchF32 computes element-wise a * b for float32 slices.
func MulBatchF32(a, b []float32) []float32 {
	n := min(len(a), len(b))
	res := make([]float32, n)
	if IsEML() && n >= 128 {
		f64A := make([]float64, n)
		f64B := make([]float64, n)
		for i := 0; i < n; i++ {
			f64A[i] = float64(a[i])
			f64B[i] = float64(b[i])
		}
		out64 := arithmetic.MulBatch(f64A, f64B)
		for i, v := range out64 {
			res[i] = float32(v)
		}
		return res
	}
	for i := 0; i < n; i++ {
		res[i] = a[i] * b[i]
	}
	return res
}
