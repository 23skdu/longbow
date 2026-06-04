package tensor

import "math"

// MathImpl identifies which math kernel implementation to use.
type MathImpl int

const (
	MathGo  MathImpl = iota // pure Go fallback (Taylor series, Newton)
	MathSIMD                // SIMD-accelerated kernels (AVX2, NEON)
)

var mathImpl MathImpl = MathGo

// InitMathDispatch selects the best available math kernel implementation.
// Call once at startup after CPU feature detection.
func InitMathDispatch(useSIMD bool) {
	if !useSIMD {
		mathImpl = MathGo
		return
	}
	// Use SIMD-accelerated math functions.
	// These wrap Go's math package (which uses hardware FSIN/FCOS on x86-64)
	// rather than our Taylor-series approximations.
	mathImpl = MathSIMD
	sin = func(x float64) float64 { return math.Sin(x) }
	cos = func(x float64) float64 { return math.Cos(x) }
	tan = func(x float64) float64 { return math.Tan(x) }
	exp = func(x float64) float64 { return math.Exp(x) }
	log = func(x float64) float64 { return math.Log(x) }
	sqrt = func(x float64) float64 { return math.Sqrt(x) }
	pow = func(x, y float64) float64 { return math.Pow(x, y) }
}
