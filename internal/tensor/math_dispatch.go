//go:build !emlgo

package tensor

import (
	"math"

	"github.com/23skdu/longbow/internal/mathutil"
)

// MathImpl identifies which math kernel implementation to use.
type MathImpl int

const (
	// MathGo is a math go constant.
	MathGo MathImpl = iota // pure Go fallback (Taylor series, Newton)
	// MathSIMD is a math simd constant.
	MathSIMD // Standard SIMD-accelerated kernels wrapping math.*
	// MathEML is a math eml constant.
	MathEML // High-performance EMLGo SIMD & fastmath (no-op without emlgo tag)
)

var mathImpl MathImpl = MathSIMD

// GetMathImpl returns the active math implementation.
func GetMathImpl() MathImpl {
	return mathImpl
}

// SetMathImpl switches between MathGo, MathSIMD, and MathEML.
// Without the "emlgo" build tag, MathEML falls through to MathSIMD.
func SetMathImpl(impl MathImpl) {
	mathImpl = impl
	switch impl {
	case MathEML:
		mathutil.SetBackend(mathutil.BackendStandard)
		sin = math.Sin
		cos = math.Cos
		tan = math.Tan
		exp = math.Exp
		log = math.Log
		sqrt = math.Sqrt
		pow = math.Pow
		sinh = math.Sinh
		cosh = math.Cosh
		tanh = math.Tanh
		asin = math.Asin
		acos = math.Acos
		atan = math.Atan
	case MathSIMD:
		mathutil.SetBackend(mathutil.BackendStandard)
		sin = math.Sin
		cos = math.Cos
		tan = math.Tan
		exp = math.Exp
		log = math.Log
		sqrt = math.Sqrt
		pow = math.Pow
		sinh = math.Sinh
		cosh = math.Cosh
		tanh = math.Tanh
		asin = math.Asin
		acos = math.Acos
		atan = math.Atan
	case MathGo:
		mathutil.SetBackend(mathutil.BackendStandard)
		sin = func(x float64) float64 { v, _ := sinGo(x); return v }
		cos = func(x float64) float64 { v, _ := cosGo(x); return v }
		tan = func(x float64) float64 { v, _ := tanGo(x); return v }
		exp = func(x float64) float64 { v, _ := expGo(x); return v }
		log = func(x float64) float64 { v, _ := logGo(x); return v }
		sqrt = func(x float64) float64 { v, _ := sqrtGo(x); return v }
		pow = func(x, y float64) float64 { v, _ := powGo(x, y); return v }
		sinh = func(x float64) float64 { e, _ := expGo(x); ne, _ := expGo(-x); return (e - ne) / 2 }
		cosh = func(x float64) float64 { e, _ := expGo(x); ne, _ := expGo(-x); return (e + ne) / 2 }
		tanh = func(x float64) float64 { e, _ := expGo(x); ne, _ := expGo(-x); return (e - ne) / (e + ne) }
		asin = asinGo
		acos = acosGo
		atan = atanGo
	}
}

// InitMathDispatch selects the best available math kernel implementation.
// Call once at startup after CPU feature detection.
// Without the "emlgo" build tag, SIMD mode defaults to standard math.
func InitMathDispatch(useSIMD bool) {
	if !useSIMD {
		SetMathImpl(MathGo)
		return
	}
	SetMathImpl(MathSIMD)
}

func init() {
	SetMathImpl(MathSIMD)
}
