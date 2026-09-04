package tensor

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/mathutil"
)

// =============================================================================
// Numerical Parity & Accuracy Validation
// =============================================================================

func TestEmlgoParity_Scalars(t *testing.T) {
	testValues := []float64{
		0.001, 0.01, 0.1, 0.5, 1.0, 1.5, 2.0, 3.1415926535, 5.0, 10.0, 50.0, 100.0,
	}

	for _, x := range testValues {
		// Sqrt
		stdSqrt := math.Sqrt(x)
		emlSqrt := mathutil.Sqrt(x)
		if math.Abs(stdSqrt-emlSqrt) > 1e-12 {
			t.Errorf("Sqrt(%f): std=%v, eml=%v, diff=%v", x, stdSqrt, emlSqrt, math.Abs(stdSqrt-emlSqrt))
		}

		// Sin
		stdSin := math.Sin(x)
		emlSin := mathutil.Sin(x)
		if math.Abs(stdSin-emlSin) > 1e-6 {
			t.Errorf("Sin(%f): std=%v, eml=%v, diff=%v", x, stdSin, emlSin, math.Abs(stdSin-emlSin))
		}

		// Cos
		stdCos := math.Cos(x)
		emlCos := mathutil.Cos(x)
		if math.Abs(stdCos-emlCos) > 1e-6 {
			t.Errorf("Cos(%f): std=%v, eml=%v, diff=%v", x, stdCos, emlCos, math.Abs(stdCos-emlCos))
		}

		// Exp (for moderate values)
		if x <= 50.0 {
			stdExp := math.Exp(x)
			emlExp := mathutil.Exp(x)
			relErr := math.Abs(stdExp-emlExp) / stdExp
			if relErr > 1e-5 {
				t.Errorf("Exp(%f): std=%v, eml=%v, relErr=%v", x, stdExp, emlExp, relErr)
			}
		}

		// Log
		stdLog := math.Log(x)
		emlLog := mathutil.Log(x)
		if math.Abs(stdLog-emlLog) > 1e-6 {
			t.Errorf("Log(%f): std=%v, eml=%v, diff=%v", x, stdLog, emlLog, math.Abs(stdLog-emlLog))
		}

		// Sinh / Cosh / Tanh
		if x <= 20.0 {
			stdSinh := math.Sinh(x)
			emlSinh := mathutil.Sinh(x)
			relErr := math.Abs(stdSinh-emlSinh) / stdSinh
			if relErr > 1e-6 {
				t.Errorf("Sinh(%f): std=%v, eml=%v, relErr=%v", x, stdSinh, emlSinh, relErr)
			}

			stdCosh := math.Cosh(x)
			emlCosh := mathutil.Cosh(x)
			relErr = math.Abs(stdCosh-emlCosh) / stdCosh
			if relErr > 1e-6 {
				t.Errorf("Cosh(%f): std=%v, eml=%v, relErr=%v", x, stdCosh, emlCosh, relErr)
			}

			stdTanh := math.Tanh(x)
			emlTanh := mathutil.Tanh(x)
			if math.Abs(stdTanh-emlTanh) > 1e-6 {
				t.Errorf("Tanh(%f): std=%v, eml=%v, diff=%v", x, stdTanh, emlTanh, math.Abs(stdTanh-emlTanh))
			}
		}
	}
}

func TestEmlgoParity_TensorBatch(t *testing.T) {
	n := 1024
	tF64 := New(DtypeFloat64, Shape{n})
	data := tF64.Float64s()
	for i := range data {
		data[i] = 0.01 + float64(i%100)/10.0
	}

	// Compare Sin tensor op in MathSIMD vs MathEML
	SetMathImpl(MathSIMD)
	outStd, err := Sin(tF64)
	if err != nil {
		t.Fatalf("Sin MathSIMD failed: %v", err)
	}

	SetMathImpl(MathEML)
	outEML, err := Sin(tF64)
	if err != nil {
		t.Fatalf("Sin MathEML failed: %v", err)
	}

	stdD := outStd.Float64s()
	emlD := outEML.Float64s()
	for i := 0; i < n; i++ {
		diff := math.Abs(stdD[i] - emlD[i])
		if diff > 1e-6 {
			t.Fatalf("Sin[%d] mismatch: std=%f, eml=%f, diff=%e", i, stdD[i], emlD[i], diff)
		}
	}

	// Compare Exp tensor op
	SetMathImpl(MathSIMD)
	outStd, err = Exp(tF64)
	if err != nil {
		t.Fatalf("Exp MathSIMD failed: %v", err)
	}

	SetMathImpl(MathEML)
	outEML, err = Exp(tF64)
	if err != nil {
		t.Fatalf("Exp MathEML failed: %v", err)
	}

	stdD = outStd.Float64s()
	emlD = outEML.Float64s()
	for i := 0; i < n; i++ {
		relErr := math.Abs(stdD[i]-emlD[i]) / stdD[i]
		if relErr > 1e-4 {
			t.Fatalf("Exp[%d] mismatch: std=%f, eml=%f, relErr=%e", i, stdD[i], emlD[i], relErr)
		}
	}
}

// =============================================================================
// Microbenchmarks: Standard Math vs EMLGo (Scalar)
// =============================================================================

func BenchmarkAB_Scalar_Sqrt(b *testing.B) {
	val := 12345.6789
	b.Run("Standard_math.Sqrt", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sqrt(val)
		}
		_ = res
	})
	b.Run("EMLGo_fastmath.Sqrt", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sqrt(val)
		}
		_ = res
	})
}

func BenchmarkAB_Scalar_Sin(b *testing.B) {
	val := 1.2345
	b.Run("Standard_math.Sin", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sin(val)
		}
		_ = res
	})
	b.Run("EMLGo_fastmath.Sin", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sin(val)
		}
		_ = res
	})
}

func BenchmarkAB_Scalar_Exp(b *testing.B) {
	val := 2.71828
	b.Run("Standard_math.Exp", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Exp(val)
		}
		_ = res
	})
	b.Run("EMLGo_fastmath.Exp", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Exp(val)
		}
		_ = res
	})
}

func BenchmarkAB_Scalar_FMA(b *testing.B) {
	x, y, z := 1.23, 4.56, 7.89
	b.Run("Standard_math.FMA", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.FMA(x, y, z)
		}
		_ = res
	})
	b.Run("EMLGo_fastmath.FMA", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.FMA(x, y, z)
		}
		_ = res
	})
}

func BenchmarkAB_Scalar_Sinh(b *testing.B) {
	val := 1.5
	b.Run("Standard_math.Sinh", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sinh(val)
		}
		_ = res
	})
	b.Run("EMLGo_hyper.Sinh", func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		var res float64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			res = mathutil.Sinh(val)
		}
		_ = res
	})
}

// =============================================================================
// Microbenchmarks: Tensor Batch SIMD Operations
// =============================================================================

func benchmarkBatchSin(b *testing.B, size int) {
	tF64 := New(DtypeFloat64, Shape{size})
	data := tF64.Float64s()
	rng := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = rng.Float64() * math.Pi
	}

	b.Run(fmt.Sprintf("Standard_Sin_N%d", size), func(b *testing.B) {
		SetMathImpl(MathSIMD)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Sin(tF64)
		}
	})

	b.Run(fmt.Sprintf("EMLGo_SinBatch_N%d", size), func(b *testing.B) {
		SetMathImpl(MathEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Sin(tF64)
		}
	})
}

func BenchmarkAB_Tensor_Sin_1K(b *testing.B)   { benchmarkBatchSin(b, 1000) }
func BenchmarkAB_Tensor_Sin_10K(b *testing.B)  { benchmarkBatchSin(b, 10000) }
func BenchmarkAB_Tensor_Sin_100K(b *testing.B) { benchmarkBatchSin(b, 100000) }

func benchmarkBatchExp(b *testing.B, size int) {
	tF64 := New(DtypeFloat64, Shape{size})
	data := tF64.Float64s()
	rng := rand.New(rand.NewSource(42))
	for i := range data {
		data[i] = rng.Float64() * 5.0
	}

	b.Run(fmt.Sprintf("Standard_Exp_N%d", size), func(b *testing.B) {
		SetMathImpl(MathSIMD)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Exp(tF64)
		}
	})

	b.Run(fmt.Sprintf("EMLGo_ExpBatch_N%d", size), func(b *testing.B) {
		SetMathImpl(MathEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Exp(tF64)
		}
	})
}

func BenchmarkAB_Tensor_Exp_1K(b *testing.B)   { benchmarkBatchExp(b, 1000) }
func BenchmarkAB_Tensor_Exp_10K(b *testing.B)  { benchmarkBatchExp(b, 10000) }
func BenchmarkAB_Tensor_Exp_100K(b *testing.B) { benchmarkBatchExp(b, 100000) }

func benchmarkBatchAdd(b *testing.B, size int) {
	tA := New(DtypeFloat64, Shape{size})
	tB := New(DtypeFloat64, Shape{size})
	rng := rand.New(rand.NewSource(42))
	for i := range tA.Float64s() {
		tA.Float64s()[i] = rng.Float64()
		tB.Float64s()[i] = rng.Float64()
	}

	b.Run(fmt.Sprintf("Standard_Add_N%d", size), func(b *testing.B) {
		SetMathImpl(MathSIMD)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Add(tA, tB)
		}
	})

	b.Run(fmt.Sprintf("EMLGo_AddBatch_N%d", size), func(b *testing.B) {
		SetMathImpl(MathEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Add(tA, tB)
		}
	})
}

func BenchmarkAB_Tensor_Add_1K(b *testing.B)   { benchmarkBatchAdd(b, 1000) }
func BenchmarkAB_Tensor_Add_10K(b *testing.B)  { benchmarkBatchAdd(b, 10000) }
func BenchmarkAB_Tensor_Add_100K(b *testing.B) { benchmarkBatchAdd(b, 100000) }

func BenchmarkAB_Tensor_Sinh_TaylorVsEMLGo(b *testing.B) {
	size := 1000
	tF64 := New(DtypeFloat64, Shape{size})
	rng := rand.New(rand.NewSource(42))
	for i := range tF64.Float64s() {
		tF64.Float64s()[i] = rng.Float64() * 2.0
	}

	b.Run("Longbow_MathGo_TaylorSeries", func(b *testing.B) {
		SetMathImpl(MathGo)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Sinh(tF64)
		}
	})

	b.Run("Longbow_MathEML_EMLGo", func(b *testing.B) {
		SetMathImpl(MathEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = Sinh(tF64)
		}
	})
}
