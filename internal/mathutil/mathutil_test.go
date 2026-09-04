package mathutil

import (
	"math"
	"testing"
)

func TestMathutilBackends(t *testing.T) {
	backends := []Backend{BackendStandard, BackendEML}

	for _, b := range backends {
		SetBackend(b)
		name := "Standard"
		if b == BackendEML {
			name = "EML"
		}
		t.Run(name, func(t *testing.T) {
			// Sqrt
			sqrtVal := Sqrt(16.0)
			if math.Abs(sqrtVal-4.0) > 1e-6 {
				t.Errorf("%s Sqrt(16) = %f, expected 4.0", name, sqrtVal)
			}

			// FMA
			fmaVal := FMA(2.0, 3.0, 4.0)
			if math.Abs(fmaVal-10.0) > 1e-6 {
				t.Errorf("%s FMA(2, 3, 4) = %f, expected 10.0", name, fmaVal)
			}

			// Sin / Cos
			sinVal := Sin(math.Pi / 6)
			if math.Abs(sinVal-0.5) > 1e-4 {
				t.Errorf("%s Sin(pi/6) = %f, expected 0.5", name, sinVal)
			}
			cosVal := Cos(math.Pi / 3)
			if math.Abs(cosVal-0.5) > 1e-4 {
				t.Errorf("%s Cos(pi/3) = %f, expected 0.5", name, cosVal)
			}

			// Exp / Log
			expVal := Exp(1.0)
			if math.Abs(expVal-math.E) > 1e-4 {
				t.Errorf("%s Exp(1) = %f, expected %f", name, expVal, math.E)
			}
			logVal := Log(math.E)
			if math.Abs(logVal-1.0) > 1e-4 {
				t.Errorf("%s Log(e) = %f, expected 1.0", name, logVal)
			}

			// Sinh / Cosh / Tanh
			sinhVal := Sinh(1.0)
			if math.Abs(sinhVal-math.Sinh(1.0)) > 1e-4 {
				t.Errorf("%s Sinh(1) = %f, expected %f", name, sinhVal, math.Sinh(1.0))
			}
			coshVal := Cosh(1.0)
			if math.Abs(coshVal-math.Cosh(1.0)) > 1e-4 {
				t.Errorf("%s Cosh(1) = %f, expected %f", name, coshVal, math.Cosh(1.0))
			}
			tanhVal := Tanh(1.0)
			if math.Abs(tanhVal-math.Tanh(1.0)) > 1e-4 {
				t.Errorf("%s Tanh(1) = %f, expected %f", name, tanhVal, math.Tanh(1.0))
			}

			// Batch tests
			f64s := []float64{0.1, 0.5, 1.0, 2.0}
			expBatch := ExpBatch(f64s)
			if len(expBatch) != 4 {
				t.Fatalf("%s ExpBatch length mismatch", name)
			}
			for i, v := range f64s {
				expected := math.Exp(v)
				if math.Abs(expBatch[i]-expected)/expected > 1e-4 {
					t.Errorf("%s ExpBatch[%d] = %f, expected %f", name, i, expBatch[i], expected)
				}
			}

			sinBatch := SinBatch(f64s)
			if len(sinBatch) != 4 {
				t.Fatalf("%s SinBatch length mismatch", name)
			}
			for i, v := range f64s {
				expected := math.Sin(v)
				if math.Abs(sinBatch[i]-expected) > 1e-4 {
					t.Errorf("%s SinBatch[%d] = %f, expected %f", name, i, sinBatch[i], expected)
				}
			}

			// AddBatch, MulBatch
			addBatch := AddBatch(f64s, f64s)
			for i, v := range f64s {
				if math.Abs(addBatch[i]-2*v) > 1e-6 {
					t.Errorf("%s AddBatch[%d] = %f, expected %f", name, i, addBatch[i], 2*v)
				}
			}
		})
	}
}

func BenchmarkBatchComparison(b *testing.B) {
	n := 1000
	x := make([]float64, n)
	for i := range x {
		x[i] = float64(i) * 0.001
	}

	b.Run("Std_MathExp_Loop", func(b *testing.B) {
		out := make([]float64, n)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j, v := range x {
				out[j] = math.Exp(v)
			}
		}
		_ = out
	})

	b.Run("EML_FastMathExp_Loop", func(b *testing.B) {
		out := make([]float64, n)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j, v := range x {
				out[j] = Exp(v)
			}
		}
		_ = out
	})

	b.Run("EML_ExpBatch", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ExpBatch(x)
		}
	})
}
