package mathutil

import (
	"encoding/binary"
	"math"
	"testing"
)

// fuzzFloatBytes packs values into seed bytes for the batch fuzz target.
func fuzzFloatBytes(vals ...float64) []byte {
	b := make([]byte, 8*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(v))
	}
	return b
}

// fuzzFloats decodes up to max float64s from the front of b.
func fuzzFloats(b []byte, max int) []float64 {
	n := len(b) / 8
	if n > max {
		n = max
	}
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
	}
	return out
}

// fuzzSameFloat compares results bit-for-bit so NaN and signed zero are
// handled without masking differences behind != comparisons.
func fuzzSameFloat(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b)
}

// FuzzScalarParity checks every scalar helper against the standard math
// library it delegates to, plus domain/range invariants (sqrt never returns
// NaN for x >= 0, exp/log round-trip, tanh stays in [-1, 1], asin/acos obey
// their domain).
func FuzzScalarParity(f *testing.F) {
	f.Add(2.0, 3.0)
	f.Add(0.0, 0.0)
	f.Add(-1.0, 0.5)
	f.Add(math.Pi, -math.Pi)
	f.Add(math.Inf(1), math.SmallestNonzeroFloat64)
	f.Add(math.NaN(), 2.0)

	f.Fuzz(func(t *testing.T, x, y float64) {
		unary := []struct {
			name string
			got  float64
			want float64
		}{
			{"Sqrt", Sqrt(x), math.Sqrt(x)},
			{"Exp", Exp(x), math.Exp(x)},
			{"Log", Log(x), math.Log(x)},
			{"Sin", Sin(x), math.Sin(x)},
			{"Cos", Cos(x), math.Cos(x)},
			{"Tan", Tan(x), math.Tan(x)},
			{"Sinh", Sinh(x), math.Sinh(x)},
			{"Cosh", Cosh(x), math.Cosh(x)},
			{"Tanh", Tanh(x), math.Tanh(x)},
			{"Asin", Asin(x), math.Asin(x)},
			{"Acos", Acos(x), math.Acos(x)},
			{"Atan", Atan(x), math.Atan(x)},
		}
		for _, tc := range unary {
			if !fuzzSameFloat(tc.got, tc.want) {
				t.Errorf("%s(%g) = %g, want %g", tc.name, x, tc.got, tc.want)
			}
		}
		if got, want := Pow(x, y), math.Pow(x, y); !fuzzSameFloat(got, want) {
			t.Errorf("Pow(%g, %g) = %g, want %g", x, y, got, want)
		}
		if got, want := FMA(x, y, x), math.FMA(x, y, x); !fuzzSameFloat(got, want) {
			t.Errorf("FMA(%g, %g, %g) = %g, want %g", x, y, x, got, want)
		}

		// Domain and range invariants.
		if x >= 0 && !math.IsNaN(x) {
			s := Sqrt(x)
			if math.IsNaN(s) || s < 0 {
				t.Errorf("Sqrt(%g) = %g, want a non-negative number", x, s)
			}
			if ss := s * s; !math.IsInf(ss, 0) && !math.IsNaN(ss) {
				if diff := math.Abs(ss - x); diff > 1e-9*math.Max(1, x) {
					t.Errorf("Sqrt(%g)^2 = %g, off by %g", x, ss, diff)
				}
			}
		}
		// exp(log(x)) round-trips in the normal range.
		if x > 1e-300 && x < 1e300 {
			if got := Exp(Log(x)); math.Abs(got-x) > 1e-9*x {
				t.Errorf("Exp(Log(%g)) = %g, want %g", x, got, x)
			}
		}
		if tn := Tanh(x); tn < -1 || tn > 1 {
			t.Errorf("Tanh(%g) = %g, want within [-1, 1]", x, tn)
		}
		if sn, cn := Sin(x), Cos(x); sn < -1 || sn > 1 || cn < -1 || cn > 1 {
			t.Errorf("Sin(%g)=%g Cos(%g)=%g, want within [-1, 1]", x, sn, x, cn)
		}
		ax := math.Abs(x)
		if ax <= 1 {
			if v := Asin(x); math.Abs(v) > math.Pi/2*(1+1e-12) {
				t.Errorf("Asin(%g) = %g, want |v| <= pi/2", x, v)
			}
			if v := Acos(x); v < -1e-12 || v > math.Pi+1e-12 {
				t.Errorf("Acos(%g) = %g, want within [0, pi]", x, v)
			}
		} else {
			if !math.IsNaN(Asin(x)) || !math.IsNaN(Acos(x)) {
				t.Errorf("Asin/Acos(%g) must be NaN outside [-1, 1]", x)
			}
		}
	})
}

// fuzzCheckBatch verifies a unary batch helper against its per-element
// reference implementation.
func fuzzCheckBatch(t *testing.T, name string, got, input []float64, ref func(float64) float64) {
	t.Helper()
	if len(got) != len(input) {
		t.Fatalf("%s returned %d elements, want %d", name, len(got), len(input))
	}
	for i := range got {
		if !fuzzSameFloat(got[i], ref(input[i])) {
			t.Fatalf("%s[%d] = %g, want %g", name, i, got[i], ref(input[i]))
		}
	}
}

// FuzzBatchParity checks the slice helpers against per-element references:
// output lengths must match the documented shape (min-length for binary
// ops) and every element must equal the scalar computation bit-for-bit,
// including NaN and ±Inf inputs.
func FuzzBatchParity(f *testing.F) {
	f.Add(fuzzFloatBytes(1, 2, 3), fuzzFloatBytes(4, 5), 2.0)
	f.Add(fuzzFloatBytes(), fuzzFloatBytes(1), 0.5)    // empty a
	f.Add(fuzzFloatBytes(1, 2), fuzzFloatBytes(), 0.0) // empty b
	f.Add(fuzzFloatBytes(math.NaN()), fuzzFloatBytes(math.Inf(-1)), 1.0)
	f.Add(fuzzFloatBytes(0.5, -0.5), fuzzFloatBytes(0.25, 0), math.Pi)

	f.Fuzz(func(t *testing.T, aRaw, bRaw []byte, y float64) {
		// Bound the input: at most 1024 elements (8 KiB) per slice.
		if len(aRaw) > 8*1024 || len(bRaw) > 8*1024 {
			return
		}
		a := fuzzFloats(aRaw, 1024)
		b := fuzzFloats(bRaw, 1024)

		fuzzCheckBatch(t, "SqrtBatch", SqrtBatch(a), a, math.Sqrt)
		fuzzCheckBatch(t, "ExpBatch", ExpBatch(a), a, math.Exp)
		fuzzCheckBatch(t, "LogBatch", LogBatch(a), a, math.Log)
		fuzzCheckBatch(t, "SinBatch", SinBatch(a), a, math.Sin)
		fuzzCheckBatch(t, "CosBatch", CosBatch(a), a, math.Cos)
		fuzzCheckBatch(t, "TanBatch", TanBatch(a), a, math.Tan)
		fuzzCheckBatch(t, "SinhBatch", SinhBatch(a), a, math.Sinh)
		fuzzCheckBatch(t, "CoshBatch", CoshBatch(a), a, math.Cosh)
		fuzzCheckBatch(t, "TanhBatch", TanhBatch(a), a, math.Tanh)

		n := min(len(a), len(b))
		binaryCases := []struct {
			name string
			got  []float64
			ref  func(i int) float64
		}{
			{"AddBatch", AddBatch(a, b), func(i int) float64 { return a[i] + b[i] }},
			{"SubBatch", SubBatch(a, b), func(i int) float64 { return a[i] - b[i] }},
			{"MulBatch", MulBatch(a, b), func(i int) float64 { return a[i] * b[i] }},
			{"DivBatch", DivBatch(a, b), func(i int) float64 { return a[i] / b[i] }},
		}
		for _, tc := range binaryCases {
			if len(tc.got) != n {
				t.Fatalf("%s returned %d elements, want min-length %d", tc.name, len(tc.got), n)
			}
			for i := 0; i < n; i++ {
				if !fuzzSameFloat(tc.got[i], tc.ref(i)) {
					t.Fatalf("%s[%d] = %g, want %g", tc.name, i, tc.got[i], tc.ref(i))
				}
			}
		}

		pb := PowBatch(a, y)
		if len(pb) != len(a) {
			t.Fatalf("PowBatch returned %d elements, want %d", len(pb), len(a))
		}
		for i := range a {
			if !fuzzSameFloat(pb[i], math.Pow(a[i], y)) {
				t.Fatalf("PowBatch[%d] = %g, want %g", i, pb[i], math.Pow(a[i], y))
			}
		}

		nb := NegBatch(a)
		if len(nb) != len(a) {
			t.Fatalf("NegBatch returned %d elements, want %d", len(nb), len(a))
		}
		for i := range a {
			if !fuzzSameFloat(nb[i], -a[i]) {
				t.Fatalf("NegBatch[%d] = %g, want %g", i, nb[i], -a[i])
			}
			// Negating twice is the identity (NaN payloads may flip sign,
			// so they are excluded).
			if !math.IsNaN(a[i]) {
				if nn := -nb[i]; !fuzzSameFloat(nn, a[i]) {
					t.Fatalf("double NegBatch[%d] = %g, want %g", i, nn, a[i])
				}
			}
		}

		// Float32 batches: parity plus the min-length contract.
		fa := make([]float32, len(a))
		for i := range a {
			fa[i] = float32(a[i])
		}
		fb := make([]float32, len(b))
		for i := range b {
			fb[i] = float32(b[i])
		}
		// Float32 bit comparison keeps NaN inputs from tripping != checks.
		add32 := AddBatchF32(fa, fb)
		if len(add32) != min(len(fa), len(fb)) {
			t.Fatalf("AddBatchF32 returned %d elements, want %d", len(add32), min(len(fa), len(fb)))
		}
		for i := range add32 {
			if math.Float32bits(add32[i]) != math.Float32bits(fa[i]+fb[i]) {
				t.Fatalf("AddBatchF32[%d] = %g, want %g", i, add32[i], fa[i]+fb[i])
			}
		}
		mul32 := MulBatchF32(fa, fb)
		if len(mul32) != min(len(fa), len(fb)) {
			t.Fatalf("MulBatchF32 returned %d elements, want %d", len(mul32), min(len(fa), len(fb)))
		}
		for i := range mul32 {
			if math.Float32bits(mul32[i]) != math.Float32bits(fa[i]*fb[i]) {
				t.Fatalf("MulBatchF32[%d] = %g, want %g", i, mul32[i], fa[i]*fb[i])
			}
		}
		exp32 := ExpBatchF32(fa)
		if len(exp32) != len(fa) {
			t.Fatalf("ExpBatchF32 returned %d elements, want %d", len(exp32), len(fa))
		}
		for i := range exp32 {
			want := float32(math.Exp(float64(fa[i])))
			if math.Float32bits(exp32[i]) != math.Float32bits(want) {
				t.Fatalf("ExpBatchF32[%d] = %g, want %g", i, exp32[i], want)
			}
		}
	})
}
