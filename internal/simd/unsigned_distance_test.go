package simd

import (
	"math"
	"testing"
)

// Unsigned distance kernels must widen before subtracting.
//
// This is a regression test for a bug that was live in production code.
// euclideanUint64Unrolled4x computed `float64(a[i]-b[i])`, performing the
// subtraction in uint64 arithmetic. For a probe pair of (1..n, 3..n+2) every
// difference is -2, which in uint64 wraps to 2^64-2, and summing the squares gave
// ~4.4e40 - a returned distance of ~2.1e20 where the answer is sqrt(4n).
//
// The consequences were worse than a slow fallback, because that function is the
// scalar reference that resolveDistanceKernel validates registered uint64 SIMD
// kernels against. A wrapped reference rejected a *correct* kernel at dims 384 and
// 768, while accepting a wrapped kernel at dims 128 because the two were wrong in
// the same way - so uint64 was simultaneously returning nonsense and falling back
// to the slow path.
func referenceL2(a, b []uint64) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

func TestEuclideanDistanceUint64DoesNotWrap(t *testing.T) {
	// Sizes straddle the 8x unroll boundary, since the unrolled loop and the tail
	// used to disagree.
	for _, n := range []int{1, 2, 7, 8, 9, 15, 16, 17, 127, 128, 384, 768} {
		a := make([]uint64, n)
		b := make([]uint64, n)
		for i := range a {
			a[i] = uint64(i + 1)
			b[i] = uint64(i + 3) // a[i] < b[i] everywhere: the wrapping case
		}
		want := float32(referenceL2(a, b))

		got, err := EuclideanDistanceUint64(a, b)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if math.Abs(float64(got-want)) > 1e-3*math.Max(math.Abs(float64(want)), 1) {
			t.Errorf("n=%d: EuclideanDistanceUint64 = %v, want %v", n, got, want)
		}
	}
}

func TestEuclideanUint64UnrolledMatchesDispatch(t *testing.T) {
	// The function used directly and the dispatched path must agree. They did not
	// before the fix, because the unrolled body and its own tail loop disagreed.
	for _, n := range []int{8, 64, 128, 384, 768} {
		a := make([]uint64, n)
		b := make([]uint64, n)
		for i := range a {
			a[i] = uint64(i + 1)
			b[i] = uint64(i + 3)
		}
		direct, err := euclideanUint64Unrolled4x(a, b)
		if err != nil {
			t.Fatal(err)
		}
		dispatched, err := EuclideanDistanceUint64(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if direct != dispatched {
			t.Errorf("n=%d: unrolled4x %v != dispatched %v", n, direct, dispatched)
		}
	}
}

func TestEuclideanDistanceUint64LargeValues(t *testing.T) {
	// Large magnitudes must not turn a small distance into a huge one.
	const n = 128
	a := make([]uint64, n)
	b := make([]uint64, n)
	for i := range a {
		a[i] = 1 << 40
		b[i] = (1 << 40) + 2
	}
	got, err := EuclideanDistanceUint64(a, b)
	if err != nil {
		t.Fatal(err)
	}
	want := float32(math.Sqrt(float64(4 * n)))
	if math.Abs(float64(got-want)) > 1e-2 {
		t.Errorf("large-magnitude vectors: got %v, want %v", got, want)
	}
}

// TestInt64Unchanged guards the fix: euclideanInt64Unrolled4x already widened
// correctly, because int64 subtraction does not wrap for these differences. An
// earlier attempt at this fix patched the int64 function by mistake.
func TestInt64Unchanged(t *testing.T) {
	for _, n := range []int{8, 128, 384} {
		a := make([]int64, n)
		b := make([]int64, n)
		for i := range a {
			a[i] = int64(i + 1)
			b[i] = int64(i + 3)
		}
		got, err := euclideanInt64Unrolled4x(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if want := float32(math.Sqrt(float64(4 * n))); math.Abs(float64(got-want)) > 1e-3 {
			t.Errorf("n=%d: euclideanInt64Unrolled4x = %v, want %v", n, got, want)
		}
	}
}

// TestUnsignedDistanceKernelsAgreeWithReference extends the check across the
// narrower unsigned types. Only uint64 had the wrap bug, because only uint64
// subtracted in its own type; this pins that so a similar change elsewhere fails.
func TestUnsignedDistanceKernelsAgreeWithReference(t *testing.T) {
	const n = 384
	check := func(name string, got float32, want float32) {
		t.Helper()
		if math.Abs(float64(got-want)) > 1e-3*math.Max(math.Abs(float64(want)), 1) {
			t.Errorf("%s: got %v want %v", name, got, want)
		}
	}

	a8 := make([]uint8, n)
	b8 := make([]uint8, n)
	a16 := make([]uint16, n)
	b16 := make([]uint16, n)
	for i := 0; i < n; i++ {
		a8[i], b8[i] = uint8(i%200+1), uint8(i%200+3)
		a16[i], b16[i] = uint16(i+1), uint16(i+3)
	}
	var s8, s16 float64
	for i := 0; i < n; i++ {
		d8 := float64(a8[i]) - float64(b8[i])
		s8 += d8 * d8
		d16 := float64(a16[i]) - float64(b16[i])
		s16 += d16 * d16
	}

	g8, _ := EuclideanDistanceUint8(a8, b8)
	g16, _ := EuclideanDistanceUint16(a16, b16)
	check("uint8", g8, float32(math.Sqrt(s8)))
	check("uint16", g16, float32(math.Sqrt(s16)))
}
