package simd

import (
	"math"
	"math/rand"
	"testing"
)

// Parity of the 16-bit AVX2 kernels against a float64 reference.
//
// The euclideanInt16AVX2Kernel / euclideanUint16AVX2Kernel pair used to square
// the widened difference with VPMULLD, in int32. Two int16 operands can differ
// by up to 65535, whose square is 4294836225 - it does not fit in an int32 and
// wrapped silently. A full-range corpus got 214317.9 where the exact answer is
// 275675.4, a 22% error, with no error returned: the kernel signature has no
// error slot for a wrong answer.
//
// Nothing caught it because resolveAllDistanceFuncs validated the registered
// int16 kernel against simd.EuclideanDistanceInt16, which dispatches to the
// registered kernel. The comparison was a kernel against itself. The resolver
// now compares against the widening float64 implementation in
// internal/store/index, and these tests are the other half of that.

// euclideanRef is the definition the kernels have to reproduce: sum the squared
// differences in float64, then take one square root at the end.
func euclideanRef(a, b []float64) float64 {
	var s float64
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return math.Sqrt(s)
}

func refI16(a, b []int16) float64 {
	x := make([]float64, len(a))
	y := make([]float64, len(b))
	for i := range a {
		x[i] = float64(a[i])
		y[i] = float64(b[i])
	}
	return euclideanRef(x, y)
}

func refU16(a, b []uint16) float64 {
	x := make([]float64, len(a))
	y := make([]float64, len(b))
	for i := range a {
		x[i] = float64(a[i])
		y[i] = float64(b[i])
	}
	return euclideanRef(x, y)
}

// relErr is the relative deviation, which is the right measure here: the
// magnitude of a distance scales with the corpus and an absolute tolerance
// would either be vacuous for large values or unreachable for small ones.
func relErr(got, want float64) float64 {
	if want == 0 {
		if got == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return math.Abs(got-want) / math.Abs(want)
}

// The three ranges below are the three regimes that matter:
//   - full: the entire type range, which is where VPMULLD overflowed
//   - half: half the type range, still above the sqrt(2^31) = 46340 bound
//   - bounded: what a quantized embedding actually holds, well inside it
func int16Ranges() []struct {
	name       string
	lo, hi     int
	spanWidths []int
} {
	return []struct {
		name       string
		lo, hi     int
		spanWidths []int
	}{
		{"full", -32768, 32768, []int{8, 16, 24, 31, 32, 33, 63, 64, 65, 127, 128, 129, 768}},
		{"half", -32768, 32767, []int{16, 31, 32, 64, 128}},
		{"bounded", -1000, 1000, []int{16, 32, 64, 128, 768}},
	}
}

func TestEuclideanInt16AVX2MatchesFloat64Reference(t *testing.T) {
	rng := rand.New(rand.NewSource(3)) // #nosec G404 -- deterministic

	for _, r := range int16Ranges() {
		r := r
		t.Run(r.name, func(t *testing.T) {
			for _, dims := range r.spanWidths {
				a := make([]int16, dims)
				b := make([]int16, dims)
				for i := 0; i < dims; i++ {
					a[i] = int16(r.lo + rng.Intn(r.hi-r.lo)) // #nosec G404
					b[i] = int16(r.lo + rng.Intn(r.hi-r.lo)) // #nosec G404
				}
				got, err := EuclideanDistanceInt16(a, b)
				if err != nil {
					t.Fatalf("dims=%d: %v", dims, err)
				}
				want := refI16(a, b)
				if e := relErr(float64(got), want); e > 1e-5 {
					t.Errorf("dims=%d EuclideanDistanceInt16 = %v, float64 reference %v (rel err %.3g)",
						dims, got, want, e)
				}
			}
		})
	}
}

func TestEuclideanUint16AVX2MatchesFloat64Reference(t *testing.T) {
	rng := rand.New(rand.NewSource(5)) // #nosec G404 -- deterministic

	cases := []struct {
		name   string
		lo, hi int
		dims   []int
	}{
		// The full range is where the difference can reach 65535 and its square
		// cannot be held in an int32.
		{"full", 0, 65536, []int{8, 16, 24, 31, 32, 33, 63, 64, 65, 127, 128, 129, 768}},
		{"high", 60000, 65536, []int{16, 32, 64, 128}},
		{"bounded", 0, 1000, []int{16, 32, 64, 128, 768}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			for _, dims := range c.dims {
				a := make([]uint16, dims)
				b := make([]uint16, dims)
				for i := 0; i < dims; i++ {
					a[i] = uint16(c.lo + rng.Intn(c.hi-c.lo)) // #nosec G404
					b[i] = uint16(c.lo + rng.Intn(c.hi-c.lo)) // #nosec G404
				}
				got, err := EuclideanDistanceUint16(a, b)
				if err != nil {
					t.Fatalf("dims=%d: %v", dims, err)
				}
				want := refU16(a, b)
				if e := relErr(float64(got), want); e > 1e-5 {
					t.Errorf("dims=%d EuclideanDistanceUint16 = %v, float64 reference %v (rel err %.3g)",
						dims, got, want, e)
				}
			}
		})
	}
}

// TestEuclideanInt16WorstCaseDifference pins the specific case that broke: every
// element differing by exactly 65535, whose square is 4294836225.
func TestEuclideanInt16WorstCaseDifference(t *testing.T) {
	const dims = 128
	a := make([]int16, dims)
	b := make([]int16, dims)
	for i := 0; i < dims; i++ {
		a[i], b[i] = -32768, 32767
	}
	got, err := EuclideanDistanceInt16(a, b)
	if err != nil {
		t.Fatal(err)
	}
	want := refI16(a, b)
	if e := relErr(float64(got), want); e > 1e-5 {
		t.Errorf("EuclideanDistanceInt16 over the maximum int16 difference = %v, want %v "+
			"(rel err %.3g); %v squared does not fit in an int32", got, want, e, 65535.0)
	}

	ua := make([]uint16, dims)
	ub := make([]uint16, dims)
	for i := 0; i < dims; i++ {
		ua[i], ub[i] = 0, 65535
	}
	gotU, err := EuclideanDistanceUint16(ua, ub)
	if err != nil {
		t.Fatal(err)
	}
	wantU := refU16(ua, ub)
	if e := relErr(float64(gotU), wantU); e > 1e-5 {
		t.Errorf("EuclideanDistanceUint16 over the maximum uint16 difference = %v, want %v "+
			"(rel err %.3g)", gotU, wantU, e)
	}
}

// BenchmarkInt16EuclideanKernel compares the 16-bit kernels against the 8-bit
// ones over a resident working set, so the figures are kernel-bound rather than
// memory-bound and the difference is the read path's arithmetic, not its traffic.
//
// Per element these should track each other closely. At 128 dimensions the
// 16-bit vector is twice the bytes of the 8-bit one, so the two are expected to
// sit close together here and to separate by up to 2x once the working set
// exceeds cache.
func BenchmarkInt16EuclideanKernel(b *testing.B) {
	const (
		dims = 128
		n    = 4096
	)
	rng := rand.New(rand.NewSource(9)) // #nosec G404 -- benchmark data

	i8 := make([]int8, n*dims)
	for i := range i8 {
		i8[i] = int8(rng.Intn(256) - 128) // #nosec G404
	}
	i16 := make([]int16, n*dims)
	for i := range i16 {
		i16[i] = int16(rng.Intn(65536) - 32768) // #nosec G404
	}
	u16 := make([]uint16, n*dims)
	for i := range u16 {
		u16[i] = uint16(rng.Intn(65536)) // #nosec G404
	}
	q8 := make([]int8, dims)
	q16 := make([]int16, dims)
	qu16 := make([]uint16, dims)
	for i := 0; i < dims; i++ {
		q8[i] = int8(i % 255)
		q16[i] = int16((i * 7) % 65535)
		qu16[i] = uint16((i * 11) % 65535)
	}

	b.Run("Int8", func(b *testing.B) {
		b.ReportMetric(float64(b.N*n), "vec/s")
		for b.Loop() {
			best := float32(math.MaxFloat32)
			for j := 0; j < n; j++ {
				d, _ := EuclideanDistanceInt8(q8, i8[j*dims:(j+1)*dims])
				if d < best {
					best = d
				}
			}
			kernelSink = best
		}
	})
	b.Run("Int16", func(b *testing.B) {
		b.ReportMetric(float64(b.N*n), "vec/s")
		for b.Loop() {
			best := float32(math.MaxFloat32)
			for j := 0; j < n; j++ {
				d, _ := EuclideanDistanceInt16(q16, i16[j*dims:(j+1)*dims])
				if d < best {
					best = d
				}
			}
			kernelSink = best
		}
	})
	b.Run("Uint16", func(b *testing.B) {
		b.ReportMetric(float64(b.N*n), "vec/s")
		for b.Loop() {
			best := float32(math.MaxFloat32)
			for j := 0; j < n; j++ {
				d, _ := EuclideanDistanceUint16(qu16, u16[j*dims:(j+1)*dims])
				if d < best {
					best = d
				}
			}
			kernelSink = best
		}
	})
}

var kernelSink float32
