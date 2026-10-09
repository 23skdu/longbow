package simd

import (
	"fmt"
	"math"
	"testing"
)

// packSizes are the lengths worth checking: the awkward ones around the
// vector width, the 16-element main loop, and the sizes that leave a scalar
// tail of 1..15 elements.
var packSizes = []int{1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 255, 256, 1023}

// packInputs returns named input distributions. Together they cover the
// clamp at both endpoints, exact .5 boundaries, monotonicity and noise.
func packInputs(n int) map[string][]float32 {
	full := float32(4 * math.Pi)

	out := map[string][]float32{}

	ramp := make([]float32, n)
	for i := range ramp {
		ramp[i] = -math.Pi + float32(i)/float32(max(1, n))*full
	}
	out["ramp"] = ramp

	// Every value at exactly +pi, i.e. the top of the range: the code must
	// saturate rather than wrap.
	plusPi := make([]float32, n)
	for i := range plusPi {
		plusPi[i] = math.Pi
	}
	out["plusPi"] = plusPi

	// Every value at exactly -pi, the bottom.
	minusPi := make([]float32, n)
	for i := range minusPi {
		minusPi[i] = -math.Pi
	}
	out["minusPi"] = minusPi

	// Far outside the range in both directions, to pin the clamp.
	far := make([]float32, n)
	for i := range far {
		if i%2 == 0 {
			far[i] = 1e6
		} else {
			far[i] = -1e6
		}
	}
	out["far"] = far

	zeros := make([]float32, n)
	out["zeros"] = zeros

	// Values that land exactly on .5 quantisation boundaries, where a
	// half-to-even convert diverges from the reference's floor.
	ticks := make([]float32, n)
	for i := range ticks {
		// norm sweeps 0..1 in 1/64 steps, so norm*maxVal hits k+0.5 exactly.
		ticks[i] = float32(float64(i)/64*2-1)*math.Pi - math.Pi
	}
	out["halfTicks"] = ticks

	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// TestPackTQ2AVX2MatchesGeneric requires the AVX2 2-bit packer to be
// bit-identical to the scalar reference. The kernel had three assembly
// defects: the constant-broadcast chain (Go assembles the two-operand
// VMOVSS m32, Xn with VEX.L=1, which zeroes the destination YMM's upper
// lanes, so every constant except the last was left zero in lanes 4-7),
// a missing floor before the int convert, and a VPERMPD that permuted the
// packed codes out of element order.
func TestPackTQ2AVX2MatchesGeneric(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			for name, src := range packInputs(n) {
				want := make([]byte, (n+3)/4)
				PackTQ2Generic(src, want)
				got := make([]byte, (n+3)/4)
				PackTQ2AVX2(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): avx2 = %08b, generic = %08b",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

// TestPackTQ4AVX2MatchesGeneric is the 4-bit equivalent, where each output
// byte carries two nibbles and the byte order additionally depends on the
// low nibble being the earlier element.
func TestPackTQ4AVX2MatchesGeneric(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			for name, src := range packInputs(n) {
				want := make([]byte, (n+1)/2)
				PackTQ4Generic(src, want)
				got := make([]byte, (n+1)/2)
				PackTQ4AVX2(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): avx2 = %02x, generic = %02x",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

// TestPackTQAVX2IsMonotonic pins the element-order defect class on its own.
// A quantiser applied to a strictly increasing input must produce a
// non-decreasing output; any lane permutation shows up here even if the
// code values happen to coincide.
func TestPackTQAVX2IsMonotonic(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	const n = 64
	// A strictly increasing sweep: the codes it produces must therefore be
	// non-decreasing for every width, which is what catches a lane permutation
	// or a reversed field order.
	src := make([]float32, n)
	for i := range src {
		src[i] = -math.Pi + float32(i)/float32(n-1)*2*math.Pi
	}

	t.Run("tq8", func(t *testing.T) {
		got := make([]byte, n)
		PackTQ8AVX2(src, got)
		for i := 1; i < n; i++ {
			if got[i] < got[i-1] {
				t.Fatalf("idx %d: %d < %d, element order scrambled", i, got[i], got[i-1])
			}
		}
	})

	t.Run("tq4", func(t *testing.T) {
		got := make([]byte, (n+1)/2)
		PackTQ4AVX2(src, got)
		for i := range got {
			lo, hi := got[i]&0x0F, got[i]>>4
			if i > 0 {
				prevHi := got[i-1] >> 4
				if lo < prevHi {
					t.Fatalf("idx %d: low nibble %d < previous high nibble %d, order scrambled",
						i, lo, prevHi)
				}
			}
			if hi < lo {
				t.Fatalf("idx %d: high nibble %d < low nibble %d, element order scrambled", i, hi, lo)
			}
		}
	})

	t.Run("tq2", func(t *testing.T) {
		got := make([]byte, (n+3)/4)
		PackTQ2AVX2(src, got)
		for i := range got {
			for b := 1; b < 4; b++ {
				if (got[i]>>(2*b))&0x03 < (got[i]>>(2*(b-1)))&0x03 {
					t.Fatalf("idx %d field %d: codes out of order in %08b", i, b, got[i])
				}
			}
		}
	})
}

// TestPackTQAVX2TopOfRange pins that a value at exactly +pi saturates to the
// maximum code rather than wrapping past it. Without the clamp this produces
// a code above max, which the 8-bit narrowing then truncates.
func TestPackTQAVX2TopOfRange(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	const n = 32
	src := make([]float32, n)
	for i := range src {
		src[i] = math.Pi
	}

	t.Run("tq8", func(t *testing.T) {
		got := make([]byte, n)
		PackTQ8AVX2(src, got)
		for i, g := range got {
			if g != 255 {
				t.Fatalf("idx %d: got %d, want 255", i, g)
			}
		}
	})

	t.Run("tq4", func(t *testing.T) {
		got := make([]byte, (n+1)/2)
		PackTQ4AVX2(src, got)
		for i, g := range got {
			if g != 0xff {
				t.Fatalf("idx %d: got %#x, want 0xff", i, g)
			}
		}
	})

	t.Run("tq2", func(t *testing.T) {
		got := make([]byte, (n+3)/4)
		PackTQ2AVX2(src, got)
		for i, g := range got {
			if g != 0xff {
				t.Fatalf("idx %d: got %#x, want 0xff", i, g)
			}
		}
	})
}

func TestPackTQ2AVX512MatchesGeneric(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			for name, src := range packInputs(n) {
				want := make([]byte, (n+3)/4)
				PackTQ2Generic(src, want)
				got := make([]byte, (n+3)/4)
				PackTQ2AVX512(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): avx512 = %08b, generic = %08b",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

func TestPackTQ4AVX512MatchesGeneric(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			for name, src := range packInputs(n) {
				want := make([]byte, (n+1)/2)
				PackTQ4Generic(src, want)
				got := make([]byte, (n+1)/2)
				PackTQ4AVX512(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): avx512 = %02x, generic = %02x",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

func TestPackTQ8AVX512MatchesGeneric(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			for name, src := range packInputs(n) {
				want := make([]byte, n)
				PackTQ8Generic(src, want)
				got := make([]byte, n)
				PackTQ8AVX512(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): avx512 = %d, generic = %d",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

func TestPackTQAVX512IsMonotonic(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not supported")
	}
	const n = 64
	src := make([]float32, n)
	for i := range src {
		src[i] = -math.Pi + float32(i)/float32(n-1)*2*math.Pi
	}

	t.Run("tq8", func(t *testing.T) {
		got := make([]byte, n)
		PackTQ8AVX512(src, got)
		for i := 1; i < n; i++ {
			if got[i] < got[i-1] {
				t.Fatalf("idx %d: %d < %d, element order scrambled", i, got[i], got[i-1])
			}
		}
	})

	t.Run("tq4", func(t *testing.T) {
		got := make([]byte, (n+1)/2)
		PackTQ4AVX512(src, got)
		for i := range got {
			lo, hi := got[i]&0x0F, got[i]>>4
			if i > 0 {
				prevHi := got[i-1] >> 4
				if lo < prevHi {
					t.Fatalf("idx %d: low nibble %d < previous high nibble %d, order scrambled",
						i, lo, prevHi)
				}
			}
			if hi < lo {
				t.Fatalf("idx %d: high nibble %d < low nibble %d, element order scrambled", i, hi, lo)
			}
		}
	})

	t.Run("tq2", func(t *testing.T) {
		got := make([]byte, (n+3)/4)
		PackTQ2AVX512(src, got)
		for i := range got {
			for b := 1; b < 4; b++ {
				if (got[i]>>(2*b))&0x03 < (got[i]>>(2*(b-1)))&0x03 {
					t.Fatalf("idx %d field %d: codes out of order in %08b", i, b, got[i])
				}
			}
		}
	})
}

func TestPackTQAVX512TopOfRange(t *testing.T) {
	if !features.HasAVX512 {
		t.Skip("AVX512 not supported")
	}
	const n = 32
	src := make([]float32, n)
	for i := range src {
		src[i] = math.Pi
	}

	t.Run("tq8", func(t *testing.T) {
		got := make([]byte, n)
		PackTQ8AVX512(src, got)
		for i, g := range got {
			if g != 255 {
				t.Fatalf("idx %d: got %d, want 255", i, g)
			}
		}
	})

	t.Run("tq4", func(t *testing.T) {
		got := make([]byte, (n+1)/2)
		PackTQ4AVX512(src, got)
		for i, g := range got {
			if g != 0xff {
				t.Fatalf("idx %d: got %#x, want 0xff", i, g)
			}
		}
	})

	t.Run("tq2", func(t *testing.T) {
		got := make([]byte, (n+3)/4)
		PackTQ2AVX512(src, got)
		for i, g := range got {
			if g != 0xff {
				t.Fatalf("idx %d: got %#x, want 0xff", i, g)
			}
		}
	})
}
