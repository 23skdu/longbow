//go:build arm64

package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// tqPackCases covers the vector loop (multiples of 16), the 4-element loop,
// the 2-element loop, the scalar tail and every remainder in between.
var tqPackCases = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 18, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 1023}

// tqPackInputs returns input vectors that exercise the clamp in both
// directions, the +PI endpoint, and inputs far outside [-PI, +PI] where the
// upper clamp is the only thing keeping the code at maxVal.
func tqPackInputs(n int) map[string][]float32 {
	pi := float64(math.Pi)
	out := map[string][]float32{
		"sweep":  make([]float32, n),
		"beyond": make([]float32, n),
		"zeros":  make([]float32, n),
		"random": make([]float32, n),
	}
	rnd := rand.New(rand.NewSource(int64(n) * 7919))
	for i := 0; i < n; i++ {
		out["sweep"][i] = float32(-pi + 2*pi*float64(i)/float64(max(n, 1)))
		out["beyond"][i] = float32((-2.0 - 6.0*float64(i)/float64(max(n, 1))) * pi)
		out["random"][i] = float32(rnd.Float64()*8*pi - 4*pi)
	}
	return out
}

// TestPackTQ8NEONMatchesGeneric is the regression guard for the 8-bit pack
// kernel. The vector path used to execute FMAX where FMIN was intended: the
// VFMIN_V macro base aliased the VFMAX_V base once the register number was
// shifted in, so the clamp to 1.0 never ran and any element above +PI produced
// a code above 255 that the 8-bit narrowing then wrapped. The tolerance is
// zero because the kernel must be bit-identical to the reference.
func TestPackTQ8NEONMatchesGeneric(t *testing.T) {
	for _, n := range tqPackCases {
		for name, src := range tqPackInputs(n) {
			t.Run(fmt.Sprintf("n%d/%s", n, name), func(t *testing.T) {
				want := make([]byte, n)
				PackTQ8Generic(src, want)
				got := make([]byte, n)
				PackTQ8NEON(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("idx %d (src %v): neon = %d, generic = %d, delta %d",
							i, src[i], got[i], want[i], int(got[i])-int(want[i]))
					}
				}
			})
		}
	}
}

// TestPackTQ4AndTQ2NEONMatchGeneric pins the same missing upper clamp in the
// 4-bit and 2-bit kernels, which share the VFMIN_V macro.
func TestPackTQ4AndTQ2NEONMatchGeneric(t *testing.T) {
	for _, n := range tqPackCases {
		for name, src := range tqPackInputs(n) {
			t.Run(fmt.Sprintf("n%d/%s", n, name), func(t *testing.T) {
				want4 := make([]byte, (n+1)/2)
				PackTQ4Generic(src, want4)
				got4 := make([]byte, (n+1)/2)
				PackTQ4NEON(src, got4)
				for i := range want4 {
					if got4[i] != want4[i] {
						t.Fatalf("tq4 idx %d (src %v): neon = %#02x, generic = %#02x",
							i, src[2*i], got4[i], want4[i])
					}
				}

				want2 := make([]byte, (n+3)/4)
				PackTQ2Generic(src, want2)
				got2 := make([]byte, (n+3)/4)
				PackTQ2NEON(src, got2)
				for i := range want2 {
					if got2[i] != want2[i] {
						t.Fatalf("tq2 idx %d: neon = %#02x, generic = %#02x", i, got2[i], want2[i])
					}
				}
			})
		}
	}
}

// TestPackTQ8NEONIsMonotonic is a sharper form of the same check: a correct
// quantizer never reorders elements, so a strictly increasing input must give
// a non-decreasing output.
func TestPackTQ8NEONIsMonotonic(t *testing.T) {
	pi := float64(math.Pi)
	for _, n := range []int{4, 8, 16, 17, 32, 64, 1023} {
		src := make([]float32, n)
		for i := range src {
			src[i] = float32(-pi + 2*pi*float64(i)/float64(n))
		}
		got := make([]byte, n)
		PackTQ8NEON(src, got)
		for i := 1; i < n; i++ {
			if got[i] < got[i-1] {
				t.Fatalf("n=%d: code[%d] = %d < code[%d] = %d: element order scrambled", n, i, got[i], i-1, got[i-1])
			}
		}
		if got[n-1] != 255 {
			t.Fatalf("n=%d: +PI endpoint produced code %d, want 255", n, got[n-1])
		}
	}
}
