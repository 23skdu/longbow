//go:build amd64

package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// Parity of the VBMI TurboQuant packer with the generic reference.
//
// turboquant_pack_amd64_test.go already checks PackTQ2AVX512, PackTQ4AVX512 and
// PackTQ8AVX512 against their generic counterparts, but nothing checked
// PackTQ2AVX512VBMI against PackTQ2Generic. The only test that touched the VBMI
// packer was TestPackTQ2AVX512VBMI, which round trips through
// UnpackTQ2AVX512VBMI with a tolerance of 2.0 - and the spacing between adjacent
// 2-bit codes over [-PI, PI] is 2*PI/3 = 2.094. A packer that is one code off
// everywhere therefore round trips within tolerance and passes.
//
// scripts/check_avx512_coverage.sh runs this under Intel SDE
// (docs/roadmap.md item 5). There was no evidence before it that the VBMI
// packer agreed with the definition it implements.

// tq2VBMIProbe spans [-2*PI, 2*PI] so both clamp arms are exercised, and
// includes exact multiples of PI, which sit on the rounding boundary and are
// where an off-by-one code shows up first.
func tq2VBMIProbe(dim int, seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic
	src := make([]float32, dim)
	for i := range src {
		switch i % 8 {
		case 0:
			src[i] = float32(math.Pi)
		case 1:
			src[i] = -float32(math.Pi)
		case 2:
			src[i] = 0
		case 3:
			src[i] = float32(math.Pi) / 6
		default:
			src[i] = float32(rng.Float64()*4-2) * float32(math.Pi)
		}
	}
	return src
}

func TestPackTQ2AVX512VBMIMatchesGeneric(t *testing.T) {
	if !features.HasAVX512 || !features.HasVBMI {
		t.Skip("AVX512 VBMI not supported")
	}
	for _, n := range packSizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			inputs := packInputs(n)
			// The distributions packInputs already covers, plus a probe that
			// lands on the rounding boundaries the generic packer rounds at.
			inputs["probe"] = tq2VBMIProbe(n, int64(n)+1)
			for name, src := range inputs {
				want := make([]byte, (n+3)/4)
				PackTQ2Generic(src, want)
				got := make([]byte, (n+3)/4)
				PackTQ2AVX512VBMI(src, got)
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s idx %d (src %v): vbmi = %08b, generic = %08b",
							name, i, src[i], got[i], want[i])
					}
				}
			}
		})
	}
}

// TestPackTQ2AVX512VBMIPropagatesInputOrder checks the packing order rather than
// only the code values. VPMULTISHIFTQB collects bits across lanes, so a wrong
// control word scrambles the order without changing the multiset of codes, and
// comparing multisets would not see it.
//
// The expectation is taken from PackTQ2Generic rather than from a hand-derived
// formula, so this test is about ordering and the other is about values.
func TestPackTQ2AVX512VBMIPropagatesInputOrder(t *testing.T) {
	if !features.HasAVX512 || !features.HasVBMI {
		t.Skip("AVX512 VBMI not supported")
	}
	const n = 256
	// A strictly increasing ramp over [-PI, PI] gives four distinct code
	// values in ascending order, so a transposition shows up as an inversion.
	src := make([]float32, n)
	for i := range src {
		src[i] = -float32(math.Pi) + float32(i)/float32(n-1)*2*float32(math.Pi)
	}
	want := make([]byte, n/4)
	PackTQ2Generic(src, want)
	got := make([]byte, n/4)
	PackTQ2AVX512VBMI(src, got)
	for i := 0; i < n; i++ {
		gotCode := (got[i/4] >> (uint(i%4) * 2)) & 0x3
		wantCode := (want[i/4] >> (uint(i%4) * 2)) & 0x3
		if gotCode != wantCode {
			t.Fatalf("element %d (src %v): code %d, want %d", i, src[i], gotCode, wantCode)
		}
		if wantCode < 0x3 && gotCode > wantCode+1 {
			t.Fatalf("element %d: code %d jumps more than one step past %d, which is "+
				"what an out-of-order bit extraction looks like", i, gotCode, wantCode)
		}
	}
}

// TestUnpackTQ2AVX512VBMIMatchesGeneric checks the VBMI unpacker the same way.
// The packer and the unpacker are two halves of one format, so a fix to either
// has to leave the pair agreeing with the generic definition of both halves.
func TestUnpackTQ2AVX512VBMIMatchesGeneric(t *testing.T) {
	if !features.HasAVX512 || !features.HasVBMI {
		t.Skip("AVX512 VBMI not supported")
	}
	const dim = 256
	const scale = float32(2 * math.Pi / 3)
	const bias = -float32(math.Pi)

	// Every byte pattern, so no code value and no bit order is missed.
	src := make([]byte, dim/4)
	for i := range src {
		src[i] = byte(i)
	}
	want := make([]float32, dim)
	got := make([]float32, dim)
	UnpackTQ2Generic(src, want, scale, bias)
	UnpackTQ2AVX512VBMI(src, got, scale, bias)
	for i := range want {
		if math.Abs(float64(got[i]-want[i])) > 1e-5 {
			t.Fatalf("element %d: vbmi = %v, generic = %v (byte %d = %#02x)",
				i, got[i], want[i], i/4, src[i/4])
		}
	}
}
