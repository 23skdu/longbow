package simd

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
)

// turboQuantDistanceGenericSincosRef is a verbatim copy of the pre-LUT
// TurboQuantDistanceGeneric inner loop: every element evaluates math.Sincos.
// It is the reference the LUT path must reproduce bit-for-bit.
func turboQuantDistanceGenericSincosRef(query []float32, tqData []byte, dim int, pow2 int, bitsPerAngle int) (float32, error) {
	if len(tqData) < 4 || bitsPerAngle <= 0 || bitsPerAngle > 8 {
		return 0, nil
	}
	radius := math.Float32frombits(uint32(tqData[0]) | uint32(tqData[1])<<8 | uint32(tqData[2])<<16 | uint32(tqData[3])<<24)

	angleCount := pow2 - 1
	angleBytes := (angleCount*bitsPerAngle + 7) / 8
	if len(tqData) < 4+angleBytes {
		return 0, nil
	}
	packedAngles := tqData[4 : 4+angleBytes]
	qjlBits := tqData[4+angleBytes:]

	maxVal := float32((uint32(1) << bitsPerAngle) - 1)
	qIndices := make([]byte, angleCount)
	var currentBit int
	for i := range qIndices {
		var q uint32
		for k := 0; k < bitsPerAngle; k++ {
			if (packedAngles[currentBit/8] & (byte(1) << (currentBit % 8))) != 0 {
				q |= (uint32(1) << k)
			}
			currentBit++
		}
		qIndices[i] = byte(q)
	}

	recon := make([]float32, pow2)
	recon[0] = radius

	currentLevelSize := 1
	angleOffset := angleCount
	for currentLevelSize < pow2 {
		angleOffset -= currentLevelSize
		for i := currentLevelSize - 1; i >= 0; i-- {
			r := recon[i]
			q := qIndices[angleOffset+i]
			theta := (float32(q)/maxVal)*2*math.Pi - math.Pi
			s, c := math.Sincos(float64(theta))
			recon[2*i] = r * float32(c)
			recon[2*i+1] = r * float32(s)
		}
		currentLevelSize *= 2
	}

	correction := radius / float32(math.Sqrt(float64(pow2))) * 0.1
	sum := l2SquaredTQCorrectionGeneric(query, recon, qjlBits, correction, dim)
	return float32(math.Sqrt(float64(sum))), nil
}

// tqSynthCode builds a synthetic TurboQuant code (radius + packed angles +
// QJL bits) whose angle codes sweep the whole 0..2^bits-1 grid.
func tqSynthCode(pow2, bits int) []byte {
	angleCount := pow2 - 1
	angleBytes := (angleCount*bits + 7) / 8
	buf := make([]byte, 4+angleBytes+(pow2+7)/8)

	// radius = 0.75f (little endian)
	buf[0], buf[1], buf[2], buf[3] = 0x00, 0x00, 0x40, 0x3F

	levels := 1 << bits
	bit := 0
	for i := 0; i < angleCount; i++ {
		q := (i*7 + i/bits + 3) % levels
		for k := 0; k < bits; k++ {
			if q&(1<<k) != 0 {
				buf[4+bit/8] |= 1 << (bit % 8)
			}
			bit++
		}
	}

	rng := rand.New(rand.NewSource(int64(bits) * 7919))
	qjl := buf[4+angleBytes:]
	for i := range qjl {
		qjl[i] = byte(rng.Uint32())
	}
	return buf
}

var (
	tqSinkF float32
	tqSinkE error
)

// TestTQLUTMatchesSincos checks the tables are correct by construction: every
// entry must equal the math.Sincos of the exact expression the per-element path
// used, for all bit depths and all codes.
func TestTQLUTMatchesSincos(t *testing.T) {
	for bits := tqLUTMinBits; bits <= tqLUTMaxBits; bits++ {
		lookup := tqLUTFor(bits)
		if len(lookup) != 2*(1<<bits) {
			t.Fatalf("bits=%d: table length %d, want %d", bits, len(lookup), 2*(1<<bits))
		}
		maxVal := float32((uint32(1) << bits) - 1)
		for q := 0; q < 1<<bits; q++ {
			theta := (float32(q)/maxVal)*2*math.Pi - math.Pi
			s, c := math.Sincos(float64(theta))
			if got := lookup[2*q]; got != float32(c) {
				t.Fatalf("bits=%d q=%d: cos = %v, want %v (bit pattern %08x vs %08x)",
					bits, q, got, float32(c), math.Float32bits(got), math.Float32bits(float32(c)))
			}
			if got := lookup[2*q+1]; got != float32(s) {
				t.Fatalf("bits=%d q=%d: sin = %v, want %v (bit pattern %08x vs %08x)",
					bits, q, got, float32(s), math.Float32bits(got), math.Float32bits(float32(s)))
			}
		}
	}
}

// TestTQLUTLayout verifies the flat layout: depth b owns the 2^b pairs starting
// at tqLUTBase(b), and no depth overlaps another.
func TestTQLUTLayout(t *testing.T) {
	covered := make([]bool, tqLUTEntries)
	for bits := tqLUTMinBits; bits <= tqLUTMaxBits; bits++ {
		base := tqLUTBase(bits)
		for i := 0; i < 1<<bits; i++ {
			if covered[base+i] {
				t.Fatalf("bits=%d: pair %d already owned by another depth", bits, base+i)
			}
			covered[base+i] = true
		}
	}
	for i, ok := range covered {
		if !ok {
			t.Fatalf("pair %d is not covered by any bit depth", i)
		}
	}
	if tqLUTFor(tqLUTMinBits-1) != nil || tqLUTFor(tqLUTMaxBits+1) != nil || tqLUTFor(0) != nil {
		t.Fatal("tqLUTFor must return nil outside [tqLUTMinBits, tqLUTMaxBits]")
	}
}

// TestTurboQuantDistanceGeneric_LUTParitySincos asserts the LUT-based generic
// distance is bit-identical to the previous per-element math.Sincos version
// for every supported bit depth.
func TestTurboQuantDistanceGeneric_LUTParitySincos(t *testing.T) {
	for _, pow2 := range []int{8, 16, 128, 1024} {
		for bits := tqLUTMinBits; bits <= tqLUTMaxBits; bits++ {
			for _, dim := range []int{pow2, pow2 / 2} {
				if dim < 1 {
					continue
				}
				data := tqSynthCode(pow2, bits)
				query := make([]float32, pow2)
				rng := rand.New(rand.NewSource(int64(pow2*31 + bits)))
				for i := range query {
					query[i] = rng.Float32()
				}
				got, err := TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
				if err != nil {
					t.Fatalf("pow2=%d bits=%d: %v", pow2, bits, err)
				}
				want, _ := turboQuantDistanceGenericSincosRef(query, data, dim, pow2, bits)
				if got != want {
					t.Errorf("pow2=%d bits=%d dim=%d: distance = %v (%08x), want %v (%08x)",
						pow2, bits, dim, got, math.Float32bits(got), want, math.Float32bits(want))
				}
			}
		}
	}
}

// TestTurboQuantDistanceFastPathUnchanged pins the 2/4/8-bit fast paths, which
// already used a table, to the same table the generic path now shares.
//
// The 2-bit fast unpack used to stop after the angleCount/4 whole groups and
// never write the 1-3 leftover codes of the always-odd angleCount, so the
// reconstruction read pooled scratch from earlier calls. That tail is now
// written, so bits=2 must match the generic path exactly like 4 and 8.
func TestTurboQuantDistanceFastPathUnchanged(t *testing.T) {
	const pow2, dim = 1024, 768
	query := make([]float32, pow2)
	rng := rand.New(rand.NewSource(99))
	for i := range query {
		query[i] = rng.Float32()
	}
	for _, bits := range []int{2, 4, 8} {
		data := tqSynthCode(pow2, bits)
		neon, err := TurboQuantDistanceNEON(query, data, dim, pow2, bits)
		if err != nil {
			t.Fatalf("bits=%d: %v", bits, err)
		}
		generic, _ := TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
		ref, _ := turboQuantDistanceGenericSincosRef(query, data, dim, pow2, bits)
		if generic != ref {
			t.Errorf("bits=%d: generic %v != sincos ref %v", bits, generic, ref)
		}
		if neon != generic {
			t.Errorf("bits=%d: neon %v != generic %v", bits, neon, generic)
		}
	}
}

// tqScratchPair allocates a (recon, codes) scratch pair filled with poison.
func tqScratchPair(pow2 int, poison byte) ([]float32, []byte) {
	recon := make([]float32, pow2)
	codes := make([]byte, pow2)
	for i := range recon {
		recon[i] = -12345
	}
	for i := range codes {
		codes[i] = poison
	}
	return recon, codes
}

// TestTurboQuantDistanceFastPathIgnoresScratchPoison is the regression guard for
// the stale-scratch bug: the fast unpacks run on a pooled, caller-supplied
// buffer, so a code they fail to write silently becomes whatever the previous
// request left there. Every poison must give the same answer, and for the NEON
// path that answer must be the generic path's, which builds its own codes.
func TestTurboQuantDistanceFastPathIgnoresScratchPoison(t *testing.T) {
	poisonMax := func(bits int) byte { return byte((1 << bits) - 1) }

	for _, bits := range []int{2, 4, 8} {
		for _, pow2 := range []int{8, 16, 64, 256, 1024} {
			t.Run(fmt.Sprintf("bits%d/pow2%d", bits, pow2), func(t *testing.T) {
				dim := pow2
				data := tqSynthCode(pow2, bits)
				query := make([]float32, pow2)
				rng := rand.New(rand.NewSource(int64(pow2*31 + bits)))
				for i := range query {
					query[i] = rng.Float32()
				}
				want, err := TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
				if err != nil {
					t.Fatal(err)
				}

				for _, poison := range []byte{0x00, 0xAA & poisonMax(bits), poisonMax(bits)} {
					recon, codes := tqScratchPair(pow2, poison)
					got, err := turboQuantDistanceNEONScratch(query, data, dim, pow2, bits, recon, codes)
					if err != nil {
						t.Fatalf("poison=%#02x: %v", poison, err)
					}
					if got != want {
						t.Errorf("neon poison=%#02x: distance = %v, want %v", poison, got, want)
					}

					// The AVX2 variant sums with a different kernel than the
					// generic path, so it is pinned to poison-independence
					// against itself rather than to want.
					var first float32
					for round := range 3 {
						recon, codes := tqScratchPair(pow2, poison+byte(round))
						avx, err := turboQuantDistanceAVX2Scratch(query, data, dim, pow2, bits, recon, codes)
						if err != nil {
							t.Fatalf("poison=%#02x: %v", poison, err)
						}
						if round == 0 {
							first = avx
							continue
						}
						if avx != first {
							t.Errorf("avx2 poison=%#02x round %d: distance = %v, want %v",
								poison+byte(round), round, avx, first)
						}
					}
				}
			})
		}
	}
}

// TestPackTQ8AVX2MatchesGeneric is the regression guard for the 8-bit pack
// kernel, which produced codes off by up to 255 for two independent reasons:
//
//   - It broadcast its constants through X0, but Go assembles the two-operand
//     "VMOVSS m32, Xn" form with VEX.L=1, i.e. as the 256-bit variant, which
//     zeroes bits [255:32] of the destination YMM. Every later constant load
//     therefore wiped the previously installed broadcast and the +PI step was
//     applied to lane 0 only.
//   - Its VEXTRACTI128 scratch was X6, the low half of the clamp constant Y6,
//     so the first group replaced Y6's upper half with quantized codes and
//     every later element collapsed to code 0.
//
// The kernel is now bit-identical to the Go reference, so the tolerance is
// zero: a difference of one code is already a bug worth failing on.
func TestPackTQ8AVX2MatchesGeneric(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	for _, n := range []int{1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 127, 128, 255, 256, 1023} {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			src := make([]float32, n)
			for i := range src {
				// Sweep the whole code range and then some, so clamping and
				// both endpoints are exercised.
				src[i] = float32(-math.Pi + float64(i)/float64(n)*4*math.Pi)
			}
			want := make([]byte, n)
			PackTQ8Generic(src, want)
			got := make([]byte, n)
			PackTQ8AVX2(src, got)
			dispatched := make([]byte, n)
			PackTQ8(src, dispatched)

			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("idx %d (src %v): avx2 = %d, generic = %d, delta %d",
						i, src[i], got[i], want[i], int(got[i])-int(want[i]))
				}
				if dispatched[i] != got[i] {
					t.Fatalf("idx %d: dispatched PackTQ8 = %d, PackTQ8AVX2 = %d",
						i, dispatched[i], got[i])
				}
			}
		})
	}
}

// TestPackTQ8AVX2IsMonotonic pins the same failure more sharply than the
// tolerance check above: a correct quantizer never reorders elements, so a
// strictly increasing input must give a non-decreasing output.
func TestPackTQ8AVX2IsMonotonic(t *testing.T) {
	if !features.HasAVX2 {
		t.Skip("AVX2 not supported")
	}
	for _, n := range []int{8, 16, 17, 32, 64, 128, 1023} {
		src := make([]float32, n)
		for i := range src {
			src[i] = float32(-math.Pi + float64(i)/float64(n)*2*math.Pi)
		}
		got := make([]byte, n)
		PackTQ8AVX2(src, got)
		for i := 1; i < n; i++ {
			if got[i] < got[i-1] {
				t.Fatalf("n=%d: code[%d] = %d < code[%d] = %d: element order scrambled",
					n, i, got[i], i-1, got[i-1])
			}
		}
	}
}

// TestTQLUTConcurrentUse exercises first use of the tables from many goroutines.
// The tables are built in init, so concurrent readers must never race.
func TestTQLUTConcurrentUse(t *testing.T) {
	const goroutines = 64
	const iters = 50
	type result struct {
		got  float32
		want float32
	}
	results := make([]result, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		bits := g%tqLUTMaxBits + tqLUTMinBits
		const pow2, dim = 256, 256
		data := tqSynthCode(pow2, bits)
		query := make([]float32, pow2)
		for i := range query {
			query[i] = float32(i%17) / 16
		}
		want, _ := turboQuantDistanceGenericSincosRef(query, data, dim, pow2, bits)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				got, _ := TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
				results[g] = result{got: got, want: want}
			}
		}()
	}
	wg.Wait()
	for g, r := range results {
		if r.got != r.want {
			t.Errorf("goroutine %d: distance = %v, want %v", g, r.got, r.want)
		}
	}
}

func BenchmarkTurboQuantDistanceGeneric_LUT_vs_Sincos(b *testing.B) {
	const dim, pow2 = 768, 1024
	query := make([]float32, pow2)
	rng := rand.New(rand.NewSource(1))
	for i := range query {
		query[i] = rng.Float32()
	}

	for _, bits := range []int{3, 5, 7} {
		data := tqSynthCode(pow2, bits)
		b.Run(fmt.Sprintf("bits%d/sincos", bits), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tqSinkF, tqSinkE = turboQuantDistanceGenericSincosRef(query, data, dim, pow2, bits)
			}
		})
		b.Run(fmt.Sprintf("bits%d/lut", bits), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tqSinkF, tqSinkE = TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
			}
		})
	}
}

// BenchmarkPackTQ8 measures the 8-bit pack kernel against the Go reference.
func BenchmarkPackTQ8(b *testing.B) {
	const dim = 1023
	src := make([]float32, dim)
	for i := range src {
		src[i] = float32(-math.Pi + float64(i)/float64(dim)*2*math.Pi)
	}
	dst := make([]byte, dim)

	b.Run("generic", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			PackTQ8Generic(src, dst)
		}
	})
	if features.HasAVX2 {
		b.Run("avx2", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				PackTQ8AVX2(src, dst)
			}
		})
	}
	b.Run("dispatched", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			PackTQ8(src, dst)
		}
	})
}

// BenchmarkTurboQuantDistanceFastPath measures the fixed-bit fast paths against
// the generic path, so the cost of the 2-bit tail that now writes the leftover
// codes stays visible.
func BenchmarkTurboQuantDistanceFastPath(b *testing.B) {
	const dim, pow2 = 768, 1024
	query := make([]float32, pow2)
	rng := rand.New(rand.NewSource(7))
	for i := range query {
		query[i] = rng.Float32()
	}
	for _, bits := range []int{2, 4, 8} {
		data := tqSynthCode(pow2, bits)
		b.Run(fmt.Sprintf("bits%d/neon", bits), func(b *testing.B) {
			recon, codes := tqScratchPair(pow2, 0)
			b.ReportAllocs()
			for b.Loop() {
				tqSinkF, tqSinkE = turboQuantDistanceNEONScratch(query, data, dim, pow2, bits, recon, codes)
			}
		})
		b.Run(fmt.Sprintf("bits%d/avx2", bits), func(b *testing.B) {
			recon, codes := tqScratchPair(pow2, 0)
			b.ReportAllocs()
			for b.Loop() {
				tqSinkF, tqSinkE = turboQuantDistanceAVX2Scratch(query, data, dim, pow2, bits, recon, codes)
			}
		})
		b.Run(fmt.Sprintf("bits%d/generic", bits), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tqSinkF, tqSinkE = TurboQuantDistanceGeneric(query, data, dim, pow2, bits)
			}
		})
	}
}
