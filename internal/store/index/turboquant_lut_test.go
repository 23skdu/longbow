package index

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
)

// polarReconstructSincosRef is a verbatim copy of the pre-LUT
// polarReconstructRecursive: it evaluates math.Sincos for every angle.
// It is the reference the LUT path must reproduce bit-for-bit.
func polarReconstructSincosRef(pow2 int, radius float32, angles []float32, dst []float32, stack []float32) {
	n := len(dst)
	if n == 1 {
		dst[0] = radius
		return
	}

	stackOffset := pow2 - n
	nextRadii := stack[stackOffset : stackOffset+n/2]

	polarReconstructSincosRef(pow2, radius, angles[n/2:], nextRadii, stack)

	for i := 0; i < n/2; i++ {
		r := nextRadii[i]
		theta := angles[i]
		sin, cos := math.Sincos(float64(theta))
		dst[2*i] = r * float32(cos)
		dst[2*i+1] = r * float32(sin)
	}
}

// tqDecodeSincosRef reproduces the pre-LUT Decode inner path: unpack the
// angles to float32 and evaluate math.Sincos per element.
func tqDecodeSincosRef(enc *TurboQuantEncoder, data []byte) []float32 {
	radius := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))

	angleCount := enc.pow2 - 1
	angleBytes := (angleCount*enc.params.BitsPerAngle + 7) / 8
	qjlOffset := 4 + angleBytes

	angles := make([]float32, angleCount)
	enc.unpackAngles(data[4:qjlOffset], angles)

	recon := make([]float32, enc.pow2)
	stack := make([]float32, enc.pow2)
	polarReconstructSincosRef(enc.pow2, radius, angles, recon, stack)

	correction := radius / float32(math.Sqrt(float64(enc.pow2))) * 0.1
	qjlBits := data[qjlOffset:]
	for i := 0; i < enc.pow2; i++ {
		if (qjlBits[i/8] & (byte(1) << (i % 8))) != 0 {
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}
	return recon
}

// tqDecodePooledSincosRef reproduces the pre-LUT Decode exactly, pooled
// workspace and all, so it can stand in for the old end-to-end cost.
func tqDecodePooledSincosRef(enc *TurboQuantEncoder, data []byte) ([]float32, error) {
	radius := math.Float32frombits(binary.LittleEndian.Uint32(data[0:4]))

	angleCount := enc.pow2 - 1
	angleBytes := (angleCount*enc.params.BitsPerAngle + 7) / 8
	qjlOffset := 4 + angleBytes

	wsPtr := enc.getWorkspace()
	workspace := *wsPtr
	defer enc.putWorkspace(wsPtr)

	angles := workspace[enc.pow2*3 : enc.pow2*3+angleCount]
	enc.unpackAngles(data[4:qjlOffset], angles)

	recon := workspace[enc.pow2 : enc.pow2*2]
	stack := workspace[enc.pow2*2 : enc.pow2*3]
	polarReconstructSincosRef(enc.pow2, radius, angles, recon, stack)

	correction := radius / float32(math.Sqrt(float64(enc.pow2))) * 0.1
	qjlBits := data[qjlOffset:]
	for i := 0; i < enc.pow2; i++ {
		if (qjlBits[i/8] & (byte(1) << (i % 8))) != 0 {
			recon[i] += correction
		} else {
			recon[i] -= correction
		}
	}

	out := make([]float32, enc.pow2)
	copy(out, recon)
	return out, nil
}

var (
	tqReconstructSink float32
	tqDecodeSink      []float32
	tqDecodeSinkErr   error
)

func tqRandomVector(dims int, seed int64) []float32 {
	vec := make([]float32, dims)
	rng := rand.New(rand.NewSource(seed))
	for i := range vec {
		vec[i] = rng.Float32()
	}
	return vec
}

// TestTurboQuantDecode_LUTParitySincos asserts Decode's LUT reconstruction is
// bit-identical to the pre-LUT math.Sincos reconstruction, for every bit depth
// and every power-of-two size.
func TestTurboQuantDecode_LUTParitySincos(t *testing.T) {
	for _, dims := range []int{8, 64, 128, 384, 768} {
		for bits := 1; bits <= 8; bits++ {
			t.Run(fmt.Sprintf("dims%d/bits%d", dims, bits), func(t *testing.T) {
				enc := NewTurboQuantEncoder(dims, bits, int64(dims*100+bits))
				vec := tqRandomVector(dims, int64(dims*7+bits))

				encoded, err := enc.Encode(vec)
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				got, err := enc.Decode(encoded)
				if err != nil {
					t.Fatalf("Decode: %v", err)
				}
				want := tqDecodeSincosRef(enc, encoded)

				if len(got) != len(want) {
					t.Fatalf("length = %d, want %d", len(got), len(want))
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatalf("idx %d: decode = %v (%08x), want %v (%08x)",
							i, got[i], math.Float32bits(got[i]), want[i], math.Float32bits(want[i]))
					}
				}
			})
		}
	}
}

// TestTurboQuantPolarReconstructCodesParity isolates the reconstruction: the
// LUT kernel and the math.Sincos kernel must agree bit-for-bit on the same
// codes, independent of the pack/unpack stage.
func TestTurboQuantPolarReconstructCodesParity(t *testing.T) {
	for _, dims := range []int{8, 128, 384, 768} {
		for bits := 1; bits <= 8; bits++ {
			t.Run(fmt.Sprintf("dims%d/bits%d", dims, bits), func(t *testing.T) {
				enc := NewTurboQuantEncoder(dims, bits, 42)
				angleCount := enc.pow2 - 1

				packed := packAngleCodes(bits, angleCount)
				codes := make([]byte, angleCount)
				enc.unpackAngleCodes(packed, codes)
				angles := make([]float32, angleCount)
				enc.unpackAngles(packed, angles)

				radius := float32(1.25)
				lutRecon := make([]float32, enc.pow2)
				lutStack := make([]float32, enc.pow2)
				refRecon := make([]float32, enc.pow2)
				refStack := make([]float32, enc.pow2)

				enc.polarReconstructCodes(radius, codes, lutRecon, lutStack)
				polarReconstructSincosRef(enc.pow2, radius, angles, refRecon, refStack)

				for i := range lutRecon {
					if lutRecon[i] != refRecon[i] {
						t.Fatalf("idx %d: lut = %v (%08x), sincos = %v (%08x)",
							i, lutRecon[i], math.Float32bits(lutRecon[i]),
							refRecon[i], math.Float32bits(refRecon[i]))
					}
				}
			})
		}
	}
}

// TestTurboQuantPolarLUTMatchesSincos checks the tables are correct by
// construction: every entry is the math.Sincos of the angle grid value the
// unpacker produces for that code.
func TestTurboQuantPolarLUTMatchesSincos(t *testing.T) {
	for bits := 1; bits <= 8; bits++ {
		lookup := tqPolarLUTFor(bits)
		n := 1 << bits
		if len(lookup) != 2*n {
			t.Fatalf("bits=%d: table length %d, want %d", bits, len(lookup), 2*n)
		}
		thetas := make([]float32, n)
		unpackAngleValues(bits, packAngleCodes(bits, n), thetas)
		for q := 0; q < n; q++ {
			s, c := math.Sincos(float64(thetas[q]))
			if got := lookup[2*q]; got != float32(c) {
				t.Fatalf("bits=%d code=%d: cos = %v (%08x), want %v (%08x)",
					bits, q, got, math.Float32bits(got), float32(c), math.Float32bits(float32(c)))
			}
			if got := lookup[2*q+1]; got != float32(s) {
				t.Fatalf("bits=%d code=%d: sin = %v (%08x), want %v (%08x)",
					bits, q, got, math.Float32bits(got), float32(s), math.Float32bits(float32(s)))
			}
		}
	}
}

// TestTurboQuantPolarLUTLayout verifies depth b owns the 2^b pairs starting at
// tqPolarLUTBase(b) with no overlap, and that out-of-range depths yield nil.
func TestTurboQuantPolarLUTLayout(t *testing.T) {
	covered := make([]bool, tqPolarLUTEntries)
	for bits := 1; bits <= 8; bits++ {
		base := tqPolarLUTBase(bits)
		for i := 0; i < 1<<bits; i++ {
			if covered[base+i] {
				t.Fatalf("bits=%d: pair %d owned by two depths", bits, base+i)
			}
			covered[base+i] = true
		}
	}
	for i, ok := range covered {
		if !ok {
			t.Fatalf("pair %d not owned by any depth", i)
		}
	}
	for _, bits := range []int{0, 9, 16, -1} {
		if tqPolarLUTFor(bits) != nil {
			t.Fatalf("bits=%d: want nil table", bits)
		}
	}
}

// TestTurboQuantUnpackAngleCodesInvertsPacking checks the code extraction is
// the exact inverse of packAngleCodes for every depth and for the odd
// angleCount (pow2-1) that Decode always sees.
func TestTurboQuantUnpackAngleCodesInvertsPacking(t *testing.T) {
	for bits := 1; bits <= 8; bits++ {
		for _, count := range []int{1, 3, 7, 8, 31, 32, 127, 255, 1023} {
			enc := NewTurboQuantEncoder(1024, bits, 1)
			packed := packAngleCodes(bits, count)
			codes := make([]byte, count)
			enc.unpackAngleCodes(packed, codes)
			for i := range codes {
				if want := byte(i % (1 << bits)); codes[i] != want {
					t.Fatalf("bits=%d count=%d idx=%d: code = %d, want %d", bits, count, i, codes[i], want)
				}
			}
		}
	}
}

func tqCosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// TestTurboQuantRoundTrip checks the round-trip contract: for the depths the
// codec reconstructs accurately the direction survives (cosine > 0.90, the
// tolerance TestTurboQuant_EncoderDecoder uses), and for every depth the LUT
// decode returns exactly what the pre-LUT math.Sincos decode returned. Depths
// 1-3 sit below the codec's accuracy floor because the angular grid is coarse.
// Depth 8 used to sit at cosine ~ -0.1: simd.PackTQ8 dispatched to the AVX2
// pack kernel, which emitted codes off by up to 255, so the LUT work only
// faithfully reproduced an already corrupted angle stream.
func TestTurboQuantRoundTrip(t *testing.T) {
	for _, dims := range []int{128, 384, 768} {
		for bits := 1; bits <= 8; bits++ {
			t.Run(fmt.Sprintf("dims%d/bits%d", dims, bits), func(t *testing.T) {
				enc := NewTurboQuantEncoder(dims, bits, 42)
				vec := make([]float32, dims)
				for i := range vec {
					vec[i] = float32(i) / float32(dims)
				}

				encoded, err := enc.Encode(vec)
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				decoded, err := enc.Decode(encoded)
				if err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if len(decoded) != enc.pow2 {
					t.Fatalf("decoded length = %d, want %d", len(decoded), enc.pow2)
				}

				rotated := make([]float32, enc.pow2)
				copy(rotated, vec)
				if err := simd.RandomRotation(rotated, enc.params.Seed); err != nil {
					t.Fatalf("RandomRotation: %v", err)
				}

				cosine := tqCosine(rotated, decoded)
				t.Logf("dims=%d pow2=%d bits=%d cosine=%.6f", dims, enc.pow2, bits, cosine)
				if got := tqCosine(rotated, tqDecodeSincosRef(enc, encoded)); got != cosine {
					t.Fatalf("cosine = %v, pre-LUT decode gives %v", cosine, got)
				}
				if bits >= 4 && cosine <= 0.90 {
					t.Errorf("cosine similarity = %v, want > 0.90", cosine)
				}

				again, err := enc.Decode(encoded)
				if err != nil {
					t.Fatalf("second Decode: %v", err)
				}
				for i := range decoded {
					if again[i] != decoded[i] {
						t.Fatalf("Decode is not deterministic at idx %d", i)
					}
				}
			})
		}
	}
}

// TestTurboQuantRoundTrip8Bit is the focused regression guard for the 8-bit
// corruption. It walks the whole supported dimension range and requires the
// direction to survive almost exactly: an 8-bit angular grid resolves angles
// to ~0.7 degrees, so anything that reaches only 0.90 (or the ~-0.1 the broken
// pack kernel produced) means the codes, not the grid, are wrong.
func TestTurboQuantRoundTrip8Bit(t *testing.T) {
	for _, dims := range []int{8, 64, 128, 256, 384, 768, 1024} {
		for _, seed := range []int64{1, 42, 12345} {
			t.Run(fmt.Sprintf("dims%d/seed%d", dims, seed), func(t *testing.T) {
				enc := NewTurboQuantEncoder(dims, 8, seed)
				vec := make([]float32, dims)
				for i := range vec {
					vec[i] = float32(i)/float32(dims) - 0.5
				}

				encoded, err := enc.Encode(vec)
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				decoded, err := enc.Decode(encoded)
				if err != nil {
					t.Fatalf("Decode: %v", err)
				}

				rotated := make([]float32, enc.pow2)
				copy(rotated, vec)
				if err := simd.RandomRotation(rotated, seed); err != nil {
					t.Fatalf("RandomRotation: %v", err)
				}

				cosine := tqCosine(rotated, decoded)
				t.Logf("dims=%d pow2=%d cosine=%.6f", dims, enc.pow2, cosine)
				if cosine <= 0.99 {
					t.Errorf("8-bit round-trip cosine = %.6f, want > 0.99", cosine)
				}
			})
		}
	}
}

// TestTurboQuantPackAngles8BitInvertible checks the 8-bit stage on its own: the
// packed codes must decode back to the angles Encode started from, to within
// the one-code resolution of the grid.
func TestTurboQuantPackAngles8BitInvertible(t *testing.T) {
	for _, bits := range []int{4, 8} {
		t.Run(fmt.Sprintf("bits%d", bits), func(t *testing.T) {
			const count = 1023
			enc := NewTurboQuantEncoder(1024, bits, 1)
			angles := make([]float32, count)
			for i := range angles {
				angles[i] = -float32(math.Pi) + float32(i)/float32(count)*2*float32(math.Pi)
			}
			packed := make([]byte, (count*bits+7)/8)
			enc.packAngles(angles, packed)

			codes := make([]byte, count)
			enc.unpackAngleCodes(packed, codes)
			levels := (1 << uint(bits)) - 1
			step := float32(2*math.Pi) / float32(levels)
			for i, code := range codes {
				got := float32(code)*step - float32(math.Pi)
				if diff := got - angles[i]; diff > step || diff < -step {
					t.Fatalf("idx %d: angle %v round-tripped to %v (code %d)", i, angles[i], got, code)
				}
			}
		})
	}
}

// TestTurboQuantDecodeConcurrent hammers Decode from many goroutines. The
// tables are built in init and the code scratch is pooled, so no goroutine may
// race or observe another goroutine's codes.
func TestTurboQuantDecodeConcurrent(t *testing.T) {
	const goroutines = 32
	const iters = 20

	type job struct {
		enc     *TurboQuantEncoder
		encoded []byte
		want    []float32
	}
	jobs := make([]job, goroutines)
	for g := range jobs {
		bits := g%8 + 1
		dims := 128
		enc := NewTurboQuantEncoder(dims, bits, int64(g)+1)
		vec := tqRandomVector(dims, int64(1000+g))
		encoded, err := enc.Encode(vec)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := enc.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		jobs[g] = job{enc: enc, encoded: encoded, want: got}
	}

	var wg sync.WaitGroup
	errs := make(chan string, goroutines*iters)
	for g := range jobs {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Share the encoders across goroutines to exercise the scratch pools.
				other := jobs[(g+i)%goroutines]
				got, err := other.enc.Decode(other.encoded)
				if err != nil {
					errs <- fmt.Sprintf("goroutine %d: %v", g, err)
					return
				}
				for k := range got {
					if got[k] != other.want[k] {
						errs <- fmt.Sprintf("goroutine %d iter %d: idx %d: %v != %v", g, i, k, got[k], other.want[k])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Error(msg)
	}
}

func benchmarkPolarReconstruct(b *testing.B, dims, bits int, useLUT bool) {
	enc := NewTurboQuantEncoder(dims, bits, 42)
	pow2 := enc.pow2

	vec := tqRandomVector(dims, int64(dims)*31+int64(bits))
	encoded, err := enc.Encode(vec)
	if err != nil {
		b.Fatal(err)
	}
	radius := enc.GetRadius(encoded)
	angleCount := pow2 - 1
	angleBytes := (angleCount*bits + 7) / 8
	packed := encoded[4 : 4+angleBytes]

	angles := make([]float32, angleCount)
	codes := make([]byte, angleCount)
	recon := make([]float32, pow2)
	stack := make([]float32, pow2)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if useLUT {
			enc.unpackAngleCodes(packed, codes)
			enc.polarReconstructCodes(radius, codes, recon, stack)
		} else {
			enc.unpackAngles(packed, angles)
			polarReconstructSincosRef(pow2, radius, angles, recon, stack)
		}
		tqReconstructSink = recon[len(recon)-1]
	}
}

// BenchmarkPolarReconstruct_Sincos measures the pre-LUT path: unpack the angles
// to float32 and evaluate math.Sincos for every element.
func BenchmarkPolarReconstruct_Sincos(b *testing.B) {
	for _, dims := range []int{128, 384, 768} {
		for _, bits := range []int{4, 3} {
			b.Run(fmt.Sprintf("dims%d/bits%d", dims, bits), func(b *testing.B) {
				benchmarkPolarReconstruct(b, dims, bits, false)
			})
		}
	}
}

// BenchmarkPolarReconstruct_LUT measures the same work with the code extraction
// plus table lookup in place of the per-element trigonometry.
func BenchmarkPolarReconstruct_LUT(b *testing.B) {
	for _, dims := range []int{128, 384, 768} {
		for _, bits := range []int{4, 3} {
			b.Run(fmt.Sprintf("dims%d/bits%d", dims, bits), func(b *testing.B) {
				benchmarkPolarReconstruct(b, dims, bits, true)
			})
		}
	}
}

// BenchmarkTurboQuantDecode_LUT_vs_Sincos runs the exported Decode end to end
// against the pre-LUT pooled math.Sincos reconstruction, so the scratch-pool
// overhead of the LUT path is included.
func BenchmarkTurboQuantDecode_LUT_vs_Sincos(b *testing.B) {
	for _, dims := range []int{128, 768} {
		for _, bits := range []int{4, 3, 7} {
			enc := NewTurboQuantEncoder(dims, bits, 42)
			encoded, err := enc.Encode(tqRandomVector(dims, int64(dims)+int64(bits)))
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("dims%d/bits%d/sincos", dims, bits), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					tqDecodeSink, tqDecodeSinkErr = tqDecodePooledSincosRef(enc, encoded)
					if tqDecodeSinkErr != nil {
						b.Fatal(tqDecodeSinkErr)
					}
				}
			})
			b.Run(fmt.Sprintf("dims%d/bits%d/lut", dims, bits), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					tqDecodeSink, tqDecodeSinkErr = enc.Decode(encoded)
					if tqDecodeSinkErr != nil {
						b.Fatal(tqDecodeSinkErr)
					}
				}
			})
		}
	}
}

// BenchmarkTurboQuantRoundTrip8Bit measures the encode and decode cost at the
// 8-bit depth, which is the one that runs simd.PackTQ8AVX2 on x86.
func BenchmarkTurboQuantRoundTrip8Bit(b *testing.B) {
	const dims = 768
	enc := NewTurboQuantEncoder(dims, 8, 42)
	vec := tqRandomVector(dims, int64(dims))
	encoded, err := enc.Encode(vec)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := enc.Encode(vec); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			tqDecodeSink, tqDecodeSinkErr = enc.Decode(encoded)
			if tqDecodeSinkErr != nil {
				b.Fatal(tqDecodeSinkErr)
			}
		}
	})
}
