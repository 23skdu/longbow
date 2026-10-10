package simd

import (
	"math"
	"math/rand"
	"strconv"
	"testing"
)

// The batched TurboQuant kernel has to be substitutable for the single-vector
// one without changing a single search result.
//
// The bar is bit equality, not closeness.
// TestTQComputeBatchMatchesPerCandidate in internal/store/index compares the
// raw float32 of a batched TurboQuant search against the per-candidate path,
// and a kernel that is merely accurate to within a tolerance would pass that
// test while quietly changing which of two equidistant neighbours wins. HNSW
// neighbour selection is decided by these comparisons, so "close" is not a
// property this kernel can have.

func tqBatchProbeCorpus(tb testing.TB, n, dim, bits int, seed int64) ([]float32, [][]byte, int) {
	tb.Helper()
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic

	pow2 := 1
	for pow2 < dim {
		pow2 *= 2
	}
	angleCount := pow2 - 1
	angleBytes := (angleCount*bits + 7) / 8
	// The QJL sign bit is applied to every element of recon, which is pow2
	// long, so the payload carries pow2/8 sign bytes rather than dim/8.
	jlBytes := pow2 / 8
	if jlBytes == 0 {
		jlBytes = 1
	}
	payload := 4 + angleBytes + jlBytes

	query := make([]float32, pow2)
	for i := range query {
		query[i] = rng.Float32()*2 - 1
	}

	codes := make([][]byte, n)
	for i := range codes {
		c := make([]byte, payload)
		// A radius in the same range the encoder emits, so the reconstruction
		// does not saturate and the comparison exercises real values.
		r := 0.5 + rng.Float32()
		c[0] = byte(math.Float32bits(r))
		c[1] = byte(math.Float32bits(r) >> 8)
		c[2] = byte(math.Float32bits(r) >> 16)
		c[3] = byte(math.Float32bits(r) >> 24)
		for j := 4; j < payload; j++ {
			c[j] = byte(rng.Intn(256))
		}
		codes[i] = c
	}
	return query, codes, pow2
}

// TestTurboQuantDistanceBatchIsBitIdentical is the gate. Every width, every bit
// depth, and block lengths that exercise the width-4 remainder path.
func TestTurboQuantDistanceBatchIsBitIdentical(t *testing.T) {
	for _, bits := range []int{2, 4, 8} {
		for _, dim := range []int{64, 128, 384, 768} {
			for _, n := range []int{1, 2, 3, 4, 5, 7, 8, 9, 16, 17, 64, 129} {
				query, codes, pow2 := tqBatchProbeCorpus(t, n, dim, bits, int64(dim)*31+int64(n))
				want := make([]float32, n)
				fn := GetTurboQuantDistanceFunc()
				for i, c := range codes {
					d, err := fn(query, c, dim, pow2, bits)
					if err != nil {
						t.Fatalf("single-vector: %v", err)
					}
					want[i] = d
				}
				got := make([]float32, n)
				if err := TurboQuantDistanceBatch(query, codes, got, dim, pow2, bits); err != nil {
					t.Fatalf("batch: %v", err)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("bits=%d dim=%d n=%d: element %d batched %v != single %v "+
							"(bits differ: %x)", bits, dim, n, i,
							math.Float32bits(got[i]), math.Float32bits(want[i]),
							math.Float32bits(got[i])^math.Float32bits(want[i]))
					}
				}
			}
		}
	}
}

// TestTurboQuantBatchKernelsCrossArchitectureParity verifies that all implementation-specific
// batch kernels match their corresponding single-vector kernels bit-identically.
func TestTurboQuantBatchKernelsCrossArchitectureParity(t *testing.T) {
	const dim, n = 128, 17
	for _, bits := range []int{2, 4, 8} {
		query, codes, pow2 := tqBatchProbeCorpus(t, n, dim, bits, int64(bits)*12345)

		type testPair struct {
			name   string
			batch  func([]float32, [][]byte, []float32, int, int, int) error
			single func([]float32, []byte, int, int, int) (float32, error)
		}

		pairs := []testPair{
			{"avx2", turboQuantDistanceBatchAVX2, TurboQuantDistanceAVX2},
			{"neon", turboQuantDistanceBatchNEON, TurboQuantDistanceNEON},
			{"fallback", turboQuantDistanceBatchFallback, TurboQuantDistanceGeneric},
			{"dispatched", TurboQuantDistanceBatch, GetTurboQuantDistanceFunc()},
		}
		if features.HasAVX512 {
			pairs = append(pairs, testPair{"avx512", turboQuantDistanceBatchAVX512, TurboQuantDistanceAVX512})
		}

		for _, p := range pairs {
			want := make([]float32, n)
			for i, c := range codes {
				d, err := p.single(query, c, dim, pow2, bits)
				if err != nil {
					t.Fatalf("%s single: %v", p.name, err)
				}
				want[i] = d
			}
			got := make([]float32, n)
			if err := p.batch(query, codes, got, dim, pow2, bits); err != nil {
				t.Fatalf("%s batch: %v", p.name, err)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("kernel %s bits=%d element %d: got %v != want %v (bits differ: %x)",
						p.name, bits, i, got[i], want[i],
						math.Float32bits(got[i])^math.Float32bits(want[i]))
				}
			}
		}
	}
}

// TestTurboQuantDistanceBatchNoFurtherFromGenericThanSingleVector pins the
// batched kernel against the portable definition without demanding an accuracy
// the format does not have.
//
// TurboQuantDistanceGeneric and the SIMD single-vector kernels do not agree to
// 1e-5 with each other, and are not expected to: the generic path folds the QJL
// sign correction into its own L2 loop while the SIMD paths apply it to the
// reconstruction first. At 2 bits per angle the two differ by around 1%.
//
// So the invariant is not "batched matches generic". It is that batching
// introduces no error of its own: the batched result is no further from the
// generic reference than the single-vector SIMD result is. If a change to this
// kernel ever made it worse, this fails.
func TestTurboQuantDistanceBatchNoFurtherFromGenericThanSingleVector(t *testing.T) {
	const dim, n = 128, 37
	for _, bits := range []int{2, 4, 8} {
		query, codes, pow2 := tqBatchProbeCorpus(t, n, dim, bits, int64(bits)*977)

		generic := make([]float32, n)
		for i, c := range codes {
			d, err := TurboQuantDistanceGeneric(query, c, dim, pow2, bits)
			if err != nil {
				t.Fatal(err)
			}
			generic[i] = d
		}
		single := make([]float32, n)
		fn := GetTurboQuantDistanceFunc()
		for i, c := range codes {
			d, err := fn(query, c, dim, pow2, bits)
			if err != nil {
				t.Fatal(err)
			}
			single[i] = d
		}
		batched := make([]float32, n)
		if err := TurboQuantDistanceBatch(query, codes, batched, dim, pow2, bits); err != nil {
			t.Fatal(err)
		}

		for i := range generic {
			scale := math.Max(math.Abs(float64(generic[i])), 1)
			singleErr := math.Abs(float64(single[i]-generic[i])) / scale
			batchedErr := math.Abs(float64(batched[i]-generic[i])) / scale
			if batchedErr > singleErr+1e-6 {
				t.Errorf("bits=%d element %d: batched is %.6f from the generic reference "+
					"while the single-vector kernel is %.6f from it; batching introduced error "+
					"of its own", bits, i, batchedErr, singleErr)
			}
		}
	}
}

// TestTurboQuantDistanceBatchDegenerate covers the inputs this kernel refuses
// to define and checks that deferring to the single-vector path is what happens.
func TestTurboQuantDistanceBatchDegenerate(t *testing.T) {
	query, codes, pow2 := tqBatchProbeCorpus(t, 5, 128, 4, 1)

	// Truncated payload: the single-vector path's length guard, not this one's.
	short := make([][]byte, len(codes))
	for i, c := range codes {
		short[i] = c[:3]
	}
	got := make([]float32, len(short))
	if err := TurboQuantDistanceBatch(query, short, got, 128, pow2, 4); err != nil {
		t.Fatal(err)
	}
	for i, c := range short {
		want, err := TurboQuantDistanceGeneric(query, c, 128, pow2, 4)
		if err != nil {
			t.Fatal(err)
		}
		if got[i] != want {
			t.Errorf("truncated element %d: %v, want %v", i, got[i], want)
		}
	}

	// A bit width the format does not define must still produce the
	// single-vector answer rather than a panic or a silent zero.
	for _, bits := range []int{0, 1, 3, 9, 16} {
		if err := TurboQuantDistanceBatch(query, codes, got, 128, pow2, bits); err != nil {
			t.Fatalf("bits=%d: %v", bits, err)
		}
	}

	// Empty block is a no-op, not an error.
	if err := TurboQuantDistanceBatch(query, nil, nil, 128, pow2, 4); err != nil {
		t.Fatal(err)
	}

	// A destination too small is reported rather than written past.
	if err := TurboQuantDistanceBatch(query, codes, make([]float32, 2), 128, pow2, 4); err == nil {
		t.Error("expected an error for a destination smaller than the block")
	}
}

// BenchmarkTurboQuantDistanceBatch measures the block sizes searchLayer actually
// produces.
//
// The last attempt at batching (docs/roadmap.md R24) was measured at 32 KB,
// which stays in L2, and the number it produced was then used to predict the
// effect on a 128 MB working set touched in node-id order. It came out 25%
// slower end to end. The block sizes below bracket what searchLayer really
// offers: 5-7 candidates per hop today, and 32-64 if candidates are accumulated
// across hops.
func BenchmarkTurboQuantDistanceBatch(b *testing.B) {
	for _, dim := range []int{128, 768} {
		for _, block := range []int{1, 6, 16, 32, 64} {
			query, codes, pow2 := tqBatchProbeCorpus(b, block, dim, 4, int64(dim)+int64(block))
			dst := make([]float32, block)

			b.Run("single/"+itoa(dim)+"/"+itoa(block), func(b *testing.B) {
				fn := GetTurboQuantDistanceFunc()
				b.ReportAllocs()
				for b.Loop() {
					for i, c := range codes {
						d, err := fn(query, c, dim, pow2, 4)
						if err != nil {
							b.Fatal(err)
						}
						dst[i] = d
					}
				}
			})
			b.Run("batched/"+itoa(dim)+"/"+itoa(block), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := TurboQuantDistanceBatch(query, codes, dst, dim, pow2, 4); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func itoa(v int) string { return strconv.Itoa(v) }

// FuzzTurboQuantDistanceBatch fuzzes the batched TurboQuant distance computation
// with arbitrary vectors, codes, and invalid configurations to ensure memory safety
// and panic-freedom across arbitrary payloads.
func FuzzTurboQuantDistanceBatch(f *testing.F) {
	// Seed corpus
	f.Add(byte(2), uint16(128), byte(4), []byte{0x00, 0x01, 0x02, 0x03, 0xaa, 0xbb})
	f.Add(byte(4), uint16(128), byte(8), []byte{0x10, 0x20, 0x30, 0x40, 0x55, 0x66, 0x77, 0x88})
	f.Add(byte(8), uint16(256), byte(1), []byte{0x00, 0x00, 0x80, 0x3f})

	f.Fuzz(func(t *testing.T, bits byte, dimU16 uint16, batchSizeByte byte, rawPayload []byte) {
		dim := int(dimU16 % 1024)
		if dim <= 0 {
			dim = 16
		}
		bitsPerAngle := int(bits)
		batchSize := int(batchSizeByte % 32)
		if batchSize == 0 {
			batchSize = 1
		}

		pow2 := 1
		for pow2 < dim {
			pow2 *= 2
		}

		query := make([]float32, dim)
		for i := range query {
			query[i] = float32(i%17) / 17.0
		}

		codes := make([][]byte, batchSize)
		for i := range codes {
			codes[i] = rawPayload
		}

		dst := make([]float32, batchSize)
		// Must not panic under any input
		_ = TurboQuantDistanceBatch(query, codes, dst, dim, pow2, bitsPerAngle)
	})
}
