package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// TestUint16Uint32SIMDParity verifies uint16 and uint32 Euclidean, Dot, and
// Cosine kernels (AVX2 / blocked / unrolled dispatch) against exact float64
// scalar references across aligned and unaligned tail dimensions (roadmap B1).
func TestUint16Uint32SIMDParity(t *testing.T) {
	dimsList := []int{1, 7, 8, 15, 16, 31, 64, 128, 384, 768}
	rng := rand.New(rand.NewSource(202611181)) // #nosec G404 -- deterministic test

	for _, dims := range dimsList {
		t.Run(fmt.Sprintf("dim=%d", dims), func(t *testing.T) {
			a16, b16 := make([]uint16, dims), make([]uint16, dims)
			a32, b32 := make([]uint32, dims), make([]uint32, dims)

			var l2_16, dot16, na16, nb16 float64
			var l2_32, dot32, na32, nb32 float64
			for i := range dims {
				u16a, u16b := uint16(rng.Intn(4096)+1), uint16(rng.Intn(4096)+1)
				a16[i], b16[i] = u16a, u16b
				d16 := float64(u16a) - float64(u16b)
				l2_16 += d16 * d16
				dot16 += float64(u16a) * float64(u16b)
				na16 += float64(u16a) * float64(u16a)
				nb16 += float64(u16b) * float64(u16b)

				u32a, u32b := uint32(rng.Intn(4096)+1), uint32(rng.Intn(4096)+1)
				a32[i], b32[i] = u32a, u32b
				d32 := float64(u32a) - float64(u32b)
				l2_32 += d32 * d32
				dot32 += float64(u32a) * float64(u32b)
				na32 += float64(u32a) * float64(u32a)
				nb32 += float64(u32b) * float64(u32b)
			}
			cos16 := 1.0 - dot16/(math.Sqrt(na16)*math.Sqrt(nb16))
			cos32 := 1.0 - dot32/(math.Sqrt(na32)*math.Sqrt(nb32))

			check := func(name string, got float32, want float64) {
				t.Helper()
				denom := math.Max(1.0, math.Abs(want))
				if math.Abs(float64(got)-want)/denom > 1e-4 {
					t.Errorf("%s: got %v, want %v", name, got, want)
				}
			}

			g, err := EuclideanDistanceUint16(a16, b16)
			if err != nil {
				t.Fatal(err)
			}
			check("uint16/L2", g, math.Sqrt(l2_16))
			g, err = DotProductUint16(a16, b16)
			if err != nil {
				t.Fatal(err)
			}
			check("uint16/Dot", g, dot16)
			g, err = CosineDistanceUint16(a16, b16)
			if err != nil {
				t.Fatal(err)
			}
			check("uint16/Cosine", g, cos16)

			g, err = EuclideanDistanceUint32(a32, b32)
			if err != nil {
				t.Fatal(err)
			}
			check("uint32/L2", g, math.Sqrt(l2_32))
			g, err = DotProductUint32(a32, b32)
			if err != nil {
				t.Fatal(err)
			}
			check("uint32/Dot", g, dot32)
			g, err = CosineDistanceUint32(a32, b32)
			if err != nil {
				t.Fatal(err)
			}
			check("uint32/Cosine", g, cos32)
		})
	}

	// Zero-allocation guard at dim=384.
	a16, b16 := make([]uint16, 384), make([]uint16, 384)
	a32, b32 := make([]uint32, 384), make([]uint32, 384)
	for i := range 384 {
		a16[i], b16[i] = uint16(i+1), uint16(384-i)
		a32[i], b32[i] = uint32(i+1), uint32(384-i)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		_, _ = EuclideanDistanceUint16(a16, b16)
		_, _ = DotProductUint16(a16, b16)
		_, _ = CosineDistanceUint16(a16, b16)
		_, _ = EuclideanDistanceUint32(a32, b32)
		_, _ = DotProductUint32(a32, b32)
		_, _ = CosineDistanceUint32(a32, b32)
	}); allocs != 0 {
		t.Fatalf("uint16/uint32 kernels allocated %.2f allocs/run, want 0", allocs)
	}
}

func BenchmarkUint16Uint32Kernels(b *testing.B) {
	const dims = 384
	a16, b16 := make([]uint16, dims), make([]uint16, dims)
	a32, b32 := make([]uint32, dims), make([]uint32, dims)
	for i := range dims {
		a16[i], b16[i] = uint16(i+1), uint16(dims-i)
		a32[i], b32[i] = uint32(i+1), uint32(dims-i)
	}

	b.Run("uint16_L2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceUint16(a16, b16)
		}
	})
	b.Run("uint16_Dot", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DotProductUint16(a16, b16)
		}
	})
	b.Run("uint32_L2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = EuclideanDistanceUint32(a32, b32)
		}
	})
	b.Run("uint32_Dot", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DotProductUint32(a32, b32)
		}
	})
}
