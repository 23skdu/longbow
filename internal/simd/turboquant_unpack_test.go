package simd

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnpackTQ4_MatchesGeneric(t *testing.T) {
	for _, n := range []int{8, 16, 32, 64, 128, 256, 512, 1024} {
		src := make([]byte, n/2)
		rng := rand.New(rand.NewSource(int64(n)))
		rng.Read(src)

		dstGen := make([]float32, n)
		dstAPI := make([]float32, n)

		scale := float32(2.0 * math.Pi / 15.0)
		bias := -float32(math.Pi)

		UnpackTQ4Generic(src, dstGen, scale, bias)
		UnpackTQ4(src, dstAPI, scale, bias)

		for i := 0; i < n; i++ {
			assert.InDelta(t, dstGen[i], dstAPI[i], 1e-4, "Mismatch at n=%d index %d", n, i)
		}
	}
}

func TestUnpackTQ2_MatchesGeneric(t *testing.T) {
	for _, n := range []int{8, 16, 32, 64, 128, 256, 512, 1024} {
		src := make([]byte, n/4)
		rng := rand.New(rand.NewSource(int64(n)))
		rng.Read(src)

		dstGen := make([]float32, n)
		dstAPI := make([]float32, n)

		scale := float32(2.0 * math.Pi / 3.0)
		bias := -float32(math.Pi)

		UnpackTQ2Generic(src, dstGen, scale, bias)
		UnpackTQ2(src, dstAPI, scale, bias)

		for i := 0; i < n; i++ {
			assert.InDelta(t, dstGen[i], dstAPI[i], 1e-4, "Mismatch at n=%d index %d", n, i)
		}
	}
}

func TestUnpackTQ8_MatchesGeneric(t *testing.T) {
	for _, n := range []int{8, 16, 32, 64, 128, 256, 512, 1024} {
		src := make([]byte, n)
		rng := rand.New(rand.NewSource(int64(n)))
		rng.Read(src)

		dstGen := make([]float32, n)
		dstAPI := make([]float32, n)

		scale := float32(2.0 * math.Pi / 255.0)
		bias := -float32(math.Pi)

		UnpackTQ8Generic(src, dstGen, scale, bias)
		UnpackTQ8(src, dstAPI, scale, bias)

		for i := 0; i < n; i++ {
			assert.InDelta(t, dstGen[i], dstAPI[i], 1e-4, "Mismatch at n=%d index %d", n, i)
		}
	}
}

func BenchmarkUnpackTQ4(b *testing.B) {
	dim := 128
	src := make([]byte, dim/2)
	dst := make([]float32, dim)
	scale := float32(2.0 * math.Pi / 15.0)
	bias := -float32(math.Pi)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnpackTQ4(src, dst, scale, bias)
	}
}

func BenchmarkUnpackTQ2(b *testing.B) {
	dim := 128
	src := make([]byte, dim/4)
	dst := make([]float32, dim)
	scale := float32(2.0 * math.Pi / 3.0)
	bias := -float32(math.Pi)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnpackTQ2(src, dst, scale, bias)
	}
}
