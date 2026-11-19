package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// TestFloat16Int8QuantizedParity verifies float16 and int8 SIMD kernels
// (AVX2 / AVX-512 dispatch) against exact float64 scalar references across
// aligned and tail dimensions (roadmap B3).
func TestFloat16Int8QuantizedParity(t *testing.T) {
	dimsList := []int{1, 7, 8, 15, 16, 31, 64, 128, 384, 768}
	rng := rand.New(rand.NewSource(202611182)) // #nosec G404 -- deterministic test

	for _, dims := range dimsList {
		t.Run(fmt.Sprintf("dim=%d", dims), func(t *testing.T) {
			aF16, bF16 := make([]float16.Num, dims), make([]float16.Num, dims)
			aI8, bI8 := make([]int8, dims), make([]int8, dims)

			var l2F16, dotF16 float64
			var l2I8, dotI8, naI8, nbI8 float64
			for i := range dims {
				af := float32(rng.Float64()*2 - 1)
				bf := float32(rng.Float64()*2 - 1)
				aF16[i], bF16[i] = float16.New(af), float16.New(bf)
				va, vb := float64(aF16[i].Float32()), float64(bF16[i].Float32())
				l2F16 += (va - vb) * (va - vb)
				dotF16 += va * vb

				ai := int8(rng.Intn(200) - 100)
				bi := int8(rng.Intn(200) - 100)
				if ai == 0 {
					ai = 1
				}
				if bi == 0 {
					bi = 1
				}
				aI8[i], bI8[i] = ai, bi
				di := float64(int(ai) - int(bi))
				l2I8 += di * di
				dotI8 += float64(ai) * float64(bi)
				naI8 += float64(ai) * float64(ai)
				nbI8 += float64(bi) * float64(bi)
			}
			cosI8 := 1.0 - dotI8/(math.Sqrt(naI8)*math.Sqrt(nbI8))

			check := func(name string, got float32, want, tol float64) {
				t.Helper()
				denom := math.Max(1.0, math.Abs(want))
				if math.Abs(float64(got)-want)/denom > tol {
					t.Errorf("%s: got %v, want %v", name, got, want)
				}
			}

			g, err := EuclideanDistanceF16(aF16, bF16)
			if err != nil {
				t.Fatal(err)
			}
			check("float16/L2", g, math.Sqrt(l2F16), 1e-3)
			g, err = DotProductF16(aF16, bF16)
			if err != nil {
				t.Fatal(err)
			}
			check("float16/Dot", g, dotF16, 1e-3)

			g, err = EuclideanDistanceInt8(aI8, bI8)
			if err != nil {
				t.Fatal(err)
			}
			check("int8/L2", g, math.Sqrt(l2I8), 1e-4)
			g, err = DotProductInt8(aI8, bI8)
			if err != nil {
				t.Fatal(err)
			}
			check("int8/Dot", g, dotI8, 1e-4)
			g, err = CosineDistanceInt8(aI8, bI8)
			if err != nil {
				t.Fatal(err)
			}
			check("int8/Cosine", g, cosI8, 1e-4)
		})
	}

	aF16, bF16 := make([]float16.Num, 384), make([]float16.Num, 384)
	aI8, bI8 := make([]int8, 384), make([]int8, 384)
	for i := range 384 {
		aF16[i], bF16[i] = float16.New(float32(i)*0.01), float16.New(float32(384-i)*0.01)
		aI8[i], bI8[i] = int8(i%100+1), int8((384-i)%100+1)
	}
	if allocs := testing.AllocsPerRun(200, func() {
		_, _ = EuclideanDistanceF16(aF16, bF16)
		_, _ = DotProductF16(aF16, bF16)
		_, _ = EuclideanDistanceInt8(aI8, bI8)
		_, _ = DotProductInt8(aI8, bI8)
		_, _ = CosineDistanceInt8(aI8, bI8)
	}); allocs != 0 {
		t.Fatalf("float16/int8 kernels allocated %.2f allocs/run, want 0", allocs)
	}
}
