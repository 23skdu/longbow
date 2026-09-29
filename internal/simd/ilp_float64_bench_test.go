//go:build amd64

package simd

import (
	"fmt"
	"math/rand"
	"testing"
)

func ilpBenchData(dims int) ([]float64, []float64) {
	rng := rand.New(rand.NewSource(42))
	va := make([]float64, dims)
	vb := make([]float64, dims)
	for i := range va {
		va[i] = rng.Float64() * 10
		vb[i] = rng.Float64() * 10
	}
	return va, vb
}

func BenchmarkEuclideanFloat64_AVX2_8Way(b *testing.B) {
	for _, dims := range []int{128, 384, 768, 1536} {
		va, vb := ilpBenchData(dims)

		b.Run(fmt.Sprintf("AVX2_8Way/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = euclideanFloat64AVX2(va, vb)
			}
		})
		b.Run(fmt.Sprintf("Go_Unrolled4x/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = euclideanFloat64Unrolled4x(va, vb)
			}
		})
		b.Run(fmt.Sprintf("Dispatch/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = EuclideanDistanceFloat64(va, vb)
			}
		})
		b.Run(fmt.Sprintf("GuardedAVX512/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = euclideanFloat64AVX512(va, vb)
			}
		})
	}
}

func BenchmarkDotFloat64_AVX2_8Way(b *testing.B) {
	for _, dims := range []int{128, 384, 768, 1536} {
		va, vb := ilpBenchData(dims)

		b.Run(fmt.Sprintf("AVX2_8Way/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = dotFloat64AVX2(va, vb)
			}
		})
		b.Run(fmt.Sprintf("Go_Unrolled4x/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = dotFloat64Unrolled4x(va, vb)
			}
		})
	}
}

func BenchmarkEuclideanComplex128_ILP(b *testing.B) {
	for _, dims := range []int{128, 384, 768, 1536} {
		rng := rand.New(rand.NewSource(42))
		va := make([]complex128, dims)
		vb := make([]complex128, dims)
		for i := range va {
			va[i] = complex(rng.Float64()*10, rng.Float64()*10)
			vb[i] = complex(rng.Float64()*10, rng.Float64()*10)
		}

		b.Run(fmt.Sprintf("Dispatch/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = EuclideanDistanceComplex128(va, vb)
			}
		})
		b.Run(fmt.Sprintf("Go_Unrolled/D%d", dims), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = euclideanComplex128Unrolled(va, vb)
			}
		})
	}
}
