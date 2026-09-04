package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/mathutil"
)

func TestEmlgoParity_DistancesFloat64(t *testing.T) {
	dimsList := []int{128, 384, 768, 1536}
	rng := rand.New(rand.NewSource(12345))

	for _, dims := range dimsList {
		a := make([]float64, dims)
		b := make([]float64, dims)
		for i := 0; i < dims; i++ {
			a[i] = rng.Float64()
			b[i] = rng.Float64()
		}

		// Euclidean
		mathutil.SetBackend(mathutil.BackendStandard)
		stdEuc, err := EuclideanDistanceFloat64(a, b)
		if err != nil {
			t.Fatalf("dims %d EuclideanDistanceFloat64 standard failed: %v", dims, err)
		}

		mathutil.SetBackend(mathutil.BackendEML)
		emlEuc, err := EuclideanDistanceFloat64(a, b)
		if err != nil {
			t.Fatalf("dims %d EuclideanDistanceFloat64 EML failed: %v", dims, err)
		}

		if math.Abs(float64(stdEuc-emlEuc)) > 1e-5 {
			t.Errorf("dims %d Euclidean mismatch: std=%f, eml=%f, diff=%e", dims, stdEuc, emlEuc, math.Abs(float64(stdEuc-emlEuc)))
		}

		// Cosine
		mathutil.SetBackend(mathutil.BackendStandard)
		stdCos, err := CosineDistanceFloat64(a, b)
		if err != nil {
			t.Fatalf("dims %d CosineDistanceFloat64 standard failed: %v", dims, err)
		}

		mathutil.SetBackend(mathutil.BackendEML)
		emlCos, err := CosineDistanceFloat64(a, b)
		if err != nil {
			t.Fatalf("dims %d CosineDistanceFloat64 EML failed: %v", dims, err)
		}

		if math.Abs(float64(stdCos-emlCos)) > 1e-5 {
			t.Errorf("dims %d Cosine mismatch: std=%f, eml=%f, diff=%e", dims, stdCos, emlCos, math.Abs(float64(stdCos-emlCos)))
		}
	}
}

func benchmarkEuclideanFloat64(b *testing.B, dims int) {
	rng := rand.New(rand.NewSource(42))
	vecA := make([]float64, dims)
	vecB := make([]float64, dims)
	for i := range vecA {
		vecA[i] = rng.Float64()
		vecB[i] = rng.Float64()
	}

	b.Run(fmt.Sprintf("Standard_Euclidean_D%d", dims), func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistanceFloat64(vecA, vecB)
		}
	})

	b.Run(fmt.Sprintf("EMLGo_Euclidean_D%d", dims), func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistanceFloat64(vecA, vecB)
		}
	})
}

func BenchmarkAB_Distance_Euclidean_128(b *testing.B)  { benchmarkEuclideanFloat64(b, 128) }
func BenchmarkAB_Distance_Euclidean_384(b *testing.B)  { benchmarkEuclideanFloat64(b, 384) }
func BenchmarkAB_Distance_Euclidean_768(b *testing.B)  { benchmarkEuclideanFloat64(b, 768) }
func BenchmarkAB_Distance_Euclidean_1536(b *testing.B) { benchmarkEuclideanFloat64(b, 1536) }

func benchmarkCosineFloat64(b *testing.B, dims int) {
	rng := rand.New(rand.NewSource(42))
	vecA := make([]float64, dims)
	vecB := make([]float64, dims)
	for i := range vecA {
		vecA[i] = rng.Float64()
		vecB[i] = rng.Float64()
	}

	b.Run(fmt.Sprintf("Standard_Cosine_D%d", dims), func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendStandard)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = CosineDistanceFloat64(vecA, vecB)
		}
	})

	b.Run(fmt.Sprintf("EMLGo_Cosine_D%d", dims), func(b *testing.B) {
		mathutil.SetBackend(mathutil.BackendEML)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = CosineDistanceFloat64(vecA, vecB)
		}
	})
}

func BenchmarkAB_Distance_Cosine_128(b *testing.B)  { benchmarkCosineFloat64(b, 128) }
func BenchmarkAB_Distance_Cosine_384(b *testing.B)  { benchmarkCosineFloat64(b, 384) }
func BenchmarkAB_Distance_Cosine_768(b *testing.B)  { benchmarkCosineFloat64(b, 768) }
func BenchmarkAB_Distance_Cosine_1536(b *testing.B) { benchmarkCosineFloat64(b, 1536) }
