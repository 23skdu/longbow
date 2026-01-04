package simd

import (
	crypto_rand "crypto/rand"
	"math/rand"
	"testing"
	"time"
)

func TestEuclideanDistanceSQ8(t *testing.T) {
	// Seed RNG
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	tests := []struct {
		name string
		size int
	}{
		{"Empty", 0},
		{"Small", 5},
		{"Medium", 32},
		{"Large", 1024},
		{"Odd", 33},
		{"Prime", 127},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := make([]byte, tt.size)
			b := make([]byte, tt.size)
			rng.Read(a)
			rng.Read(b)

			// Reference
			expected := EuclideanSQ8Generic(a, b)

			// Optimized (via Dispatch)
			got := EuclideanDistanceSQ8(a, b)

			if got != expected {
				t.Errorf("expected %d, got %d", expected, got)
			}
		})
	}
}

func BenchmarkEuclideanDistanceSQ8(b *testing.B) {
	size := 1024
	a := make([]byte, size)
	vecB := make([]byte, size)
	_, _ = crypto_rand.Read(a)
	_, _ = crypto_rand.Read(vecB)

	b.Run("Generic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EuclideanSQ8Generic(a, vecB)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			EuclideanDistanceSQ8(a, vecB)
		}
	})
}
