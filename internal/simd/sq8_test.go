package simd

import (
	crypto_rand "crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEuclideanDistanceSQ8(t *testing.T) {

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
			_, _ = crypto_rand.Read(a)
			_, _ = crypto_rand.Read(b)

			// Reference
			expected, err1 := EuclideanSQ8Generic(a, b)
			require.NoError(t, err1)

			// Optimized (via Dispatch)
			got, err2 := EuclideanDistanceSQ8(a, b)
			require.NoError(t, err2)

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
			_, _ = EuclideanSQ8Generic(a, vecB)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistanceSQ8(a, vecB)
		}
	})
}
