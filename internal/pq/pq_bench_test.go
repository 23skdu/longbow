package pq

import (
	"math/rand"
	"testing"
	"time"
)

func BenchmarkPQ_1536d(b *testing.B) {
	dim := 1536
	m := 192 // 1536 divided by 8 is 192
	k := 256
	numSamples := 1000

	// Generate random vectors
	vectors := make([][]float32, numSamples)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numSamples; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	b.Run("Train", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			encoder, _ := NewPQEncoder(dim, m, k)
			_ = encoder.Train(vectors)
		}
	})

	encoder, _ := NewPQEncoder(dim, m, k)
	_ = encoder.Train(vectors)

	vector := vectors[0]

	b.Run("Encode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = encoder.Encode(vector)
		}
	})

	b.Run("BuildADCTable", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = encoder.BuildADCTable(vector)
		}
	})
}
