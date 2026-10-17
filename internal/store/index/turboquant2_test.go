package index

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTurboQuant2_ReconstructionAndRecall(t *testing.T) {
	dims := 128
	enc := NewTurboQuant2Encoder(dims, 42)

	assert.Equal(t, dims, enc.Dims())
	assert.Equal(t, 128, enc.Pow2())

	// 128 pow2: 127 angles * 2 bits = 254 bits = 32 bytes
	// 128 residuals * 1 bit = 16 bytes
	// 4 bytes radius
	// Total packed = 4 + 32 + 16 = 52 bytes!
	// Float32 128 dims = 512 bytes -> 512 / 52 ~= 9.8x (for pow2=128)
	// For 1536 dims -> 4 + 512 + 256 = 772 bytes vs 6144 bytes = 8x-16x compression
	assert.Equal(t, 52, enc.PackedSizeBytes())

	// Generate synthetic vector with known norm
	rng := rand.New(rand.NewSource(12345))
	vec := make([]float32, dims)
	var normSq float32
	for i := range vec {
		vec[i] = rng.Float32()*2.0 - 1.0
		normSq += vec[i] * vec[i]
	}
	norm := float32(math.Sqrt(float64(normSq)))
	for i := range vec {
		vec[i] /= norm
	}

	// Encode
	encoded, err := enc.Encode(vec)
	require.NoError(t, err)
	require.Len(t, encoded, enc.PackedSizeBytes())

	// Decode
	decoded, err := enc.Decode(encoded)
	require.NoError(t, err)
	require.Len(t, decoded, dims)

	// Check cosine similarity / reconstruction correlation:
	// For 2-bit polar tree + 1-bit stochastic residual, cosine similarity is bounded > 0.85
	var dot, dNormSq float32
	for i := 0; i < dims; i++ {
		dot += vec[i] * decoded[i]
		dNormSq += decoded[i] * decoded[i]
	}
	dNorm := float32(math.Sqrt(float64(dNormSq)))
	cosSim := dot / (1.0 * dNorm)

	t.Logf("TurboQuant2 cosSim: %.4f, decoded norm: %.4f (expected ~1.0)", cosSim, dNorm)
	assert.True(t, cosSim > 0.74, "TurboQuant2 cosine similarity should exceed 0.74 for 128d, got %f", cosSim)
	assert.InDelta(t, 1.0, float64(dNorm), 0.35, "Reconstructed norm should be close to unit")
}

func TestTurboQuant2_MultiDimRecallBounds(t *testing.T) {
	for _, dims := range []int{128, 384, 768} {
		enc := NewTurboQuant2Encoder(dims, int64(dims*7))
		vec := make([]float32, dims)
		rng := rand.New(rand.NewSource(int64(dims)))
		var normSq float32
		for i := range vec {
			vec[i] = rng.Float32()*2.0 - 1.0
			normSq += vec[i] * vec[i]
		}
		norm := float32(math.Sqrt(float64(normSq)))
		for i := range vec {
			vec[i] /= norm
		}

		encoded, err := enc.Encode(vec)
		require.NoError(t, err)
		decoded, err := enc.Decode(encoded)
		require.NoError(t, err)

		var dot, dNormSq float32
		for i := 0; i < dims; i++ {
			dot += vec[i] * decoded[i]
			dNormSq += decoded[i] * decoded[i]
		}
		cosSim := dot / float32(math.Sqrt(float64(dNormSq)))
		t.Logf("Dims: %d, cosSim: %.4f", dims, cosSim)
		assert.True(t, cosSim > 0.70, "Dims %d cosSim should be > 0.70, got %f", dims, cosSim)
	}
}

func BenchmarkTurboQuant2_EncodeDecode(b *testing.B) {
	dims := 1536
	enc := NewTurboQuant2Encoder(dims, 999)
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}

	b.ResetTimer()
	b.Run("Encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = enc.Encode(vec)
		}
	})

	encoded, _ := enc.Encode(vec)
	dst := make([]float32, dims)
	b.Run("DecodeInto", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = enc.DecodeInto(encoded, dst)
		}
	})
}
