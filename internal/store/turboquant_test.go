package store

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/stretchr/testify/assert"
)

func TestTurboQuant_EncoderDecoder(t *testing.T) {
	dims := 128
	encoder := NewTurboQuantEncoder(dims, 4, 42)

	// Create a random vector
	vec := make([]float32, dims)
	for i := 0; i < dims; i++ {
		vec[i] = float32(i) / float32(dims)
	}

	// 1. Encode
	encoded, err := encoder.Encode(vec)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// 2. Decode (Rotated)
	rotatedRecon, err := encoder.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// 3. Prepare original rotated vector for comparison
	rotatedOrig := make([]float32, encoder.pow2)
	copy(rotatedOrig, vec)
	if err := simd.RandomRotation(rotatedOrig, 42); err != nil {
		t.Fatalf("RandomRotation failed: %v", err)
	}

	// 4. Compare Dot Product or L2
	// For quantized vectors, we expect some error but high similarity
	dist, _ := simd.L2SquaredFloat32(rotatedOrig, rotatedRecon)
	t.Logf("L2 Squared Error (Rotated): %f", dist)

	// In the local space, the reconstructed vector should be close to the original (rotated)
	// we check if at least it has the same order of magnitude/orientation
	dot, _ := simd.DotProduct(rotatedOrig, rotatedRecon)
	norm1, _ := simd.DotProduct(rotatedOrig, rotatedOrig)
	norm2, _ := simd.DotProduct(rotatedRecon, rotatedRecon)
	cosine := dot / (float32(math.Sqrt(float64(norm1))) * float32(math.Sqrt(float64(norm2))))
	
	t.Logf("Cosine Similarity (Rotated Space): %f", cosine)
	if cosine < 0.9 {
		t.Errorf("Cosine similarity too low: %f", cosine)
	}
}

func TestTurboQuant_CompressionRatio(t *testing.T) {
	dims := 768
	bits := 3
	encoder := NewTurboQuantEncoder(dims, bits, 42)
	
	vec := make([]float32, dims)
	encoded, _ := encoder.Encode(vec)
	
	origSize := dims * 4
	compSize := len(encoded)
	ratio := float64(origSize) / float64(compSize)
	
	fmt.Printf("Original Size: %d bytes\n", origSize)
	fmt.Printf("TurboQuant Size (%d-bit): %d bytes\n", bits, compSize)
	fmt.Printf("Compression Ratio: %.2fx\n", ratio)
	
	// Expect ~6x
	if ratio < 5.0 { // 5.0 is acceptable for 768 due to padding 1024
		t.Errorf("Compression ratio too low: %.2fx", ratio)
	}
}
func TestTurboQuant_ZeroVector(t *testing.T) {
	dims := 128
	encoder := NewTurboQuantEncoder(dims, 4, 42)
	vec := make([]float32, dims)

	encoded, err := encoder.Encode(vec)
	assert.NoError(t, err)

	decoded, err := encoder.Decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, len(vec), len(decoded))
}

func TestTurboQuant_VaryingBitDepths(t *testing.T) {
	dims := 64
	for _, bits := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("%d-bits", bits), func(t *testing.T) {
			encoder := NewTurboQuantEncoder(dims, bits, 42)
			vec := make([]float32, dims)
			for i := range vec {
				vec[i] = rand.Float32()
			}

			encoded, err := encoder.Encode(vec)
			assert.NoError(t, err)

			decoded, err := encoder.Decode(encoded)
			assert.NoError(t, err)
			assert.Equal(t, len(vec), len(decoded))
		})
	}
}

func TestTurboQuant_LargeDimensions(t *testing.T) {
	dims := 1536
	encoder := NewTurboQuantEncoder(dims, 4, 42)
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i) / 1536.0
	}

	encoded, err := encoder.Encode(vec)
	assert.NoError(t, err)
	assert.True(t, len(encoded) > 0)
}
