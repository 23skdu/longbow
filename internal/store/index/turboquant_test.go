package index

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
	if err := simd.RandomRotation(rotatedOrig, encoder.params.Seed); err != nil {
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

	assert.Greater(t, cosine, float32(0.90), "Reconstructed vector must maintain high cosine similarity (>0.90)")
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

// TestTurboQuant_OddBitPackUnpack exercises the bit-accumulator fallback
// (bits 1,3,5,6,7) with a pack→unpack round-trip that must stay within one
// quantization step of the input.
func TestTurboQuant_OddBitPackUnpack(t *testing.T) {
	const dims = 64
	for _, bits := range []int{1, 3, 5, 6, 7} {
		t.Run(fmt.Sprintf("%d-bits", bits), func(t *testing.T) {
			enc := NewTurboQuantEncoder(dims, bits, 42)
			maxVal := float32((uint32(1) << bits) - 1)
			step := (2 * math.Pi) / float64(maxVal)

			// Deterministic angles across the representable range.
			n := enc.pow2 - 1
			angles := make([]float32, n)
			for i := range angles {
				angles[i] = float32(-math.Pi) + float32(2*math.Pi)*float32(i)/float32(n-1)
			}

			angleBytes := (n*bits + 7) / 8
			packed := make([]byte, angleBytes)
			enc.packAngles(angles, packed)

			out := make([]float32, n)
			enc.unpackAngles(packed, out)

			// Within one quantization step (unpack quantizes to the same grid).
			tolerance := float64(step) + 1e-6
			for i := range angles {
				diff := math.Abs(float64(angles[i]) - float64(out[i]))
				assert.LessOrEqualf(t, diff, tolerance,
					"idx %d: got %v want %v (diff %v > step %v)",
					i, out[i], angles[i], diff, step)
			}
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

func FuzzTurboQuantEncodeDecode(f *testing.F) {
	f.Fuzz(func(t *testing.T, dim int, bits int, seed int64) {
		if dim <= 0 || dim > 8192 || bits < 2 || bits > 8 {
			t.Skip()
		}

		encoder := NewTurboQuantEncoder(dim, bits, seed)
		rng := rand.New(rand.NewSource(seed))

		vec := make([]float32, dim)
		for i := 0; i < dim; i++ {
			vec[i] = rng.Float32()
		}

		encoded, err := encoder.Encode(vec)
		if err != nil {
			t.Skip()
		}

		decoded, err := encoder.Decode(encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if len(decoded) != dim {
			t.Fatalf("Dimension mismatch: got %d, want %d", len(decoded), dim)
		}
	})
}

func FuzzTurboQuantCompression(f *testing.F) {
	f.Fuzz(func(t *testing.T, dim int, bits int) {
		if dim <= 0 || dim > 8192 || bits < 2 || bits > 8 {
			t.Skip()
		}

		encoder := NewTurboQuantEncoder(dim, bits, 42)
		vec := make([]float32, dim)

		encoded, err := encoder.Encode(vec)
		if err != nil {
			t.Skip()
		}

		origSize := dim * 4
		compSize := len(encoded)
		ratio := float64(origSize) / float64(compSize)

		if ratio < 1.0 {
			t.Fatalf("Compression ratio too low: %.2fx", ratio)
		}
	})
}

func BenchmarkTurboQuant_Encode(b *testing.B) {
	dims := 128
	for _, bits := range []int{4, 3} {
		b.Run(fmt.Sprintf("bits%d", bits), func(b *testing.B) {
			enc := NewTurboQuantEncoder(dims, bits, 42)
			vec := make([]float32, dims)
			for i := range vec {
				vec[i] = float32(i) / float32(dims)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := enc.Encode(vec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTurboQuant_Decode(b *testing.B) {
	dims := 128
	for _, bits := range []int{4, 3} {
		b.Run(fmt.Sprintf("bits%d", bits), func(b *testing.B) {
			enc := NewTurboQuantEncoder(dims, bits, 42)
			vec := make([]float32, dims)
			for i := range vec {
				vec[i] = float32(i) / float32(dims)
			}
			encoded, err := enc.Encode(vec)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := enc.Decode(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
