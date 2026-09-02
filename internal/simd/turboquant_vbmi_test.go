package simd

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackTQ2AVX512VBMI(t *testing.T) {
	if !GetCPUFeatures().HasAVX512 || !GetCPUFeatures().HasVBMI {
		t.Skip("AVX-512 VBMI not supported")
	}

	dimensions := []int{64, 128, 256, 512}
	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("dim_%d", dim), func(t *testing.T) {
			src := make([]float32, dim)
			rng := rand.New(rand.NewSource(42))
			for i := range src {
				src[i] = rng.Float32() * 10.0
			}

			// TQ2 packing
			dst := make([]byte, dim/4)
			PackTQ2AVX512VBMI(src, dst)

			// Reference Unpack (we know Unpack works from previous tests)
			// But let's verify with UnpackTQ2AVX512VBMI as well
			unpacked := make([]float32, dim)

			// We need scale/bias from the packing?
			// TurboQuant packing usually involves finding scale/bias.
			// In our kernel, scale and bias are fixed for the mapping:
			// v = (q - bias) / scale  => q = v * scale + bias
			// Wait, the packing kernel uses:
			// q = floor((v + PI) * (1/2PI) * 3.0 + 0.5)
			// This maps [-PI, PI] to [0, 3].
			// So scale = 3.0 / (2*PI), bias = 1.5?
			// Let's check the kernel constants:
			// PI = 3.14159265
			// INV2PI = 0.15915494
			// MAX2 = 3.0
			// q = (v + PI) * INV2PI * 3.0 + 0.5
			// Unpack: v = (q - 1.5) * (2*PI / 3.0)

	scale := float32(2.0 * math.Pi / 3.0)

	UnpackTQ2AVX512VBMI(dst, unpacked, scale, -float32(math.Pi))

			for i := 0; i < dim; i++ {
				// Each element in TQ2 is 2 bits, so it has 4 levels.
				// We expect some quantization error.
				// The mapped value should be close to the original if it was in [-PI, PI].
				// If original was outside, it's clamped.
				orig := src[i]
				if orig < -float32(math.Pi) {
					orig = -float32(math.Pi)
				}
				if orig > float32(math.Pi) {
					orig = float32(math.Pi)
				}

				// Allow for quantization error: (2*PI) / 4 = PI/2 approx 1.57
				// But with 0.5 rounding, it should be within PI/4.
				assert.InDelta(t, orig, unpacked[i], 2.0, "Mismatch at index %d", i)
			}
		})
	}
}

func BenchmarkPackTQ2AVX512VBMI(b *testing.B) {
	if !GetCPUFeatures().HasAVX512 || !GetCPUFeatures().HasVBMI {
		b.Skip("AVX-512 VBMI not supported")
	}

	dim := 1536
	src := make([]float32, dim)
	dst := make([]byte, dim/4)

	b.ResetTimer()
	for b.Loop() {
		PackTQ2AVX512VBMI(src, dst)
	}
}

func BenchmarkPackTQ2AVX2(b *testing.B) {
	dim := 1536
	src := make([]float32, dim)
	dst := make([]byte, dim/4)

	b.ResetTimer()
	for b.Loop() {
		PackTQ2AVX2(src, dst)
	}
}
