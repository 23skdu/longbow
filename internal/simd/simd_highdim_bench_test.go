package simd

import (
	"testing"
)

func BenchmarkL2Float32_3072(b *testing.B) {
	dim := 3072
	a := make([]float32, dim)
	bVec := make([]float32, dim)

	// Baseline: Standard L2 (AVX2/AVX-512 depending on arch)
	b.Run("Baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = EuclideanDistance(a, bVec)
		}
	})

	// Current Blocked Implementation (Block=1024)
	b.Run("Blocked_1024", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = L2Float32Blocked(a, bVec)
		}
	})

	// Test Tiled implementation (simulate manually for now)
	b.Run("ManuallyTiled_512", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			manualBlockedL2(a, bVec, 512)
		}
	})

	b.Run("ManuallyTiled_256", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			manualBlockedL2(a, bVec, 256)
		}
	})
}

var sink float32

// manualBlockedL2 simulates a different block size
func manualBlockedL2(a, b []float32, blockSize int) {
	var sum float32
	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		res, _ := L2SquaredFloat32(a[i:i+blockSize], b[i:i+blockSize])
		sum += res
	}
	if i < len(a) {
		res, _ := L2SquaredFloat32(a[i:], b[i:])
		sum += res
	}
	sink = sum
}
