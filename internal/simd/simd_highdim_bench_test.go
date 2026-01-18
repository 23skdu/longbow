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
			EuclideanDistance(a, bVec)
		}
	})

	// Current Blocked Implementation (Block=1024)
	b.Run("Blocked_1024", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			L2Float32Blocked(a, bVec)
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

// manualBlockedL2 simulates a different block size
func manualBlockedL2(a, b []float32, blockSize int) float32 {
	var sum float32
	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		sum += L2SquaredFloat32(a[i:i+blockSize], b[i:i+blockSize])
	}
	if i < len(a) {
		sum += L2SquaredFloat32(a[i:], b[i:])
	}
	// Simulate Sqrt cost (though trivial compared to loop)
	// We use a dummy sqrt to be fair
	// To avoid import cycle or just keep it simple, checking loop speed.
	return sum
}
