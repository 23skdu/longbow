package simd

import "testing"

// BenchmarkTQPolarReconstructLevel runs the benchmark tq polar reconstruct level operation.
func BenchmarkTQPolarReconstructLevel(b *testing.B) {
	const levelSize = 256
	recon := make([]float32, levelSize*2)
	qIndices := make([]byte, levelSize*2)
	lookup := make([]float32, 32)
	for i := range recon {
		recon[i] = 1.0
		qIndices[i] = byte(i % 16)
	}
	for i := range lookup {
		lookup[i] = 0.5
	}

	b.Run("Generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			TQPolarReconstructLevelGeneric(recon, qIndices, 0, levelSize, lookup)
		}
	})

	b.Run("AVX2", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			TQPolarReconstructLevelAVX2(recon, qIndices, 0, levelSize, lookup)
		}
	})
}
