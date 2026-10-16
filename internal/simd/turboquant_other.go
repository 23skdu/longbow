//go:build !amd64

package simd

// TQPolarReconstructLevelAVX2 falls back to the generic 4-lane unrolled loop on non-amd64 architectures.
func TQPolarReconstructLevelAVX2(recon []float32, qIndices []byte, angleOffset, levelSize int, lookup []float32) {
	TQPolarReconstructLevelGeneric(recon, qIndices, angleOffset, levelSize, lookup)
}
