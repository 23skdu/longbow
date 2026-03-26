//go:build arm64

package simd

// FastWalshHadamardTransform32NEON falls back to generic for now to ensure correctness
// while we resolve assembly instruction issues.
func FastWalshHadamardTransform32NEON(a []float32) error {
	return fastWalshHadamardTransform32Generic(a)
}

// RandomRotationNEON falls back to generic.
func RandomRotationNEON(a []float32, seed int64) error {
	return randomRotationGeneric(a, seed)
}
