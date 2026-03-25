package simd

import (
	"errors"
)

// FastWalshHadamardTransform32 implements the In-place Fast Walsh-Hadamard Transform
// for float32 vectors. The length of the vector must be a power of 2.
func FastWalshHadamardTransform32(a []float32) error {
	n := len(a)
	if n == 0 || (n&(n-1)) != 0 {
		return errors.New("simd: vector length must be a power of 2 for FWHT")
	}

	for h := 1; h < n; h <<= 1 {
		for i := 0; i < n; i += h << 1 {
			for j := i; j < i+h; j++ {
				x := a[j]
				y := a[j+h]
				a[j] = x + y
				a[j+h] = x - y
			}
		}
	}

	// Normalization factor: 1/sqrt(n) to keep it orthogonal
	// For TurboQuant, we often skip this since we only care about "spreading"
	// but let's keep it for correctness if requested.
	// factor := float32(1.0 / math.Sqrt(float64(n)))
	// for i := 0; i < n; i++ {
	// 	a[i] *= factor
	// }

	return nil
}

// RandomRotation rotates the vector using a randomized Hadamard transform.
// It applies a random sign flip (diagonal matrix D) followed by FWHT.
// seed is used to generate the deterministic sign flips for reconstruction.
func RandomRotation(a []float32, seed int64) error {
	// 1. Random sign flip (D)
	// We use a simple bit-manipulation based on seed+index for speed
	for i := range a {
		// xorshift-like sign flip
		if ((uint64(seed+int64(i)) * 6364136223846793005) >> 63) == 1 {
			a[i] = -a[i]
		}
	}

	// 2. FWHT (H)
	// If length is not power of 2, we need to pad (handled by caller or here)
	return FastWalshHadamardTransform32(a)
}

// PadToPowerOf2 pads a vector with zeros to the next power of 2.
func PadToPowerOf2(a []float32) []float32 {
	n := len(a)
	if n == 0 {
		return a
	}
	if (n & (n - 1)) == 0 {
		return a
	}
	pow2 := 1
	for pow2 < n {
		pow2 <<= 1
	}
	padded := make([]float32, pow2)
	copy(padded, a)
	return padded
}
