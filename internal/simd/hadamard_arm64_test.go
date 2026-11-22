//go:build arm64

package simd

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFastWalshHadamardTransform32NEON runs the test fast walsh hadamard transform32 neon operation.
func TestFastWalshHadamardTransform32NEON(t *testing.T) {
	if !features.HasNEON {
		t.Skip("NEON not available")
	}

	sizes := []int{4, 8, 16, 32, 64, 128, 256, 512, 1024}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			a := make([]float32, n)
			ref := make([]float32, n)
			for i := range a {
				v := rand.Float32()
				a[i] = v
				ref[i] = v
			}

			err := FastWalshHadamardTransform32NEON(a)
			assert.NoError(t, err)

			err = fastWalshHadamardTransform32Generic(ref)
			assert.NoError(t, err)

			for i := range a {
				if !approxEqual(a[i], ref[i], 1e-4) {
					fmt.Printf("FAIL n=%d index=%d NEON=%f Generic=%f\n", n, i, a[i], ref[i])
					t.Fatalf("at index %d: NEON=%f, Generic=%f", i, a[i], ref[i])
				}
			}
		})
	}
}

// TestRandomRotationNEON runs the test random rotation neon operation.
func TestRandomRotationNEON(t *testing.T) {
	if !features.HasNEON {
		t.Skip("NEON not available")
	}

	n := 128
	seed := int64(42)
	a := make([]float32, n)
	ref := make([]float32, n)
	for i := range a {
		v := rand.Float32()
		a[i] = v
		ref[i] = v
	}

	err := RandomRotationNEON(a, seed)
	assert.NoError(t, err)

	err = randomRotationGeneric(ref, seed)
	assert.NoError(t, err)

	for i := range a {
		if !approxEqual(a[i], ref[i], 1e-4) {
			t.Fatalf("at index %d: NEON=%f, Generic=%f", i, a[i], ref[i])
		}
	}
}

// TestL2SquaredNEON runs the test l2 squared neon operation.
func TestL2SquaredNEON(t *testing.T) {
	if !features.HasNEON {
		t.Skip("NEON not available")
	}

	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}
	// (1-5)^2 + (2-6)^2 + (3-7)^2 + (4-8)^2 = 16 + 16 + 16 + 16 = 64

	res, err := l2SquaredNEON(a, b)
	assert.NoError(t, err)
	assert.Equal(t, float32(64.0), res)
}

// BenchmarkFWHT_NEON_512 runs the benchmark fwht_neon_512 operation.
func BenchmarkFWHT_NEON_512(b *testing.B) {
	if !features.HasNEON {
		b.Skip("NEON not available")
	}
	n := 512
	a := make([]float32, n)
	for i := range a {
		a[i] = rand.Float32()
	}
	for b.Loop() {
		_ = FastWalshHadamardTransform32NEON(a)
	}
}

// BenchmarkFWHT_Generic_512 runs the benchmark fwht_generic_512 operation.
func BenchmarkFWHT_Generic_512(b *testing.B) {
	n := 512
	a := make([]float32, n)
	for i := range a {
		a[i] = rand.Float32()
	}
	for b.Loop() {
		_ = fastWalshHadamardTransform32Generic(a)
	}
}
