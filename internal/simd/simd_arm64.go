//go:build arm64

package simd

import (
	"errors"
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ARM64 NEON implementations
// Defined in simd_arm64.s

// Internal assembly kernels (return single value for machine code compatibility)
//
//go:noescape
func euclideanNEONKernel(a, b []float32) float32

//go:noescape
func dotNEONKernel(a, b []float32) float32

//go:noescape
func l2SquaredNEONKernel(a, b []float32) float32

//go:noescape
func euclideanF16NEONKernel(a, b []float16.Num) float32 //nolint:unused

//go:noescape
func dotF16NEONKernel(a, b []float16.Num) float32 //nolint:unused

//go:noescape
func cosineF16NEONKernel(a, b []float16.Num) float32 //nolint:unused

//go:noescape
func fastWalshHadamardTransform32NEONKernel(a []float32)

//go:noescape
func vectorButterflyNEONKernel(a, b []float32)

// Public Go wrappers (with error propagation)

func euclideanNEON(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanNEONKernel(a, b), nil
}

func dotNEON(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotNEONKernel(a, b), nil
}

// Optimized for 384 dimensions - use generic NEON kernel which is SIMD-optimized
func euclidean384NEON(a, b []float32) (float32, error) {
	return euclideanNEON(a, b)
}

// Optimized for 768 dimensions - use generic NEON kernel which is SIMD-optimized
func euclidean768NEON(a, b []float32) (float32, error) {
	return euclideanNEON(a, b)
}

// Optimized for 1536 dimensions - use generic NEON kernel which is SIMD-optimized
func euclidean1536NEON(a, b []float32) (float32, error) {
	return euclideanNEON(a, b)
}

func euclidean128NEON(a, b []float32) (float32, error) {
	return euclidean128Unrolled4x(a, b)
}

func dot384NEON(a, b []float32) (float32, error) {
	return dotNEON(a, b)
}

func dot768NEON(a, b []float32) (float32, error) {
	return dotNEON(a, b)
}

func dot1536NEON(a, b []float32) (float32, error) {
	return dotNEON(a, b)
}

func dot128NEON(a, b []float32) (float32, error) {
	return dotNEON(a, b)
}

// Cosine is still generic for now (or combine Dot / Norms later)
func cosineNEON(a, b []float32) (float32, error) {
	if !features.HasNEON {
		return cosineGeneric(a, b)
	}

	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}

	var dot, normA, normB float32
	n := len(a)
	i := 0

	for ; i <= n-4; i += 4 {
		dot += a[i]*b[i] + a[i+1]*b[i+1] + a[i+2]*b[i+2] + a[i+3]*b[i+3]
		normA += a[i]*a[i] + a[i+1]*a[i+1] + a[i+2]*a[i+2] + a[i+3]*a[i+3]
		normB += b[i]*b[i] + b[i+1]*b[i+1] + b[i+2]*b[i+2] + b[i+3]*b[i+3]
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

func adcBatchNEON(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}
func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := euclideanNEON(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func dotBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := dotNEON(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := cosineNEON(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchNEON(query, vectors, results)
}

func l2SquaredNEON(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return l2SquaredNEONKernel(a, b), nil
}

func FastWalshHadamardTransform32NEON(a []float32) error {
	n := len(a)
	if n == 0 || (n&(n-1)) != 0 {
		return errors.New("simd: vector length must be a power of 2 for FWHT")
	}

	// For sizes < 32, use generic
	if n < 32 {
		return fastWalshHadamardTransform32Generic(a)
	}

	// 1. Initial stages h=1, 2, 4, 8, 16 using the 32-element kernel
	for i := 0; i < n; i += 32 {
		fastWalshHadamardTransform32NEONKernel(a[i : i+32])
	}

	// 2. Larger stages h=32, 64... using vector butterfly kernel
	if n > 32 {
		for h := 32; h < n; h <<= 1 {
			for i := 0; i < n; i += h << 1 {
				for j := i; j < i+h; j += 4 {
					vectorButterflyNEONKernel(a[j:j+4], a[j+h:j+h+4])
				}
			}
		}
	}

	return nil
}

func RandomRotationNEON(a []float32, seed int64) error {
	// 1. Random sign flip (D)
	for i := range a {
		// xorshift-like sign flip
		if ((uint64(seed+int64(i)) * 6364136223846793005) >> 63) == 1 {
			a[i] = -a[i]
		}
	}

	// 2. FWHT (H)
	return FastWalshHadamardTransform32NEON(a)
}
