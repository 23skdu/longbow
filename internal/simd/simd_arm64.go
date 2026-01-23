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
func euclideanF16NEONKernel(a, b []float16.Num) float32

//go:noescape
func dotF16NEONKernel(a, b []float16.Num) float32

//go:noescape
func cosineF16NEONKernel(a, b []float16.Num) float32

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

func euclideanF16NEON(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanF16NEONKernel(a, b), nil
}

func dotF16NEON(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotF16NEONKernel(a, b), nil
}

func cosineF16NEON(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return cosineF16NEONKernel(a, b), nil
}

// Optimized for 384 dimensions
func euclidean384NEON(a, b []float32) (float32, error) {
	return euclideanNEON(a, b)
}

func euclidean128NEON(a, b []float32) (float32, error) {
	return euclideanNEON(a, b)
}

func dot384NEON(a, b []float32) (float32, error) {
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
	return L2SquaredFloat32(a, b) // Fallback to unrolled Go implementation
}
