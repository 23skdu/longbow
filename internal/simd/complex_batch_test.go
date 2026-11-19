package simd

import (
	"math"
	"math/rand"
	"testing"
)

func scalarEuclideanComplex128(a, b []complex128) float32 {
	var sum float64
	for i := range a {
		dr := real(a[i]) - real(b[i])
		di := imag(a[i]) - imag(b[i])
		sum += dr*dr + di*di
	}
	return float32(math.Sqrt(sum))
}

func scalarDotComplex128(a, b []complex128) float32 {
	var sum float64
	for i := range a {
		sum += real(a[i])*real(b[i]) + imag(a[i])*imag(b[i])
	}
	return float32(sum)
}

func scalarCosineComplex128(a, b []complex128) float32 {
	var dot, na, nb float64
	for i := range a {
		ar, ai := real(a[i]), imag(a[i])
		br, bi := real(b[i]), imag(b[i])
		dot += ar*br + ai*bi
		na += ar*ar + ai*ai
		nb += br*br + bi*bi
	}
	if na == 0 || nb == 0 {
		return 1.0
	}
	return float32(1.0 - dot/(math.Sqrt(na)*math.Sqrt(nb)))
}

// TestComplexBatchZeroAlloc verifies zero-allocation execution and numerical
// parity for complex64 and complex128 batch distance kernels (roadmap A3 & B2).
func TestComplexBatchZeroAlloc(t *testing.T) {
	const (
		dims   = 128
		nBatch = 32
	)
	rng := rand.New(rand.NewSource(20261118)) // #nosec G404 -- deterministic test vectors

	q128 := make([]complex128, dims)
	q64 := make([]complex64, dims)
	for i := range dims {
		re, im := rng.Float64()*2-1, rng.Float64()*2-1
		q128[i] = complex(re, im)
		q64[i] = complex(float32(re), float32(im))
	}

	vecs128 := make([][]complex128, nBatch)
	vecs64 := make([][]complex64, nBatch)
	for i := range nBatch {
		v128 := make([]complex128, dims)
		v64 := make([]complex64, dims)
		for j := range dims {
			re, im := rng.Float64()*2-1, rng.Float64()*2-1
			v128[j] = complex(re, im)
			v64[j] = complex(float32(re), float32(im))
		}
		vecs128[i] = v128
		vecs64[i] = v64
	}

	res := make([]float32, nBatch)
	f64Scratch := make([][]float64, nBatch)
	f32Scratch := make([][]float32, nBatch)

	t.Run("EuclideanComplex128Batch", func(t *testing.T) {
		if err := EuclideanDistanceComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range vecs128 {
			want := scalarEuclideanComplex128(q128, v)
			if diff := math.Abs(float64(res[i] - want)); diff > 1e-4 {
				t.Fatalf("idx %d: got %v, want %v (diff %v)", i, res[i], want, diff)
			}
		}
		allocs := testing.AllocsPerRun(200, func() {
			if err := EuclideanDistanceComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
		if allocs != 0 {
			t.Fatalf("EuclideanDistanceComplex128Batch allocated %.2f allocs/run, want 0", allocs)
		}
	})

	t.Run("DotProductComplex128Batch", func(t *testing.T) {
		if err := DotProductComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range vecs128 {
			want := scalarDotComplex128(q128, v)
			if diff := math.Abs(float64(res[i] - want)); diff > 1e-4 {
				t.Fatalf("idx %d: got %v, want %v (diff %v)", i, res[i], want, diff)
			}
		}
		allocs := testing.AllocsPerRun(200, func() {
			if err := DotProductComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
		if allocs != 0 {
			t.Fatalf("DotProductComplex128Batch allocated %.2f allocs/run, want 0", allocs)
		}
	})

	t.Run("CosineDistanceComplex128Batch", func(t *testing.T) {
		if err := CosineDistanceComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, v := range vecs128 {
			want := scalarCosineComplex128(q128, v)
			if diff := math.Abs(float64(res[i] - want)); diff > 1e-4 {
				t.Fatalf("idx %d: got %v, want %v (diff %v)", i, res[i], want, diff)
			}
		}
		allocs := testing.AllocsPerRun(200, func() {
			if err := CosineDistanceComplex128Batch(q128, vecs128, res, f64Scratch); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
		if allocs != 0 {
			t.Fatalf("CosineDistanceComplex128Batch allocated %.2f allocs/run, want 0", allocs)
		}
	})

	t.Run("Complex64BatchesZeroAlloc", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			fn   func([]complex64, [][]complex64, []float32, [][]float32) error
		}{
			{"Euclidean", EuclideanDistanceComplex64Batch},
			{"Dot", DotProductComplex64Batch},
			{"Cosine", CosineDistanceComplex64Batch},
		} {
			allocs := testing.AllocsPerRun(200, func() {
				if err := tc.fn(q64, vecs64, res, f32Scratch); err != nil {
					t.Fatalf("%s error: %v", tc.name, err)
				}
			})
			if allocs != 0 {
				t.Fatalf("%sComplex64Batch allocated %.2f allocs/run, want 0", tc.name, allocs)
			}
		}
	})
}
