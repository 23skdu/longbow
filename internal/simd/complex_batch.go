package simd

import (
	"errors"
	"math"
	"unsafe"
)

// =============================================================================
// Complex Type Batch Operations
//
// Complex64 is reinterpreted as float32 (2x length) for SIMD batch dispatch.
// Complex128 is reinterpreted as float64 (2x length) and computed per-vector
// using the SIMD float64 single-vector kernel.
// =============================================================================

// EuclideanDistanceComplex64Batch computes Euclidean distances between one complex64 query and multiple complex64 vectors.
func EuclideanDistanceComplex64Batch(query []complex64, vectors [][]complex64, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF32 := unsafe.Slice((*float32)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	f32Vecs := make([][]float32, len(vectors))
	for i, v := range vectors {
		if v == nil || len(v) != dims {
			f32Vecs[i] = nil
			continue
		}
		f32Vecs[i] = unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
	}

	return EuclideanDistanceBatch(qF32, f32Vecs, results)
}

// DotProductComplex64Batch computes dot products between one complex64 query and multiple complex64 vectors.
func DotProductComplex64Batch(query []complex64, vectors [][]complex64, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF32 := unsafe.Slice((*float32)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	f32Vecs := make([][]float32, len(vectors))
	for i, v := range vectors {
		if v == nil || len(v) != dims {
			f32Vecs[i] = nil
			continue
		}
		f32Vecs[i] = unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
	}

	return DotProductBatch(qF32, f32Vecs, results)
}

// CosineDistanceComplex64Batch computes cosine distances between one complex64 query and multiple complex64 vectors.
func CosineDistanceComplex64Batch(query []complex64, vectors [][]complex64, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF32 := unsafe.Slice((*float32)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	f32Vecs := make([][]float32, len(vectors))
	for i, v := range vectors {
		if v == nil || len(v) != dims {
			f32Vecs[i] = nil
			continue
		}
		f32Vecs[i] = unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
	}

	return CosineDistanceBatch(qF32, f32Vecs, results)
}

// EuclideanDistanceComplex128Batch computes Euclidean distances between one complex128 query and multiple complex128 vectors.
func EuclideanDistanceComplex128Batch(query []complex128, vectors [][]complex128, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF64 := unsafe.Slice((*float64)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	for i, v := range vectors {
		if v == nil || len(v) != dims {
			results[i] = math.MaxFloat32
			continue
		}
		vF64 := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
		d, _ := EuclideanDistanceFloat64(qF64, vF64)
		results[i] = d
	}
	return nil
}

// DotProductComplex128Batch computes dot products between one complex128 query and multiple complex128 vectors.
func DotProductComplex128Batch(query []complex128, vectors [][]complex128, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF64 := unsafe.Slice((*float64)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	for i, v := range vectors {
		if v == nil || len(v) != dims {
			results[i] = math.MaxFloat32
			continue
		}
		vF64 := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
		d, _ := DotProductF64(qF64, vF64)
		results[i] = d
	}
	return nil
}

// CosineDistanceComplex128Batch computes cosine distances between one complex128 query and multiple complex128 vectors.
func CosineDistanceComplex128Batch(query []complex128, vectors [][]complex128, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	dims := len(query)
	if dims == 0 {
		return nil
	}

	qF64 := unsafe.Slice((*float64)(unsafe.Pointer(&query[0])), dims*2) // #nosec G103

	for i, v := range vectors {
		if v == nil || len(v) != dims {
			results[i] = math.MaxFloat32
			continue
		}
		vF64 := unsafe.Slice((*float64)(unsafe.Pointer(&v[0])), dims*2) // #nosec G103
		d, _ := CosineDistanceFloat64(qF64, vF64)
		results[i] = d
	}
	return nil
}
