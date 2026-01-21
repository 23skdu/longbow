//go:build !amd64

package simd

import (
	"errors"
	"unsafe"
)

// Stubs for non-AMD64 architectures to satisfy simd.go references

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) error {
	return matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) error {
	return matchFloat32Generic(src, val, op, dst)
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) error {
	return adcBatchGeneric(table, flatCodes, m, results)
}

func euclideanAVX2(a, b []float32) (float32, error)      { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) (float32, error)    { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) (float32, error)         { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) (float32, error)       { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) (float32, error)            { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) (float32, error)          { return dotGeneric(a, b) }
func euclidean384AVX512(a, b []float32) (float32, error) { return euclideanGeneric(a, b) }
func dot384AVX512(a, b []float32) (float32, error)       { return dotGeneric(a, b) }

func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return dotBatchGeneric(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchGeneric(query, vectors, results)
}
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return cosineBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) error {
	return euclideanBatchGeneric(query, vectors, results)
}

func prefetchNTA(p unsafe.Pointer) {}

func euclideanFloat64AVX2(a, b []float64) (float32, error) {
	return 0, errors.New("avx2 not supported")
}
func euclideanFloat64AVX512(a, b []float64) (float32, error) {
	return 0, errors.New("avx512 not supported")
}
func euclideanInt8AVX2(a, b []int8) (float32, error)   { return 0, errors.New("avx2 not supported") }
func euclideanInt16AVX2(a, b []int16) (float32, error) { return 0, errors.New("avx2 not supported") }

func l2SquaredAVX2(a, b []float32) (float32, error)   { return L2SquaredFloat32(a, b) }
func l2SquaredAVX512(a, b []float32) (float32, error) { return L2SquaredFloat32(a, b) }
