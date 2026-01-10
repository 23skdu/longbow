//go:build !amd64

package simd

import (
	"unsafe"
)

// Stubs for non-AMD64 architectures to satisfy simd.go references

func matchInt64AVX2(src []int64, val int64, op CompareOp, dst []byte) {
	matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX2(src []float32, val float32, op CompareOp, dst []byte) {
	matchFloat32Generic(src, val, op, dst)
}

func matchInt64AVX512(src []int64, val int64, op CompareOp, dst []byte) {
	matchInt64Generic(src, val, op, dst)
}

func matchFloat32AVX512(src []float32, val float32, op CompareOp, dst []byte) {
	matchFloat32Generic(src, val, op, dst)
}

func adcBatchAVX2(table []float32, flatCodes []byte, m int, results []float32) {
	adcBatchGeneric(table, flatCodes, m, results)
}

func adcBatchAVX512(table []float32, flatCodes []byte, m int, results []float32) {
	adcBatchGeneric(table, flatCodes, m, results)
}

func euclideanAVX2(a, b []float32) float32      { return euclideanGeneric(a, b) }
func euclideanAVX512(a, b []float32) float32    { return euclideanGeneric(a, b) }
func cosineAVX2(a, b []float32) float32         { return cosineGeneric(a, b) }
func cosineAVX512(a, b []float32) float32       { return cosineGeneric(a, b) }
func dotAVX2(a, b []float32) float32            { return dotGeneric(a, b) }
func dotAVX512(a, b []float32) float32          { return dotGeneric(a, b) }
func euclidean384AVX512(a, b []float32) float32 { return euclideanGeneric(a, b) }
func dot384AVX512(a, b []float32) float32       { return dotGeneric(a, b) }

func euclideanBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}
func euclideanBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}
func dotBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	dotBatchGeneric(query, vectors, results)
}
func dotBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	dotBatchGeneric(query, vectors, results)
}
func cosineBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	cosineBatchGeneric(query, vectors, results)
}
func cosineBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	cosineBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX2(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchAVX512(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}

func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchGeneric(query, vectors, results)
}

func prefetchNTA(p unsafe.Pointer) {}
