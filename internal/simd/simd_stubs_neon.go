//go:build !arm64 && !amd64

package simd

import (
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func euclideanNEON(a, b []float32) float32    { return euclideanUnrolled4x(a, b) }
func euclidean384NEON(a, b []float32) float32 { return euclideanUnrolled4x(a, b) }
func euclidean128NEON(a, b []float32) float32 { return euclidean128Unrolled4x(a, b) }
func cosineNEON(a, b []float32) float32       { return cosineUnrolled4x(a, b) }
func dotNEON(a, b []float32) float32          { return dotUnrolled4x(a, b) }
func dot384NEON(a, b []float32) float32       { return dotUnrolled4x(a, b) }
func dot128NEON(a, b []float32) float32       { return dot128Unrolled4x(a, b) }

func euclideanBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchUnrolled4x(query, vectors, results)
}
func cosineBatchNEON(query []float32, vectors [][]float32, results []float32) {
	cosineBatchUnrolled4x(query, vectors, results)
}
func dotBatchNEON(query []float32, vectors [][]float32, results []float32) {
	dotBatchUnrolled4x(query, vectors, results)
}
func adcBatchNEON(table []float32, flatCodes []byte, m int, results []float32) {
	adcBatchGeneric(table, flatCodes, m, results)
}
func euclideanVerticalBatchNEON(query []float32, vectors [][]float32, results []float32) {
	euclideanBatchUnrolled4x(query, vectors, results)
}

func euclideanF16NEON(a, b []float16.Num) float32 { return euclideanF16Unrolled4x(a, b) }
func cosineF16NEON(a, b []float16.Num) float32    { return cosineF16Unrolled4x(a, b) }
func dotF16NEON(a, b []float16.Num) float32       { return dotF16Unrolled4x(a, b) }
