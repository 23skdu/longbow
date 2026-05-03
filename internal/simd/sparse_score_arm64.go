//go:build arm64
// +build arm64

package simd

import (
	"unsafe"
)

//go:noescape
func bm25ScoreBatchNEON(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)

func bm25ScoreBatchArch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	n := len(tfs)
	if n == 0 {
		return nil
	}
	
	results := make([]float32, n)
	if avgDL == 0 {
		avgDL = 1.0
	}
	
	bm25ScoreBatchNEON(
		unsafe.Pointer(&tfs[0]),         // #nosec G103
		unsafe.Pointer(&docLengths[0]),  // #nosec G103
		n,
		1.0/avgDL,
		idf,
		k1,
		b,
		unsafe.Pointer(&results[0]),     // #nosec G103
	)
	
	return results
}
