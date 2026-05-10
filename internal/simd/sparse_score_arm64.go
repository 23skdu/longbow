//go:build arm64
// +build arm64

package simd

import "unsafe"

// func bm25ScoreBatchNEON(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
func bm25ScoreBatchNEON(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)

func bm25ScoreBatchArch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	if len(tfs) == 0 {
		return nil
	}
	
	n := len(tfs)
	results := make([]float32, n)
	
	// The assembly kernel expects 32-bit integers.
	// On ARM64, 'int' is 64-bit, so we must convert.
	tfs32 := make([]int32, n)
	docLengths32 := make([]int32, n)
	for i := 0; i < n; i++ {
		tfs32[i] = int32(tfs[i])         // #nosec G115
		docLengths32[i] = int32(docLengths[i]) // #nosec G115
	}

	invAvgDL := float32(1.0)
	if avgDL != 0 {
		invAvgDL = 1.0 / avgDL
	}
	
	bm25ScoreBatchNEON(
		unsafe.Pointer(&tfs32[0]),       // #nosec G103
		unsafe.Pointer(&docLengths32[0]), // #nosec G103
		n,
		invAvgDL,
		idf,
		k1,
		b,
		unsafe.Pointer(&results[0]),    // #nosec G103
	)
	
	return results
}
