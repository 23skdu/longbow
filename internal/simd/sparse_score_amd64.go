//go:build amd64

package simd

import "unsafe"

// bm25ScoreBatchAVX512 is implemented in sparse_score_amd64.s
func bm25ScoreBatchAVX512(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)

// bm25ScoreBatchAVX2 is implemented in sparse_score_amd64.s
func bm25ScoreBatchAVX2(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)

func bm25ScoreBatchArch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	if len(tfs) == 0 {
		return nil
	}

	results := make([]float32, len(tfs))
	if features.HasAVX512 {
		invAvgDL := float32(1.0)
		if avgDL != 0 {
			invAvgDL = 1.0 / avgDL
		}
		bm25ScoreBatchAVX512(
			unsafe.Pointer(&tfs[0]),
			unsafe.Pointer(&docLengths[0]),
			len(tfs),
			invAvgDL,
			idf,
			k1,
			b,
			unsafe.Pointer(&results[0]),
		)
		return results
	}
	if features.HasAVX2 {
		invAvgDL := float32(1.0)
		if avgDL != 0 {
			invAvgDL = 1.0 / avgDL
		}
		bm25ScoreBatchAVX2(
			unsafe.Pointer(&tfs[0]),
			unsafe.Pointer(&docLengths[0]),
			len(tfs),
			invAvgDL,
			idf,
			k1,
			b,
			unsafe.Pointer(&results[0]),
		)
		return results
	}
	return bm25ScoreBatchGeneric(tfs, docLengths, avgDL, idf, k1, b)
}
