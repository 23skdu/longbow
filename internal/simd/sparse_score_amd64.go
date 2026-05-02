//go:build amd64
 
package simd
 
import "unsafe"
 
// bm25ScoreBatchAVX512 is implemented in sparse_score_amd64.s
func bm25ScoreBatchAVX512(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
 
func bm25ScoreBatchArch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	if len(tfs) == 0 {
		return nil
	}
 
	results := make([]float32, len(tfs))
	if hasAVX512 {
		invAvgDL := 1.0 / avgDL
		if avgDL == 0 {
			invAvgDL = 1.0
		}
		bm25ScoreBatchAVX512(
			unsafe.Pointer(&tfs[0]),
			unsafe.Pointer(&docLengths[0]),
			len(tfs),
			float32(invAvgDL),
			idf,
			k1,
			b,
			unsafe.Pointer(&results[0]),
		)
		return results
	}
	return bm25ScoreBatchGeneric(tfs, docLengths, avgDL, idf, k1, b)
}
