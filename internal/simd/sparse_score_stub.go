//go:build !arm64 && !amd64
 
package simd
 
func bm25ScoreBatchArch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	return bm25ScoreBatchGeneric(tfs, docLengths, avgDL, idf, k1, b)
}
