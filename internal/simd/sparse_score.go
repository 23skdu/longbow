package simd

// BM25ScoreBatch computes the BM25 score for a batch of documents.
// It delegates to architecture-specific SIMD implementations when available.
func BM25ScoreBatch(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	if len(tfs) == 0 {
		return nil
	}
	return bm25ScoreBatchArch(tfs, docLengths, avgDL, idf, k1, b)
}

func bm25ScoreBatchGeneric(tfs []int, docLengths []int, avgDL float32, idf float32, k1 float32, b float32) []float32 {
	n := len(tfs)
	scores := make([]float32, n)
	
	if avgDL == 0 {
		avgDL = 1.0
	}
	invAvgDL := float32(1.0) / avgDL
	k1PlusOne := k1 + 1.0
	
	for i := 0; i < n; i++ {
		tf := float32(tfs[i])
		docLen := float32(docLengths[i])
		
		lengthNorm := float32(1.0) - b + b*(docLen*invAvgDL)
		if lengthNorm <= 0 {
			lengthNorm = 0.0001
		}
		
		numerator := tf * k1PlusOne
		denominator := tf + k1*lengthNorm
		
		scores[i] = idf * (numerator / denominator)
	}
	
	return scores
}
