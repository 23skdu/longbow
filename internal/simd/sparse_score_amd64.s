//go:build amd64

#include "textflag.h"

// func bm25ScoreBatchAVX512(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
// Fallback: Pure Go implementation handles all cases. This file exists for future AVX2 optimization.

TEXT ·bm25ScoreBatchAVX512(SB), NOSPLIT, $0-64
	// No-op: use pure Go fallback in sparse_score_amd64.go
	RET
