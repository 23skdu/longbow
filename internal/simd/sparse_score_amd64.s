//go:build amd64
 
#include "textflag.h"
 
// func bm25ScoreBatchAVX512(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
// Registers:
// DI: tfs
// SI: docLens
// DX: n
// X0: invAvgDL
// X1: idf
// X2: k1
// X3: b
// R8: results
 
TEXT ·bm25ScoreBatchAVX512(SB), NOSPLIT, $0-64
	MOVQ tfs+0(FP), DI
	MOVQ docLens+8(FP), SI
	MOVQ n+16(FP), DX
	MOVSS invAvgDL+24(FP), X0
	MOVSS idf+28(FP), X1
	MOVSS k1+32(FP), X2
	MOVSS b+36(FP), X3
	MOVQ results+40(FP), R8
 
	// Broadcast constants to ZMM
	VBROADCASTSS X0, Z4  // Z4 = [invAvgDL, ...]
	VBROADCASTSS X1, Z5  // Z5 = [idf, ...]
	VBROADCASTSS X2, Z6  // Z6 = [k1, ...]
	VBROADCASTSS X3, Z7  // Z7 = [b, ...]
 
	// Constant: 1.0
	MOVQ $0x3F800000, AX
	MOVQ AX, X8
	VBROADCASTSS X8, Z8  // Z8 = [1.0, ...]
 
	// k1_plus_1 = k1 + 1.0
	VADDPS Z8, Z6, Z9    // Z9 = [k1+1, ...]
 
	// one_minus_b = 1.0 - b
	VSUBPS Z7, Z8, Z10   // Z10 = [1.0-b, ...]
 
loop:
	CMPQ DX, $16
	JL tail
 
	// Load 16 TFs (int64) and convert to float32
	// Wait, tfs are []int. On amd64, int is int64.
	// So we need to load 16 int64.
	// ZMM can hold 8 int64 or 16 int32.
	// If []int is []int64, we need 2 ZMMs for 16 TFs.
	
	// Let's assume n is the number of elements.
	// Load 8 TFs (int64)
	VMOVDQU64 (DI), Z11
	VMOVDQU64 64(DI), Z12
	
	// Convert to float32 (CVTDQ2PS converts int32, we need CVTQQ2PS for int64)
	VCVTQQ2PS Z11, Y11
	VCVTQQ2PS Z12, Y12
	VINSERTF32X8 $1, Y12, Z11, Z11 // Z11 now has 16 float32 TFs
 
	// Load 16 docLens (int64)
	VMOVDQU64 (SI), Z13
	VMOVDQU64 64(SI), Z14
	VCVTQQ2PS Z13, Y13
	VCVTQQ2PS Z14, Y14
	VINSERTF32X8 $1, Y14, Z13, Z13 // Z13 now has 16 float32 docLens
 
	// score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen/avgDL))
	// L factor = (1 - b + b * docLen * invAvgDL)
	VMULPS Z4, Z13, Z14   // Z14 = docLen * invAvgDL
	VMULPS Z7, Z14, Z14   // Z14 = b * docLen * invAvgDL
	VADDPS Z10, Z14, Z14  // Z14 = (1-b) + b * docLen * invAvgDL
 
	// denominator = tf + k1 * factor
	VMULPS Z6, Z14, Z14   // Z14 = k1 * factor
	VADDPS Z11, Z14, Z14  // Z14 = tf + k1 * factor
 
	// numerator = tf * (k1 + 1)
	VMULPS Z9, Z11, Z15   // Z15 = tf * (k1 + 1)
 
	// score = IDF * (numerator / denominator)
	VDIVPS Z14, Z15, Z15  // Z15 = numerator / denominator
	VMULPS Z5, Z15, Z15   // Z15 = IDF * (numerator / denominator)
 
	// Store 16 results
	VMOVUPS Z15, (R8)
 
	ADDQ $128, DI
	ADDQ $128, SI
	ADDQ $64, R8
	SUBQ $16, DX
	JMP loop
 
tail:
	// Simple scalar tail for remaining elements
	TESTQ DX, DX
	JZ done
 
	// Load TF
	MOVQ (DI), AX
	CVTSI2SS AX, X11  // X11 = tf
 
	// Load docLen
	MOVQ (SI), AX
	CVTSI2SS AX, X13  // X13 = docLen
 
	// score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen * invAvgDL))
	MOVSS X13, X14
	MULSS X0, X14     // X14 = docLen * invAvgDL
	MULSS X3, X14     // X14 = b * docLen * invAvgDL
	
	// 1-b
	MOVSS X8, X10
	SUBSS X3, X10     // X10 = 1-b
	ADDSS X10, X14    // X14 = (1-b) + b * docLen * invAvgDL
 
	MULSS X2, X14     // X14 = k1 * factor
	ADDSS X11, X14    // X14 = tf + k1 * factor
 
	// k1 + 1
	MOVSS X2, X9
	ADDSS X8, X9      // X9 = k1 + 1
	MULSS X9, X11     // X11 = tf * (k1+1)
 
	DIVSS X14, X11    // X11 = numerator / denominator
	MULSS X1, X11     // X11 = IDF * (numerator / denominator)
 
	MOVSS X11, (R8)
 
	ADDQ $8, DI
	ADDQ $8, SI
	ADDQ $4, R8
	DECQ DX
	JMP tail
 
done:
	RET
