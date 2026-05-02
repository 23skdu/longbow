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

	// Broadcast constants to YMM (AVX2)
	VBROADCASTSS X0, Y4  // Y4 = [invAvgDL, ...]
	VBROADCASTSS X1, Y5  // Y5 = [idf, ...]
	VBROADCASTSS X2, Y6  // Y6 = [k1, ...]
	VBROADCASTSS X3, Y7  // Y7 = [b, ...]

	// Constant: 1.0
	MOVQ $0x3F800000, AX
	MOVQ AX, X8
	VBROADCASTSS X8, Y8  // Y8 = [1.0, ...]

	// k1_plus_1 = k1 + 1.0
	VADDPS Y8, Y6, Y9    // Y9 = [k1+1, ...]

	// one_minus_b = 1.0 - b
	VSUBPS Y7, Y8, Y10   // Y10 = [1.0-b, ...]

loop:
	CMPQ DX, $8
	JL tail

	// Load 8 TFs (int64)
	VMOVDQU (DI), Y11
	VMOVDQU 32(DI), Y12

	// Convert to float32
	VCVTQQ2PS Y11, Y11
	VCVTQQ2PS Y12, Y12

	// Load 8 docLens (int64)
	VMOVDQU (SI), Y13
	VMOVDQU 32(SI), Y14
	VCVTQQ2PS Y13, Y13
	VCVTQQ2PS Y14, Y14

	// Compute factor = (1-b) + b * docLen * invAvgDL
	VMULPS Y4, Y13, Y14   // Y14 = docLen * invAvgDL
	VMULPS Y7, Y14, Y14   // Y14 = b * docLen * invAvgDL
	VADDPS Y10, Y14, Y14  // Y14 = (1-b) + b * docLen * invAvgDL

	// denominator = tf + k1 * factor
	VMULPS Y6, Y14, Y14   // Y14 = k1 * factor
	VADDPS Y11, Y14, Y14  // Y14 = tf + k1 * factor

	// numerator = tf * (k1 + 1)
	VMULPS Y9, Y11, Y15   // Y15 = tf * (k1 + 1)

	// score = IDF * (numerator / denominator)
	VDIVPS Y14, Y15, Y15  // Y15 = numerator / denominator
	VMULPS Y5, Y15, Y15   // Y15 = IDF * (numerator / denominator)

	// Store 8 results
	VMOVUPS Y15, (R8)

	ADDQ $64, DI
	ADDQ $64, SI
	ADDQ $32, R8
	SUBQ $8, DX
	JMP loop

tail:
	// Simple scalar tail for remaining elements
	TESTQ DX, DX
	JZ done

	// Load TF using MOVD (avoids CVTSI2SS issue)
	MOVQ (DI), AX
	CVTSS2SS X0, X0        // dummy to set round mode
	MOVD AX, X11           // X11 = tf (as int in lower bits)

	// Load docLen
	MOVQ (SI), AX
	MOVD AX, X13           // X13 = docLen (as int in lower bits)

	// Use scalar SSE instructions that work on all platforms
	CVTSS2SD X11, X11      // convert to double
	CVTSS2SD X13, X13      // convert to double
	CVTSD2SS X11, X11      // convert back to float (truncates)

	// Compute
	MOVSS X13, X14
	MULSS X0, X14
	MULSS X3, X14

	MOVSS X8, X10
	SUBSS X3, X10
	ADDSS X10, X14

	MULSS X2, X14
	ADDSS X11, X14

	MOVSS X2, X9
	ADDSS X8, X9
	MULSS X9, X11

	DIVSS X14, X11
	MULSS X1, X11

	MOVSS X11, (R8)

	ADDQ $8, DI
	ADDQ $8, SI
	ADDQ $4, R8
	DECQ DX
	JMP tail

done:
	RET
