// Code generated manually. AVX2-accelerated sqrt for float32.

#include "textflag.h"

// func sqrtFloat32AVX2Kernel(src, dst unsafe.Pointer, n int)
// Uses VSQRTPS to compute 4 float32 square roots per instruction.
// Requires: SSE2 (VSQRTPS)
TEXT ·sqrtFloat32AVX2Kernel(SB), NOSPLIT, $0-24
	MOVQ src+0(FP), SI     // source pointer
	MOVQ dst+8(FP), DI     // destination pointer
	MOVQ n+16(FP), CX      // length

	TESTQ CX, CX
	JLE   done

	// Process 4 elements at a time with VSQRTPS (XMM, 128-bit)
	MOVQ  CX, AX
	SHRQ  $2, AX           // AX = n / 4
	SHLQ  $4, AX           // AX = (n / 4) * 16 (bytes per 4 floats)
	ADDQ  SI, AX           // AX = end of 4-aligned source

loop4:
	CMPQ SI, AX
	JAE  remainder

	MOVUPS (SI), X0        // load 4 floats
	SQRTPS X0, X0          // sqrt of 4 floats
	MOVUPS X0, (DI)        // store 4 results

	ADDQ $16, SI           // advance 4 floats
	ADDQ $16, DI
	JMP  loop4

remainder:
	// Handle remaining elements one at a time
	MOVQ n+16(FP), CX
	ANDQ $3, CX            // CX = n % 4
	JZ   done

	// Move DI to the position for remaining elements
	// (DI was already advanced past 4-aligned chunks)

loop1:
	MOVSS (SI), X0         // load 1 float
	SQRTSS X0, X0          // scalar sqrt
	MOVSS X0, (DI)         // store 1 result

	ADDQ $4, SI
	ADDQ $4, DI
	DECQ CX
	JNZ  loop1

done:
	RET
