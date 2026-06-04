#include "textflag.h"

// func gemm4x8KernelPacked(a, b, c uintptr, k int, ldc int)
// Requires: AVX, AVX2, FMA3
//
// Computes C[0:4][0:8] += A[0:4][0:k] * B_packed[0:k][0:8]
// (accumulates into existing C values — load C first, FMA, store back)
//
// a:        base address of A (4 rows × k cols, stride = k elements)
// b_packed: packed B (k × 8, row-major, stride = 8 elements = 32 bytes always)
// c:        base address of C (4 rows × ldc cols, stride = ldc elements)
// k:        inner (contracted) dimension
// ldc:      leading dimension of C (in float32 elements)
//
// Register map:
//   Y0-Y3: C row accumulators
//   Y4-Y7: B row values (loaded via VMOVUPS, 8 floats each)
//   Y8-Y11: A row broadcasts
//   AX:    A row 0 pointer
//   R8:    A row 1 pointer
//   R9:    A row 2 pointer
//   R10:   A row 3 pointer
//   CX:    B pointer (advances by 32 bytes per k)
//   DX:    C row 0 pointer
//   R11:   C row 1 pointer
//   R12:   C row 2 pointer
//   R13:   C row 3 pointer
//   BX:    k counter
//   SI:    ldc * 4 (byte stride between C rows)
//   DI:    k / 4 or scratch
TEXT ·gemm4x8KernelPacked(SB), NOSPLIT, $0-48
	MOVQ a+0(FP), AX
	MOVQ b+8(FP), CX
	MOVQ c+16(FP), DX
	MOVQ k+24(FP), BX
	MOVQ lda+32(FP), DI
	MOVQ ldc+40(FP), SI

	// SI = ldc * 4 (byte stride between C rows)
	SHLQ $2, SI

	// DI = lda * 4 (byte stride between A rows)
	SHLQ $2, DI

	// A row pointers
	LEAQ (AX)(DI*1), R8       // R8 = A row 1
	LEAQ (R8)(DI*1), R9       // R9 = A row 2
	LEAQ (R9)(DI*1), R10      // R10 = A row 3

	// C row pointers
	LEAQ (DX)(SI*1), R11
	LEAQ (R11)(SI*1), R12
	LEAQ (R12)(SI*1), R13

	// Load C values into accumulators (accumulate mode)
	VMOVUPS (DX), Y0          // Y0 = C row 0
	VMOVUPS (R11), Y1         // Y1 = C row 1
	VMOVUPS (R12), Y2         // Y2 = C row 2
	VMOVUPS (R13), Y3         // Y3 = C row 3

	TESTQ BX, BX
	JZ    store

	// Process k in chunks of 4
	MOVQ BX, DI
	SHRQ $2, DI               // DI = k / 4
	ANDQ $3, BX               // BX = k % 4

	TESTQ DI, DI
	JZ    k_rem

k_unrolled4:
	// ---- k iteration 0 ----
	VMOVUPS (CX), Y4          // B[l][0:8]
	VMOVSS  (AX), X8
	VBROADCASTSS X8, Y8       // A[0][l]
	VMOVSS  (R8), X9
	VBROADCASTSS X9, Y9       // A[1][l]
	VMOVSS  (R9), X10
	VBROADCASTSS X10, Y10     // A[2][l]
	VMOVSS  (R10), X11
	VBROADCASTSS X11, Y11     // A[3][l]
	VFMADD231PS Y8, Y4, Y0    // C[0] += A[0][l] * B[l][:]
	VFMADD231PS Y9, Y4, Y1
	VFMADD231PS Y10, Y4, Y2
	VFMADD231PS Y11, Y4, Y3
	ADDQ $4, AX
	ADDQ $4, R8
	ADDQ $4, R9
	ADDQ $4, R10
	ADDQ $32, CX              // B advances by 32 bytes (8 floats)

	// ---- k iteration 1 ----
	VMOVUPS (CX), Y5
	VMOVSS  (AX), X8
	VBROADCASTSS X8, Y8
	VMOVSS  (R8), X9
	VBROADCASTSS X9, Y9
	VMOVSS  (R9), X10
	VBROADCASTSS X10, Y10
	VMOVSS  (R10), X11
	VBROADCASTSS X11, Y11
	VFMADD231PS Y8, Y5, Y0
	VFMADD231PS Y9, Y5, Y1
	VFMADD231PS Y10, Y5, Y2
	VFMADD231PS Y11, Y5, Y3
	ADDQ $4, AX
	ADDQ $4, R8
	ADDQ $4, R9
	ADDQ $4, R10
	ADDQ $32, CX

	// ---- k iteration 2 ----
	VMOVUPS (CX), Y6
	VMOVSS  (AX), X8
	VBROADCASTSS X8, Y8
	VMOVSS  (R8), X9
	VBROADCASTSS X9, Y9
	VMOVSS  (R9), X10
	VBROADCASTSS X10, Y10
	VMOVSS  (R10), X11
	VBROADCASTSS X11, Y11
	VFMADD231PS Y8, Y6, Y0
	VFMADD231PS Y9, Y6, Y1
	VFMADD231PS Y10, Y6, Y2
	VFMADD231PS Y11, Y6, Y3
	ADDQ $4, AX
	ADDQ $4, R8
	ADDQ $4, R9
	ADDQ $4, R10
	ADDQ $32, CX

	// ---- k iteration 3 ----
	VMOVUPS (CX), Y7
	VMOVSS  (AX), X8
	VBROADCASTSS X8, Y8
	VMOVSS  (R8), X9
	VBROADCASTSS X9, Y9
	VMOVSS  (R9), X10
	VBROADCASTSS X10, Y10
	VMOVSS  (R10), X11
	VBROADCASTSS X11, Y11
	VFMADD231PS Y8, Y7, Y0
	VFMADD231PS Y9, Y7, Y1
	VFMADD231PS Y10, Y7, Y2
	VFMADD231PS Y11, Y7, Y3
	ADDQ $4, AX
	ADDQ $4, R8
	ADDQ $4, R9
	ADDQ $4, R10
	ADDQ $32, CX

	DECQ DI
	JNZ  k_unrolled4

k_rem:
	TESTQ BX, BX
	JZ    store

k_rem_loop:
	VMOVUPS (CX), Y4
	VMOVSS  (AX), X8
	VBROADCASTSS X8, Y8
	VMOVSS  (R8), X9
	VBROADCASTSS X9, Y9
	VMOVSS  (R9), X10
	VBROADCASTSS X10, Y10
	VMOVSS  (R10), X11
	VBROADCASTSS X11, Y11
	VFMADD231PS Y8, Y4, Y0
	VFMADD231PS Y9, Y4, Y1
	VFMADD231PS Y10, Y4, Y2
	VFMADD231PS Y11, Y4, Y3
	ADDQ $4, AX
	ADDQ $4, R8
	ADDQ $4, R9
	ADDQ $4, R10
	ADDQ $32, CX
	DECQ BX
	JNZ  k_rem_loop

store:
	VMOVUPS Y0, (DX)
	VMOVUPS Y1, (R11)
	VMOVUPS Y2, (R12)
	VMOVUPS Y3, (R13)

	VZEROUPPER
	RET
