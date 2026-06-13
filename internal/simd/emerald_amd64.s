//go:build amd64 && emerald

#include "textflag.h"

// AMX tile config for dot product (palette 1, 64 bytes).
// TMM0: 1 row × 64 bytes (32×BF16 = 16 pairs)
// TMM1: 16 rows × 4 bytes (1 BF16 pair/row)
// TMM2: 1 row × 4 bytes (1×float32 accumulator)
//
// TDPBF16PS(TMM2,TMM0,TMM1): C[0][0] += Σ_k (A[0][2k]*B[k][0] + A[0][2k+1]*B[k][1])

DATA dot_tile_config+0(SB)/1,  $1
DATA dot_tile_config+1(SB)/1,  $0
DATA dot_tile_config+2(SB)/8, $0
DATA dot_tile_config+10(SB)/2, $0
DATA dot_tile_config+12(SB)/4, $0
DATA dot_tile_config+16(SB)/2, $64
DATA dot_tile_config+18(SB)/2, $4
DATA dot_tile_config+20(SB)/2, $4
DATA dot_tile_config+22(SB)/8, $0
DATA dot_tile_config+30(SB)/2, $0
DATA dot_tile_config+32(SB)/1, $1
DATA dot_tile_config+33(SB)/1, $16
DATA dot_tile_config+34(SB)/1, $1
DATA dot_tile_config+35(SB)/8, $0
DATA dot_tile_config+43(SB)/4, $0
DATA dot_tile_config+47(SB)/1, $0
DATA dot_tile_config+48(SB)/8, $0
DATA dot_tile_config+56(SB)/8, $0
GLOBL dot_tile_config(SB), RODATA, $64

GLOBL tmp_buf(SB), NOPTR, $64

// enableAMX requests AMX permission from kernel for the current thread.
// ARCH_REQ_XCOMP_PERM(18) = request XFEATURE_XTILECFG (bit 18).
TEXT ·enableAMX(SB), NOSPLIT, $0
	MOVQ $158, AX
	MOVQ $0x1023, DI
	MOVQ $18, SI
	SYSCALL
	RET

// releaseTiles issues TILERELEASE.
TEXT ·releaseTiles(SB), NOSPLIT, $0
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0
	RET

// dotAMXKernelBF16 computes dot product of BF16 vectors.
// a, b are uint16 BF16 pointers, n is element count.
// Processes 32 BF16 values per TDPBF16PS iteration.
TEXT ·dotAMXKernelBF16(SB), NOSPLIT, $0-28
	MOVQ   $158, AX
	MOVQ   $0x1023, DI
	MOVQ   $18, SI
	SYSCALL

	MOVQ   a+0(FP), AX
	MOVQ   b+8(FP), CX
	MOVQ   n+16(FP), DX

	LEAQ   dot_tile_config(SB), R8
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x49; BYTE $0x10

	CMPQ   DX, $32
	JL     bf16_tail_prefix

bf16_loop:
	CMPQ   DX, $32
	JL     bf16_drain

	VMOVDQU (AX), Y0
	VMOVDQA Y0, tmp_buf(SB)
	VMOVDQU 32(AX), Y1
	VMOVDQA Y1, tmp_buf+32(SB)
	LEAQ   tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4B; BYTE $0x09

	VMOVDQU (CX), Y2
	VMOVDQA Y2, tmp_buf(SB)
	VMOVDQU 32(CX), Y3
	VMOVDQA Y3, tmp_buf+32(SB)
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4B; BYTE $0x19

	BYTE $0xC4; BYTE $0xE2; BYTE $0x62; BYTE $0x5C; BYTE $0xD1

	ADDQ  $64, AX
	ADDQ  $64, CX
	SUBQ  $32, DX
	JMP   bf16_loop

bf16_drain:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4C; BYTE $0x19
	MOVSS tmp_buf(SB), X0
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0

	JMP   bf16_tail

bf16_tail_prefix:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79;BYTE $0x4C; BYTE $0x19
	MOVSS tmp_buf(SB), X0
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0

bf16_tail:
	CMPQ  DX, $0
	JE    bf16_ret

bf16_tail_loop:
	MOVWLZX (AX), R10
	MOVWLZX (CX), R11
	SHLQ    $16, R10
	SHLQ    $16, R11
	VMOVQ   R10, X1
	VMOVQ   R11, X2
	VFMADD231SS X1, X2, X0
	ADDQ  $2, AX
	ADDQ  $2, CX
	DECQ  DX
	JNZ   bf16_tail_loop

bf16_ret:
	MOVSS X0, ret+24(FP)
	VZEROUPPER
	RET

// dotAMXKernelINT8 computes dot product of INT8 vectors.
// a, b are int8 pointers, n is element count.
// Processes 64 INT8 values per TDPBSSD iteration.
TEXT ·dotAMXKernelINT8(SB), NOSPLIT, $0-28
	MOVQ   $158, AX
	MOVQ   $0x1023, DI
	MOVQ   $18, SI
	SYSCALL

	MOVQ   a+0(FP), AX
	MOVQ   b+8(FP), CX
	MOVQ   n+16(FP), DX
	XORL   R12, R12 // Tail accumulator

	LEAQ   dot_tile_config(SB), R8
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x49; BYTE $0x10 // ldtilecfg (%r8)

	CMPQ   DX, $64
	JL     int8_tail_prefix

int8_loop:
	CMPQ   DX, $64
	JL     int8_drain

	VMOVDQU (AX), Y0
	VMOVDQA Y0, tmp_buf(SB)
	VMOVDQU 32(AX), Y1
	VMOVDQA Y1, tmp_buf+32(SB)
	LEAQ   tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4B; BYTE $0x09 // tileloaddt1 (%r9), %tmm1

	VMOVDQU (CX), Y2
	VMOVDQA Y2, tmp_buf(SB)
	VMOVDQU 32(CX), Y3
	VMOVDQA Y3, tmp_buf+32(SB)
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4B; BYTE $0x19 // tileloaddt1 (%r9), %tmm3

	BYTE $0xC4; BYTE $0xE2; BYTE $0x63; BYTE $0x5E; BYTE $0xD1 // tdpbssd %tmm3, %tmm1, %tmm2

	ADDQ  $64, AX
	ADDQ  $64, CX
	SUBQ  $64, DX
	JMP   int8_loop

int8_drain:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4C; BYTE $0x19 // tilestored %tmm2, (%r9)
	MOVL tmp_buf(SB), R10
	ADDL R10, R12
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0 // tilerelease

	JMP   int8_tail

int8_tail_prefix:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4C; BYTE $0x19 // tilestored %tmm2, (%r9)
	MOVL tmp_buf(SB), R10
	ADDL R10, R12
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0 // tilerelease

int8_tail:
	CMPQ  DX, $0
	JE    int8_ret

int8_tail_loop:
	MOVBQSX (AX), R10
	MOVBQSX (CX), R11
	IMULL R11, R10
	ADDL  R10, R12
	INCQ  AX
	INCQ  CX
	DECQ  DX
	JNZ   int8_tail_loop

int8_ret:
	MOVL R12, ret+24(FP)
	VZEROUPPER
	RET

// matMulAMXKernelBF16 computes matrix multiplication of BF16 matrices.
TEXT ·matMulAMXKernelBF16(SB), NOSPLIT, $0-48
	// Stub: return immediately. Full implementation requires TDPBF16PS.
	RET
