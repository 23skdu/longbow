//go:build amd64 && granite

#include "textflag.h"

// AMX tile config for dot product (palette 1, 64 bytes).
// TMM0: 1 row × 64 bytes (32×FP16 = 16 pairs)
// TMM1: 16 rows × 4 bytes (1 FP16 pair/row)
// TMM2: 1 row × 4 bytes (1×float32 accumulator)

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

// dotF16AMXKernel computes dot product of FP16 vectors.
// a, b are uint16 FP16 pointers, n is element count.
// Uses AMX TDPFP16PS.
TEXT ·dotF16AMXKernel(SB), NOSPLIT, $0-28
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
	JL     f16_tail_prefix

f16_loop:
	CMPQ   DX, $32
	JL     f16_drain

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

	BYTE $0xC4; BYTE $0xE2; BYTE $0x63; BYTE $0x5C; BYTE $0xD1

	ADDQ  $64, AX
	ADDQ  $64, CX
	SUBQ  $32, DX
	JMP   f16_loop

f16_drain:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4C; BYTE $0x19
	MOVSS tmp_buf(SB), X0
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0

	JMP   f16_tail

f16_tail_prefix:
	LEAQ  tmp_buf(SB), R9
	BYTE $0xC4; BYTE $0x82; BYTE $0x79; BYTE $0x4C; BYTE $0x19
	MOVSS tmp_buf(SB), X0
	BYTE $0xC4; BYTE $0xE2; BYTE $0x78; BYTE $0x49; BYTE $0xC0

f16_tail:
	CMPQ  DX, $0
	JE    f16_ret

f16_tail_loop:
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
	JNZ   f16_tail_loop

f16_ret:
	MOVSS X0, ret+24(FP)
	VZEROUPPER
	RET

// matMulAMXKernelFP16 computes matrix multiplication of FP16 matrices.
TEXT ·matMulAMXKernelFP16(SB), NOSPLIT, $0-48
	// Stub: return immediately. Full implementation requires TDPFP16PS.
	RET
