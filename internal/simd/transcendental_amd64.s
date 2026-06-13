#include "textflag.h"

// func expAVX512Kernel(src uintptr, dst uintptr, n int)
// Requires: AVX, AVX512F
TEXT ·expAVX512Kernel(SB), NOSPLIT, $0-24
	MOVQ src+0(FP), AX
	MOVQ dst+8(FP), CX
	MOVQ n+16(FP), DX

	CMPQ DX, $0
	JE   exp_done

exp_loop:
	CMPQ DX, $16
	JL   exp_tail

	VMOVUPS (AX), Z0
	
	// z = x * log2(e)
	VMOVUPS log2e_const_tr<>+0(SB), Z1
	VMULPS  Z1, Z0, Z1
	
	// n = floor(z + 0.5)
	VADDPS      half_const_tr<>+0(SB), Z1, Z2
	VRNDSCALEPS $0x01, Z2, Z2
	
	// f = z - n
	VSUBPS Z2, Z1, Z1 // Z1 = f
	
	// Poly: 2^f approx c0 + f*(c1 + f*(c2 + f*(c3 + f*(c4 + f*c5))))
	VMOVUPS     exp_c0_const_tr<>+0(SB), Z3
	VFMADD213PS exp_c1_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c2_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c3_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c4_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c5_const_tr<>+0(SB), Z1, Z3
	
	// 2^n
	VCVTPS2DQ Z2, Z1
	VPADDD    exp_bias_const_tr<>+0(SB), Z1, Z1
	VPSLLD    $23, Z1, Z1
	
	// res = 2^f * 2^n
	VMULPS Z1, Z3, Z0
	
	VMOVUPS Z0, (CX)
	
	ADDQ $64, AX
	ADDQ $64, CX
	SUBQ $16, DX
	JMP  exp_loop

exp_tail:
	// Masked tail for remaining <16 elements
	CMPQ DX, $0
	JE   exp_done
	
	MOVQ $0xFFFF, R8
	MOVQ CX, R10       // save dst pointer
	MOVQ DX, CX        // shift count (must be in CX for SHLQ)
	SHLQ CX, R8
	MOVQ R10, CX       // restore dst pointer
	NOTQ R8
	KMOVQ R8, K1
	
	VMOVUPS.Z (AX), K1, Z0
	
	// Same logic with K1
	VMOVUPS log2e_const_tr<>+0(SB), Z1
	VMULPS  Z1, Z0, Z1
	VADDPS  half_const_tr<>+0(SB), Z1, Z2
	VRNDSCALEPS $0x01, Z2, Z2
	VSUBPS Z2, Z1, Z1
	VMOVUPS     exp_c0_const_tr<>+0(SB), Z3
	VFMADD213PS exp_c1_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c2_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c3_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c4_const_tr<>+0(SB), Z1, Z3
	VFMADD213PS exp_c5_const_tr<>+0(SB), Z1, Z3
	VCVTPS2DQ Z2, Z1
	VPADDD    exp_bias_const_tr<>+0(SB), Z1, Z1
	VPSLLD    $23, Z1, Z1
	VMULPS Z1, Z3, Z0
	
	VMOVUPS Z0, K1, (CX)

exp_done:
	VZEROUPPER
	RET

// func logAVX512Kernel(src uintptr, dst uintptr, n int)
// Requires: AVX, AVX512F
TEXT ·logAVX512Kernel(SB), NOSPLIT, $0-24
	MOVQ src+0(FP), AX
	MOVQ dst+8(FP), CX
	MOVQ n+16(FP), DX

	CMPQ DX, $0
	JE   log_done

log_loop:
	CMPQ DX, $16
	JL   log_tail

	VMOVUPS (AX), Z0
	
	// Extract exponent
	VPSRLD $23, Z0, Z1
	VPANDD exp_mask_tr<>+0(SB), Z1, Z1 // Z1 = raw_exp
	VPSUBD exp_bias_const_tr<>+0(SB), Z1, Z1 // Z1 = n
	VCVTDQ2PS Z1, Z1 // Z1 = n as float
	
	// Extract mantissa
	VPANDD mant_mask_tr<>+0(SB), Z0, Z0
	VPORD  one_float_bits_tr<>+0(SB), Z0, Z0 // Z0 = f in [1, 2)
	
	// log(f) approx (f-1) * (a1 + (f-1)*(a2 + ...))
	VSUBPS one_const_tr<>+0(SB), Z0, Z2 // Z2 = m = f - 1
	
	VMOVUPS     log_a6_const_tr<>+0(SB), Z3
	VFMADD213PS log_a5_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a4_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a3_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a2_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a1_const_tr<>+0(SB), Z2, Z3
	VMULPS      Z2, Z3, Z3 // Z3 = log(f)
	
	// log(x) = n*ln(2) + log(f)
	VMOVUPS ln2_const_tr<>+0(SB), Z4
	VFMADD213PS Z3, Z4, Z1
	
	VMOVUPS Z1, (CX)
	
	ADDQ $64, AX
	ADDQ $64, CX
	SUBQ $16, DX
	JMP  log_loop

log_tail:
	CMPQ DX, $0
	JE   log_done
	
	MOVQ $0xFFFF, R8
	MOVQ CX, R10       // save dst pointer
	MOVQ DX, CX        // shift count (must be in CX for SHLQ)
	SHLQ CX, R8
	MOVQ R10, CX       // restore dst pointer
	NOTQ R8
	KMOVQ R8, K1
	
	VMOVUPS.Z (AX), K1, Z0
	
	VPSRLD $23, Z0, Z1
	VPANDD exp_mask_tr<>+0(SB), Z1, Z1
	VPSUBD exp_bias_const_tr<>+0(SB), Z1, Z1
	VCVTDQ2PS Z1, Z1
	
	VPANDD mant_mask_tr<>+0(SB), Z0, Z0
	VPORD  one_float_bits_tr<>+0(SB), Z0, Z0
	
	VSUBPS one_const_tr<>+0(SB), Z0, Z2
	VMOVUPS     log_a6_const_tr<>+0(SB), Z3
	VFMADD213PS log_a5_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a4_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a3_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a2_const_tr<>+0(SB), Z2, Z3
	VFMADD213PS log_a1_const_tr<>+0(SB), Z2, Z3
	VMULPS      Z2, Z3, Z3
	VMOVUPS ln2_const_tr<>+0(SB), Z4
	VFMADD213PS Z3, Z4, Z1
	
	VMOVUPS Z1, K1, (CX)

log_done:
	VZEROUPPER
	RET

// func expAVX2Kernel(src uintptr, dst uintptr, n int)
// Requires: AVX, AVX2
TEXT ·expAVX2Kernel(SB), NOSPLIT, $0-24
	MOVQ src+0(FP), AX
	MOVQ dst+8(FP), CX
	MOVQ n+16(FP), DX

	CMPQ DX, $0
	JE   exp2_done

exp2_loop:
	CMPQ DX, $8
	JL   exp2_tail

	VMOVUPS (AX), Y0
	
	// z = x * log2(e)
	VBROADCASTSS log2e_const_tr<>+0(SB), Y1
	VMULPS       Y1, Y0, Y1
	
	// n = floor(z + 0.5)
	VBROADCASTSS half_const_tr<>+0(SB), Y2
	VADDPS       Y2, Y1, Y2
	// Rounding on AVX2 is via VROUNDPS
	VROUNDPS $0x01, Y2, Y2
	
	// f = z - n
	VSUBPS Y2, Y1, Y1 // Y1 = f
	
	// Poly: 2^f approx c0 + f*(c1 + f*(c2 + f*(c3 + f*(c4 + f*c5))))
	VBROADCASTSS exp_c0_const_tr<>+0(SB), Y3
	// FMA on AVX2
	VMOVUPS      exp_c5_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VMOVUPS      exp_c4_const_tr<>+0(SB), X5
	VBROADCASTSS X5, Y5
	VFMADD213PS  Y5, Y1, Y4
	VMOVUPS      exp_c3_const_tr<>+0(SB), X5
	VBROADCASTSS X5, Y5
	VFMADD213PS  Y5, Y1, Y4
	VMOVUPS      exp_c2_const_tr<>+0(SB), X5
	VBROADCASTSS X5, Y5
	VFMADD213PS  Y5, Y1, Y4
	VMOVUPS      exp_c1_const_tr<>+0(SB), X5
	VBROADCASTSS X5, Y5
	VFMADD213PS  Y5, Y1, Y4
	VFMADD213PS  Y3, Y1, Y4    // Y4 = 2^f
	
	// 2^n
	VCVTPS2DQ Y2, Y1
	VPADDD    exp_bias_const_tr<>+0(SB), Y1, Y1
	VPSLLD    $23, Y1, Y1
	
	// res = 2^f * 2^n
	VMULPS Y1, Y4, Y0
	
	VMOVUPS Y0, (CX)
	
	ADDQ $32, AX
	ADDQ $32, CX
	SUBQ $8, DX
	JMP  exp2_loop

exp2_tail:
	CMPQ DX, $0
	JE   exp2_done
	MOVSS (AX), X0
	// ... (scalar exp1)
	ADDQ $4, AX
	ADDQ $4, CX
	SUBQ $1, DX
	JMP  exp2_tail

exp2_done:
	VZEROUPPER
	RET

// func logAVX2Kernel(src uintptr, dst uintptr, n int)
// Requires: AVX, AVX2
TEXT ·logAVX2Kernel(SB), NOSPLIT, $0-24
	MOVQ src+0(FP), AX
	MOVQ dst+8(FP), CX
	MOVQ n+16(FP), DX

	CMPQ DX, $0
	JE   log2_done

log2_loop:
	CMPQ DX, $8
	JL   log2_tail

	VMOVUPS (AX), Y0
	
	// Extract exponent
	VPSRLD $23, Y0, Y1
	VPAND  exp_mask_tr<>+0(SB), Y1, Y1 // Y1 = raw_exp
	VPSUBD exp_bias_const_tr<>+0(SB), Y1, Y1 // Y1 = n
	VCVTDQ2PS Y1, Y1 // Y1 = n as float
	
	// Extract mantissa
	VPAND mant_mask_tr<>+0(SB), Y0, Y0
	VPOR  one_float_bits_tr<>+0(SB), Y0, Y0 // Y0 = f in [1, 2)
	
	// log(f) approx (f-1) * (a1 + (f-1)*(a2 + ...))
	VBROADCASTSS one_const_tr<>+0(SB), Y2
	VSUBPS Y2, Y0, Y2 // Y2 = m = f - 1
	
	VBROADCASTSS log_a6_const_tr<>+0(SB), Y3
	VMOVUPS      log_a5_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VFMADD213PS  Y4, Y2, Y3
	VMOVUPS      log_a4_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VFMADD213PS  Y4, Y2, Y3
	VMOVUPS      log_a3_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VFMADD213PS  Y4, Y2, Y3
	VMOVUPS      log_a2_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VFMADD213PS  Y4, Y2, Y3
	VMOVUPS      log_a1_const_tr<>+0(SB), X4
	VBROADCASTSS X4, Y4
	VFMADD213PS  Y4, Y2, Y3
	VMULPS       Y2, Y3, Y3 // Y3 = log(f)
	
	// log(x) = n*ln(2) + log(f)
	VBROADCASTSS ln2_const_tr<>+0(SB), Y4
	VFMADD213PS  Y3, Y4, Y1
	
	VMOVUPS Y1, (CX)
	
	ADDQ $32, AX
	ADDQ $32, CX
	SUBQ $8, DX
	JMP  log2_loop

log2_tail:
	CMPQ DX, $0
	JE   log2_done
	ADDQ $4, AX
	ADDQ $4, CX
	SUBQ $1, DX
	JMP  log2_tail

log2_done:
	VZEROUPPER
	RET

DATA log2e_const_tr<>+0(SB)/4, $1.44269504
GLOBL log2e_const_tr<>(SB), RODATA|NOPTR, $4

DATA ln2_const_tr<>+0(SB)/4, $0.69314718
GLOBL ln2_const_tr<>(SB), RODATA|NOPTR, $4

DATA half_const_tr<>+0(SB)/4, $0.5
GLOBL half_const_tr<>(SB), RODATA|NOPTR, $4

DATA one_const_tr<>+0(SB)/4, $1.0
GLOBL one_const_tr<>(SB), RODATA|NOPTR, $4

DATA exp_bias_const_tr<>+0(SB)/4, $127
DATA exp_bias_const_tr<>+4(SB)/4, $127
DATA exp_bias_const_tr<>+8(SB)/4, $127
DATA exp_bias_const_tr<>+12(SB)/4, $127
DATA exp_bias_const_tr<>+16(SB)/4, $127
DATA exp_bias_const_tr<>+20(SB)/4, $127
DATA exp_bias_const_tr<>+24(SB)/4, $127
DATA exp_bias_const_tr<>+28(SB)/4, $127
DATA exp_bias_const_tr<>+32(SB)/4, $127
DATA exp_bias_const_tr<>+36(SB)/4, $127
DATA exp_bias_const_tr<>+40(SB)/4, $127
DATA exp_bias_const_tr<>+44(SB)/4, $127
DATA exp_bias_const_tr<>+48(SB)/4, $127
DATA exp_bias_const_tr<>+52(SB)/4, $127
DATA exp_bias_const_tr<>+56(SB)/4, $127
DATA exp_bias_const_tr<>+60(SB)/4, $127
GLOBL exp_bias_const_tr<>(SB), RODATA|NOPTR, $64

DATA exp_c0_const_tr<>+0(SB)/4, $1.0
GLOBL exp_c0_const_tr<>(SB), RODATA|NOPTR, $4
DATA exp_c1_const_tr<>+0(SB)/4, $0.69314718
GLOBL exp_c1_const_tr<>(SB), RODATA|NOPTR, $4
DATA exp_c2_const_tr<>+0(SB)/4, $0.240226507
GLOBL exp_c2_const_tr<>(SB), RODATA|NOPTR, $4
DATA exp_c3_const_tr<>+0(SB)/4, $0.0555041086
GLOBL exp_c3_const_tr<>(SB), RODATA|NOPTR, $4
DATA exp_c4_const_tr<>+0(SB)/4, $0.009618129
GLOBL exp_c4_const_tr<>(SB), RODATA|NOPTR, $4
DATA exp_c5_const_tr<>+0(SB)/4, $0.00134204
GLOBL exp_c5_const_tr<>(SB), RODATA|NOPTR, $4

DATA exp_mask_tr<>+0(SB)/4, $0x000000FF
DATA exp_mask_tr<>+4(SB)/4, $0x000000FF
DATA exp_mask_tr<>+8(SB)/4, $0x000000FF
DATA exp_mask_tr<>+12(SB)/4, $0x000000FF
DATA exp_mask_tr<>+16(SB)/4, $0x000000FF
DATA exp_mask_tr<>+20(SB)/4, $0x000000FF
DATA exp_mask_tr<>+24(SB)/4, $0x000000FF
DATA exp_mask_tr<>+28(SB)/4, $0x000000FF
DATA exp_mask_tr<>+32(SB)/4, $0x000000FF
DATA exp_mask_tr<>+36(SB)/4, $0x000000FF
DATA exp_mask_tr<>+40(SB)/4, $0x000000FF
DATA exp_mask_tr<>+44(SB)/4, $0x000000FF
DATA exp_mask_tr<>+48(SB)/4, $0x000000FF
DATA exp_mask_tr<>+52(SB)/4, $0x000000FF
DATA exp_mask_tr<>+56(SB)/4, $0x000000FF
DATA exp_mask_tr<>+60(SB)/4, $0x000000FF
GLOBL exp_mask_tr<>(SB), RODATA|NOPTR, $64

DATA mant_mask_tr<>+0(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+4(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+8(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+12(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+16(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+20(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+24(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+28(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+32(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+36(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+40(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+44(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+48(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+52(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+56(SB)/4, $0x007FFFFF
DATA mant_mask_tr<>+60(SB)/4, $0x007FFFFF
GLOBL mant_mask_tr<>(SB), RODATA|NOPTR, $64

DATA one_float_bits_tr<>+0(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+4(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+8(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+12(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+16(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+20(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+24(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+28(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+32(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+36(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+40(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+44(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+48(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+52(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+56(SB)/4, $0x3F800000
DATA one_float_bits_tr<>+60(SB)/4, $0x3F800000
GLOBL one_float_bits_tr<>(SB), RODATA|NOPTR, $64

DATA log_a1_const_tr<>+0(SB)/4, $0.99999642
GLOBL log_a1_const_tr<>(SB), RODATA|NOPTR, $4
DATA log_a2_const_tr<>+0(SB)/4, $-0.49987412
GLOBL log_a2_const_tr<>(SB), RODATA|NOPTR, $4
DATA log_a3_const_tr<>+0(SB)/4, $0.33179904
GLOBL log_a3_const_tr<>(SB), RODATA|NOPTR, $4
DATA log_a4_const_tr<>+0(SB)/4, $-0.2407338
GLOBL log_a4_const_tr<>(SB), RODATA|NOPTR, $4
DATA log_a5_const_tr<>+0(SB)/4, $0.16765407
GLOBL log_a5_const_tr<>(SB), RODATA|NOPTR, $4
DATA log_a6_const_tr<>+0(SB)/4, $-0.09532939
GLOBL log_a6_const_tr<>(SB), RODATA|NOPTR, $4
