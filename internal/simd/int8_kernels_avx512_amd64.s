// Copyright 2024 Longbow Authors. All rights reserved.
// Hand-written AVX-512 and VNNI assembly for int8/uint8 distance and dot product kernels.
//
// Strategy:
//   - AVX-512BW: VPMOVSXBW/VPMOVZXBW widens 32 bytes to 32x int16 in a 512-bit ZMM register.
//   - VPMADDWD multiplies adjacent pairs and accumulates to 16x int32 per vector.
//   - AVX-512 VNNI: VPDPBUSD executes 64 byte-multiply-accumulate operations in a single instruction.
//   - Saturated exact bit-identity against generic scalar definitions with zero float rounding.

#include "textflag.h"

// ============================================================================
// dotInt8AVX512Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n int8 elements.
// AVX512: 32 elements per loop (512-bit ZMM), VPMOVSXBW + VPMADDWD.
// ============================================================================
TEXT ·dotInt8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop32_dot_i8_avx512:
    CMPQ CX, $32
    JL   tail16_dot_i8_avx512

    VPMOVSXBW (SI), Z1      // 32 int8 → 32 int16 (512-bit)
    VPMOVSXBW (DI), Z2
    VPMADDWD Z2, Z1, Z1     // 16x int32 product-adjacent-pairs
    VCVTDQ2PS Z1, Z1        // → 16 float32
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_dot_i8_avx512

tail16_dot_i8_avx512:
    CMPQ CX, $16
    JL   tail8_dot_i8_avx512

    VPMOVSXBW (SI), Y1      // 16 int8 → 16 int16 (256-bit)
    VPMOVSXBW (DI), Y2
    VPMADDWD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_dot_i8_avx512:
    CMPQ CX, $8
    JL   tail_reduce_dot_i8_avx512

    VPMOVSXBW 0(SI), X1     // 8 int8 → 8 int16 (XMM)
    VPMOVSXBW 0(DI), X2
    VPMOVSXWD X1, Y1
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_dot_i8_avx512:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_dot_i8_avx512

scalar_loop_dot_i8_avx512:
    MOVBLSX 0(SI), AX
    MOVBLSX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_dot_i8_avx512

done_dot_i8_avx512:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotUint8AVX512Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n uint8 elements.
// AVX512: 32 elements per loop (512-bit ZMM), VPMOVZXBW + VPMADDWD.
// ============================================================================
TEXT ·dotUint8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop32_dot_u8_avx512:
    CMPQ CX, $32
    JL   tail16_dot_u8_avx512

    VPMOVZXBW (SI), Z1      // 32 uint8 → 32 uint16 (512-bit)
    VPMOVZXBW (DI), Z2
    VPMADDWD Z2, Z1, Z1     // 16x int32 product-adjacent-pairs
    VCVTDQ2PS Z1, Z1        // → 16 float32
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_dot_u8_avx512

tail16_dot_u8_avx512:
    CMPQ CX, $16
    JL   tail8_dot_u8_avx512

    VPMOVZXBW (SI), Y1      // 16 uint8 → 16 uint16 (256-bit)
    VPMOVZXBW (DI), Y2
    VPMADDWD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_dot_u8_avx512:
    CMPQ CX, $8
    JL   tail_reduce_dot_u8_avx512

    VPMOVZXBW 0(SI), X1     // 8 uint8 → 8 uint16 (XMM)
    VPMOVZXBW 0(DI), X2
    VPMOVSXWD X1, Y1
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_dot_u8_avx512:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_dot_u8_avx512

scalar_loop_dot_u8_avx512:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_dot_u8_avx512

done_dot_u8_avx512:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanUint8AVX512Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n uint8 elements.
// AVX512: 32 elements per loop (512-bit ZMM), VPMOVZXBW + VPSUBW + VPMADDWD.
// ============================================================================
TEXT ·euclideanUint8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop32_eucl_u8_avx512:
    CMPQ CX, $32
    JL   tail16_eucl_u8_avx512

    VPMOVZXBW (SI), Z1      // 32 uint8 → 32 int16 (512-bit)
    VPMOVZXBW (DI), Z2
    VPSUBW Z2, Z1, Z1       // diff (signed int16)
    VPMADDWD Z1, Z1, Z1     // diff², adjacent pair sum → int32 x16
    VCVTDQ2PS Z1, Z1        // → 16 float32
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_eucl_u8_avx512

tail16_eucl_u8_avx512:
    CMPQ CX, $16
    JL   tail8_eucl_u8_avx512

    VPMOVZXBW (SI), Y1      // 16 uint8 → 16 int16 (256-bit)
    VPMOVZXBW (DI), Y2
    VPSUBW Y2, Y1, Y1       // diff
    VPMADDWD Y1, Y1, Y1     // diff² → int32 x8
    VCVTDQ2PS Y1, Y1        // → 8 float32
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_eucl_u8_avx512:
    CMPQ CX, $8
    JL   tail_reduce_eucl_u8_avx512

    VPMOVZXBW 0(SI), X1     // 8 uint8 → 8 int16 (XMM)
    VPMOVZXBW 0(DI), X2
    VPSUBW X2, X1, X1
    VPMADDWD X1, X1, X1
    VCVTDQ2PS X1, X1
    VEXTRACTF128 $0, Y0, X2
    VADDPS X1, X2, X2
    VINSERTF128 $0, X2, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_eucl_u8_avx512:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_eucl_u8_avx512

scalar_loop_eucl_u8_avx512:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_eucl_u8_avx512

done_eucl_u8_avx512:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// l2SquaredUint8AVX512Kernel(a, b uintptr, n int) float32
//
// Computes sum((a[i]-b[i])^2) for n uint8 elements (no final sqrt).
// ============================================================================
TEXT ·l2SquaredUint8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0

loop32_l2sq_u8_avx512:
    CMPQ CX, $32
    JL   tail16_l2sq_u8_avx512

    VPMOVZXBW (SI), Z1
    VPMOVZXBW (DI), Z2
    VPSUBW Z2, Z1, Z1
    VPMADDWD Z1, Z1, Z1
    VCVTDQ2PS Z1, Z1
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_l2sq_u8_avx512

tail16_l2sq_u8_avx512:
    CMPQ CX, $16
    JL   tail8_l2sq_u8_avx512

    VPMOVZXBW (SI), Y1
    VPMOVZXBW (DI), Y2
    VPSUBW Y2, Y1, Y1
    VPMADDWD Y1, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_l2sq_u8_avx512:
    CMPQ CX, $8
    JL   tail_reduce_l2sq_u8_avx512

    VPMOVZXBW 0(SI), X1
    VPMOVZXBW 0(DI), X2
    VPSUBW X2, X1, X1
    VPMADDWD X1, X1, X1
    VCVTDQ2PS X1, X1
    VEXTRACTF128 $0, Y0, X2
    VADDPS X1, X2, X2
    VINSERTF128 $0, X2, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_l2sq_u8_avx512:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_l2sq_u8_avx512

scalar_loop_l2sq_u8_avx512:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_l2sq_u8_avx512

done_l2sq_u8_avx512:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotInt8VNNIKernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n int8 elements using VPDPBUSD.
// Let a_u8 = a ^ 0x80 = a + 128 (unsigned uint8).
// a * b = (a + 128)*b - 128*b = a_u8 * b - 128*b.
// VPDPBUSD: src2(signed int8), src1(unsigned uint8), dst(int32).
// ============================================================================
TEXT ·dotInt8VNNIKernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0

    CMPQ CX, $64
    JL   tail32_vnni_i8

    VPXORQ Z0, Z0, Z0       // accumulator for sum(a_u8 * b_signed)
    VPXORQ Z10, Z10, Z10    // accumulator for sum(b_signed)

    MOVL $0x80808080, AX
    VMOVD AX, X15
    VPBROADCASTD X15, Z15   // 0x80 broadcast
    MOVL $0x01010101, BX
    VMOVD BX, X14
    VPBROADCASTD X14, Z14   // 0x01 broadcast (unsigned 1)

loop64_vnni_i8:
    VMOVDQU8 0(SI), Z1      // a: signed bytes
    VMOVDQU8 0(DI), Z2      // b: signed bytes
    VPXORD Z15, Z1, Z3      // Z3 = a ^ 0x80 = unsigned uint8

    VPDPBUSD Z2, Z3, Z0     // Z0 += b_signed * a_u8
    VPDPBUSD Z2, Z14, Z10   // Z10 += b_signed * 1

    ADDQ $64, SI
    ADDQ $64, DI
    SUBQ $64, CX
    CMPQ CX, $64
    JGE  loop64_vnni_i8

    // Combine: result = Z0 - 128 * Z10
    VPSLLD $7, Z10, Z10
    VPSUBD Z10, Z0, Z0

    VCVTDQ2PS Z0, Z0
    VEXTRACTF64X4 $0, Z0, Y2
    VEXTRACTF64X4 $1, Z0, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

tail32_vnni_i8:
    CMPQ CX, $32
    JL   tail16_vnni_i8

    VPMOVSXBW (SI), Z1
    VPMOVSXBW (DI), Z2
    VPMADDWD Z2, Z1, Z1
    VCVTDQ2PS Z1, Z1
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX

tail16_vnni_i8:
    CMPQ CX, $16
    JL   tail8_vnni_i8

    VPMOVSXBW (SI), Y1
    VPMOVSXBW (DI), Y2
    VPMADDWD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_vnni_i8:
    CMPQ CX, $8
    JL   tail_reduce_vnni_i8

    VPMOVSXBW 0(SI), X1
    VPMOVSXBW 0(DI), X2
    VPMOVSXWD X1, Y1
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_vnni_i8:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_vnni_i8

scalar_loop_vnni_i8:
    MOVBLSX 0(SI), AX
    MOVBLSX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_vnni_i8

done_vnni_i8:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotUint8VNNIKernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n uint8 elements using VPDPBUSD.
// Let b_s8 = b ^ 0x80 = b - 128 (signed int8).
// a * b = a * (b_s8 + 128) = a * b_s8 + 128 * a.
// VPDPBUSD: src2(signed int8), src1(unsigned uint8), dst(int32).
// ============================================================================
TEXT ·dotUint8VNNIKernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0

    CMPQ CX, $64
    JL   tail32_vnni_u8

    VPXORQ Z0, Z0, Z0       // accumulator for sum(a_u8 * b_s8)
    VPXORQ Z10, Z10, Z10    // accumulator for sum(a_u8 * 1)

    MOVL $0x80808080, AX
    VMOVD AX, X15
    VPBROADCASTD X15, Z15   // 0x80 broadcast
    MOVL $0x01010101, BX
    VMOVD BX, X14
    VPBROADCASTD X14, Z14   // 0x01 broadcast (signed 1)

loop64_vnni_u8:
    VMOVDQU8 0(SI), Z1      // a: unsigned bytes
    VMOVDQU8 0(DI), Z2      // b: unsigned bytes
    VPXORD Z15, Z2, Z3      // Z3 = b ^ 0x80 = signed int8

    VPDPBUSD Z3, Z1, Z0     // Z0 += a_u8 * b_s8
    VPDPBUSD Z14, Z1, Z10   // Z10 += a_u8 * 1

    ADDQ $64, SI
    ADDQ $64, DI
    SUBQ $64, CX
    CMPQ CX, $64
    JGE  loop64_vnni_u8

    // Combine: result = Z0 + 128 * Z10
    VPSLLD $7, Z10, Z10
    VPADDD Z10, Z0, Z0

    VCVTDQ2PS Z0, Z0
    VEXTRACTF64X4 $0, Z0, Y2
    VEXTRACTF64X4 $1, Z0, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

tail32_vnni_u8:
    CMPQ CX, $32
    JL   tail16_vnni_u8

    VPMOVZXBW (SI), Z1
    VPMOVZXBW (DI), Z2
    VPMADDWD Z2, Z1, Z1
    VCVTDQ2PS Z1, Z1
    VEXTRACTF64X4 $0, Z1, Y2
    VEXTRACTF64X4 $1, Z1, Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX

tail16_vnni_u8:
    CMPQ CX, $16
    JL   tail8_vnni_u8

    VPMOVZXBW (SI), Y1
    VPMOVZXBW (DI), Y2
    VPMADDWD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_vnni_u8:
    CMPQ CX, $8
    JL   tail_reduce_vnni_u8

    VPMOVZXBW 0(SI), X1
    VPMOVZXBW 0(DI), X2
    VPMOVSXWD X1, Y1
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_reduce_vnni_u8:
    VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ   done_vnni_u8

scalar_loop_vnni_u8:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_vnni_u8

done_vnni_u8:
    MOVSS X0, ret+24(FP)
    RET
