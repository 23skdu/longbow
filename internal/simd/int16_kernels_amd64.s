// Copyright 2024 Longbow Authors. All rights reserved.
// Hand-written AVX2 assembly for int16/uint16 distance kernels.
// These replace the generic Go fallbacks wired in simd_amd64.go.
//
// Strategy:
//   - VPMOVSXWD/VPMOVZXWD sign/zero-extends 8x int16 → 8x int32 per 128-bit lane.
//   - Two such loads per loop iteration → 16 elements consumed per cycle.
//   - Integer arithmetic in int32, then VCVTDQ2PS → float32 for accumulation.
//   - Final horizontal reduction via VEXTRACTF128 + VADDPS/VHADDPS.
//
// Kernel signatures (matching all_kernels_stubs_amd64.go):
//   func euclideanInt16AVX2Kernel(a, b uintptr, n int) float32
//   func euclideanUint16AVX2Kernel(a, b uintptr, n int) float32
//   func dotInt16AVX2Kernel(a, b uintptr, n int) float32
//   func dotUint16AVX2Kernel(a, b uintptr, n int) float32

#include "textflag.h"

// reduceYMM: horizontal sum of 8 float32 lanes in Y0 → XMM result in X0
// Clobbers: X1, X2
#define REDUCE_YMM(ysrc, xdst, xtmp1, xtmp2) \
    VEXTRACTF128 $1, ysrc, xtmp1;            \
    VEXTRACTF128 $0, ysrc, xdst;             \
    VADDPS xtmp1, xdst, xdst;               \
    VMOVHLPS xdst, xdst, xtmp1;             \
    VADDPS xtmp1, xdst, xdst;               \
    VMOVSHDUP xdst, xtmp2;                  \
    VADDSS xtmp2, xdst, xdst

// ============================================================================
// euclideanInt16AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) over n int16 elements.
// Processes 16 elements per main loop iteration (2x 8-wide VPMOVSXWD).
// ============================================================================
TEXT ·euclideanInt16AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0   // accumulator (float32 x8)

loop16:
    CMPQ CX, $16
    JL   tail8

    // Load 16 int16 from a and b (32 bytes each)
    // Use two VPMOVSXWD to sign-extend 8 int16 → 8 int32 each
    VPMOVSXWD 0(SI), Y1      // a[0..7]  → int32 x8
    VPMOVSXWD 16(SI), Y2     // a[8..15] → int32 x8
    VPMOVSXWD 0(DI), Y3      // b[0..7]  → int32 x8
    VPMOVSXWD 16(DI), Y4     // b[8..15] → int32 x8

    VPSUBD Y3, Y1, Y1        // diff0 = a[0..7] - b[0..7]
    VPSUBD Y4, Y2, Y2        // diff1 = a[8..15] - b[8..15]

    VPMULLD Y1, Y1, Y1       // diff0^2 (int32)
    VPMULLD Y2, Y2, Y2       // diff1^2 (int32)

    VCVTDQ2PS Y1, Y1         // → float32 x8
    VCVTDQ2PS Y2, Y2         // → float32 x8

    VADDPS Y1, Y0, Y0        // accumulate
    VADDPS Y2, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $16, CX
    JMP  loop16

tail8:
    CMPQ CX, $8
    JL   tail_scalar

    VPMOVSXWD 0(SI), Y1
    VPMOVSXWD 0(DI), Y3
    VPSUBD Y3, Y1, Y1
    VPMULLD Y1, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $8, CX

tail_scalar:
    // Horizontal reduction of Y0 → X0
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    // Scalar tail
    TESTQ CX, CX
    JZ    done_eucl_int16

scalar_loop_eucl_int16:
    MOVWLSX 0(SI), AX        // sign-extend int16 → int32
    MOVWLSX 0(DI), BX
    SUBL BX, AX              // diff
    IMULL AX, AX             // diff^2
    CVTSL2SS AX, X1          // int32 → float32
    VADDSS X1, X0, X0

    ADDQ $2, SI
    ADDQ $2, DI
    DECQ CX
    JNZ  scalar_loop_eucl_int16

done_eucl_int16:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanUint16AVX2Kernel(a, b uintptr, n int) float32
//
// Same as above but uses VPMOVZXWD (zero-extend) for uint16.
// ============================================================================
TEXT ·euclideanUint16AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0

loop16_u16:
    CMPQ CX, $16
    JL   tail8_u16

    VPMOVZXWD 0(SI), Y1      // zero-extend uint16 → uint32 (treated as int32 for diff)
    VPMOVZXWD 16(SI), Y2
    VPMOVZXWD 0(DI), Y3
    VPMOVZXWD 16(DI), Y4

    VPSUBD Y3, Y1, Y1
    VPSUBD Y4, Y2, Y2

    VPMULLD Y1, Y1, Y1
    VPMULLD Y2, Y2, Y2

    VCVTDQ2PS Y1, Y1
    VCVTDQ2PS Y2, Y2

    VADDPS Y1, Y0, Y0
    VADDPS Y2, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $16, CX
    JMP  loop16_u16

tail8_u16:
    CMPQ CX, $8
    JL   tail_scalar_u16

    VPMOVZXWD 0(SI), Y1
    VPMOVZXWD 0(DI), Y3
    VPSUBD Y3, Y1, Y1
    VPMULLD Y1, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $8, CX

tail_scalar_u16:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_uint16

scalar_loop_eucl_uint16:
    MOVWLZX 0(SI), AX        // zero-extend uint16 → uint32
    MOVWLZX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    ADDQ $2, SI
    ADDQ $2, DI
    DECQ CX
    JNZ  scalar_loop_eucl_uint16

done_eucl_uint16:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotInt16AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n int16 elements.
// Uses VPMADDWD which multiplies 16x int16 pairs and horizontally adds to 8x int32.
// This is the canonical AVX2 int16 dot product instruction.
// ============================================================================
TEXT ·dotInt16AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VPXOR Y0, Y0, Y0         // int32 x8 accumulator

loop16_dot_i16:
    CMPQ CX, $16
    JL   tail8_dot_i16

    VMOVDQU 0(SI), Y1        // 16x int16 from a
    VMOVDQU 0(DI), Y2        // 16x int16 from b
    VPMADDWD Y2, Y1, Y1      // 16x (a*b) reduced to 8x int32 (pairs added)
    VPADDD Y1, Y0, Y0        // accumulate

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $16, CX
    JMP  loop16_dot_i16

tail8_dot_i16:
    CMPQ CX, $8
    JL   tail_scalar_dot_i16

    VMOVDQU 0(SI), X1        // 8x int16 (128-bit)
    VMOVDQU 0(DI), X2
    // Extend to 256-bit for VPMADDWD
    VPMOVSXWD X1, Y1
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1       // element-wise multiply (already 32-bit)
    VPADDD Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $8, CX

tail_scalar_dot_i16:
    // Convert int32 accumulator to float32 and reduce
    VCVTDQ2PS Y0, Y0
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_int16

scalar_loop_dot_i16:
    MOVWLSX 0(SI), AX
    MOVWLSX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    ADDQ $2, SI
    ADDQ $2, DI
    DECQ CX
    JNZ  scalar_loop_dot_i16

done_dot_int16:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotUint16AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n uint16 elements.
// Zero-extends to uint32 and uses VPMULLD.
// ============================================================================
TEXT ·dotUint16AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0        // float32 accumulator

loop16_dot_u16:
    CMPQ CX, $16
    JL   tail8_dot_u16

    VPMOVZXWD 0(SI), Y1      // zero-extend 8x uint16 → 8x uint32
    VPMOVZXWD 16(SI), Y2     // next 8
    VPMOVZXWD 0(DI), Y3
    VPMOVZXWD 16(DI), Y4

    VPMULLD Y3, Y1, Y1       // 8x uint32 products
    VPMULLD Y4, Y2, Y2

    VCVTDQ2PS Y1, Y1         // → float32 (products <= 65535^2 = 4.3e9, fits float32)
    VCVTDQ2PS Y2, Y2

    VADDPS Y1, Y0, Y0
    VADDPS Y2, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $16, CX
    JMP  loop16_dot_u16

tail8_dot_u16:
    CMPQ CX, $8
    JL   tail_scalar_dot_u16

    VPMOVZXWD 0(SI), Y1
    VPMOVZXWD 0(DI), Y3
    VPMULLD Y3, Y1, Y1
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $8, CX

tail_scalar_dot_u16:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_uint16

scalar_loop_dot_u16:
    MOVWLZX 0(SI), AX
    MOVWLZX 0(DI), BX
    MULL BX                  // AX = AX * BX (unsigned 32-bit)
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    ADDQ $2, SI
    ADDQ $2, DI
    DECQ CX
    JNZ  scalar_loop_dot_u16

done_dot_uint16:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotInt8AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n int8 elements.
// Uses VPMOVSXBW + VPMADDWD for 16 elements per main loop.
// ============================================================================
TEXT ·dotInt8AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop16_dot_i8:
    CMPQ CX, $16
    JL   tail8_dot_i8

    VPMOVSXBW 0(SI), Y1     // 16 int8 → 16 int16
    VPMOVSXBW 0(DI), Y2
    VPMADDWD Y2, Y1, Y1     // 16x int16 multiply-adjacent-sum → 8x int32
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX
    JMP  loop16_dot_i8

tail8_dot_i8:
    CMPQ CX, $8
    JL   tail_scalar_dot_i8

    VPMOVSXBW 0(SI), X1     // 8 int8 → 8 int16 (XMM)
    VPMOVSXBW 0(DI), X2
    VPMOVSXWD X1, Y1        // 8 int16 → 8 int32
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1      // element-wise 8x int32 product
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_dot_i8:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_int8

scalar_loop_dot_i8:
    MOVBLSX 0(SI), AX
    MOVBLSX 0(DI), BX
    IMULL BX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_dot_i8

done_dot_int8:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanUint8AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n uint8 elements.
// Uses VPMOVZXBW + VPSUBW + VPMADDWD (self-multiply for diff²).
// ============================================================================
TEXT ·euclideanUint8AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop16_eucl_u8:
    CMPQ CX, $16
    JL   tail8_eucl_u8

    VPMOVZXBW 0(SI), Y1     // 16 uint8 → 16 int16
    VPMOVZXBW 0(DI), Y2
    VPSUBW Y2, Y1, Y1       // diff (signed int16)
    VPMADDWD Y1, Y1, Y1     // diff², adjacent pair sum → int32 x8
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX
    JMP  loop16_eucl_u8

tail8_eucl_u8:
    CMPQ CX, $8
    JL   tail_scalar_eucl_u8

    VPMOVZXBW 0(SI), X1     // 8 uint8 → 8 int16 (XMM: reads 8 bytes)
    VPMOVZXBW 0(DI), X2
    VPSUBW X2, X1, X1        // diff (signed int16 x8)
    VPMADDWD X1, X1, X1      // diff², adjacent pair sum → int32 x4 (XMM)
    VCVTDQ2PS X1, X1         // → 4 float32
    VEXTRACTF128 $0, Y0, X2  // save current low accumulator
    VADDPS X1, X2, X2        // add tail result to low accumulator
    VINSERTF128 $0, X2, Y0, Y0 // merge updated low back

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_eucl_u8:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_uint8

scalar_loop_eucl_u8:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_eucl_u8

done_eucl_uint8:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotUint8AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n uint8 elements.
// Uses VPMOVZXBW + VPMADDWD for 16 elements per main loop.
// ============================================================================
TEXT ·dotUint8AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop16_dot_u8:
    CMPQ CX, $16
    JL   tail8_dot_u8

    VPMOVZXBW 0(SI), Y1     // 16 uint8 → 16 int16
    VPMOVZXBW 0(DI), Y2
    VPMADDWD Y2, Y1, Y1     // 16x int16 multiply-adjacent-sum → 8x int32
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX
    JMP  loop16_dot_u8

tail8_dot_u8:
    CMPQ CX, $8
    JL   tail_scalar_dot_u8

    VPMOVZXBW 0(SI), X1     // 8 uint8 → 8 int16 (XMM: reads 8 bytes)
    VPMOVZXBW 0(DI), X2
    VPMOVSXWD X1, Y1        // 8 int16 → 8 int32
    VPMOVSXWD X2, Y2
    VPMULLD Y2, Y1, Y1      // element-wise 8x int32 product
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_dot_u8:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_uint8

scalar_loop_dot_u8:
    MOVBLZX 0(SI), AX
    MOVBLZX 0(DI), BX
    MULL BX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_dot_u8

done_dot_uint8:
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanInt8AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n int8 elements.
// Uses VPMOVSXBW (sign-extend) + VPSUBW + VPMADDWD (self-multiply for diff²).
// ============================================================================
TEXT ·euclideanInt8AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8

loop16_eucl_i8:
    CMPQ CX, $16
    JL   tail8_eucl_i8

    VPMOVSXBW 0(SI), Y1     // 16 int8 → 16 int16
    VPMOVSXBW 0(DI), Y2
    VPSUBW Y2, Y1, Y1       // diff (signed int16)
    VPMADDWD Y1, Y1, Y1     // diff², adjacent pair sum → int32 x8
    VCVTDQ2PS Y1, Y1
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX
    JMP  loop16_eucl_i8

tail8_eucl_i8:
    CMPQ CX, $8
    JL   tail_scalar_eucl_i8

    VPMOVSXBW 0(SI), X1     // 8 int8 → 8 int16 (XMM: reads 8 bytes)
    VPMOVSXBW 0(DI), X2
    VPSUBW X2, X1, X1        // diff (signed int16 x8)
    VPMADDWD X1, X1, X1      // diff², adjacent pair sum → int32 x4 (XMM)
    VCVTDQ2PS X1, X1         // → 4 float32
    VEXTRACTF128 $0, Y0, X2  // save current low accumulator
    VADDPS X1, X2, X2        // add tail result to low accumulator
    VINSERTF128 $0, X2, Y0, Y0 // merge updated low back

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_eucl_i8:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_int8

scalar_loop_eucl_i8:
    MOVBLSX 0(SI), AX        // sign-extend byte → int32
    MOVBLSX 0(DI), BX
    SUBL BX, AX              // diff
    IMULL AX, AX             // diff²
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_eucl_i8

done_eucl_int8:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanInt8AVX512Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n int8 elements.
// AVX512: 32 elements per loop (512-bit ZMM), VPMOVSXBW + VPSUBW + VPMADDWD.
// ============================================================================
TEXT ·euclideanInt8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8 (YMM — reuse AVX2 reduction)

loop32_eucl_i8_avx512:
    CMPQ CX, $32
    JL   tail16_eucl_i8_avx512

    VPMOVSXBW (SI), Z1      // 32 int8 → 32 int16 (512-bit)
    VPMOVSXBW (DI), Z2
    VPSUBW Z2, Z1, Z1       // diff (signed int16)
    VPMADDWD Z1, Z1, Z1     // diff², adjacent pair sum → int32 x16
    VCVTDQ2PS Z1, Z1        // → 16 float32
    VEXTRACTF64X4 $0, Z1, Y2 // low 8 floats → Y2
    VEXTRACTF64X4 $1, Z1, Y3 // high 8 floats → Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_eucl_i8_avx512

tail16_eucl_i8_avx512:
    CMPQ CX, $16
    JL   tail8_eucl_i8_avx512

    VPMOVSXBW (SI), Y1      // 16 int8 → 16 int16 (256-bit)
    VPMOVSXBW (DI), Y2
    VPSUBW Y2, Y1, Y1       // diff (signed int16)
    VPMADDWD Y1, Y1, Y1     // diff² → int32 x8
    VCVTDQ2PS Y1, Y1        // → 8 float32
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_eucl_i8_avx512:
    CMPQ CX, $8
    JL   tail_scalar_eucl_i8_avx512

    VPMOVSXBW 0(SI), X1     // 8 int8 → 8 int16 (XMM)
    VPMOVSXBW 0(DI), X2
    VPSUBW X2, X1, X1        // diff
    VPMADDWD X1, X1, X1      // diff² → int32 x4
    VCVTDQ2PS X1, X1         // → 4 float32
    VEXTRACTF128 $0, Y0, X2
    VADDPS X1, X2, X2
    VINSERTF128 $0, X2, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_eucl_i8_avx512:
    REDUCE_YMM(Y0, X0, X1, X2)
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_int8_avx512

scalar_loop_eucl_i8_avx512:
    MOVBLSX 0(SI), AX
    MOVBLSX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_eucl_i8_avx512

done_eucl_int8_avx512:
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotInt32AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n int32 elements.
// Uses XMM (128-bit): 4 elements per iteration, VPMULDQ for int64 accumulation.
// ============================================================================
TEXT ·dotInt32AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VPXOR X0, X0, X0

loop4_dot_i32:
    CMPQ CX, $4
    JL   reduce_dot_i32

    VMOVDQU (SI), X1
    VMOVDQU (DI), X2

    VPMULDQ X2, X1, X3
    VPADDQ  X3, X0, X0

    VPALIGNR $4, X1, X1, X4
    VPALIGNR $4, X2, X2, X5
    VPMULDQ X5, X4, X6
    VPADDQ  X6, X0, X0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $4, CX
    JMP  loop4_dot_i32

reduce_dot_i32:
    VMOVQ  X0, R8
    VPSHUFD $0xEE, X0, X1
    VMOVQ  X1, AX
    ADDQ   AX, R8
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_i32

scalar_dot_i32:
    MOVL   (SI), AX
    MOVL   (DI), BX
    SHLQ   $32, AX
    SARQ   $32, AX
    SHLQ   $32, BX
    SARQ   $32, BX
    IMULQ  BX, AX
    ADDQ   AX, R8

    ADDQ $4, SI
    ADDQ $4, DI
    DECQ CX
    JNZ  scalar_dot_i32

done_dot_i32:
    CVTSQ2SS R8, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanInt32AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n int32 elements.
// XMM approach: 4 elements per iteration.
// ============================================================================
TEXT ·euclideanInt32AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VPXOR X0, X0, X0

loop4_eucl_i32:
    CMPQ CX, $4
    JL   reduce_eucl_i32

    VMOVDQU (SI), X1
    VMOVDQU (DI), X2
    VPSUBD X2, X1, X1

    VPMULDQ X1, X1, X3
    VPADDQ  X3, X0, X0

    VPALIGNR $4, X1, X1, X4
    VPMULDQ X4, X4, X6
    VPADDQ  X6, X0, X0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $4, CX
    JMP  loop4_eucl_i32

reduce_eucl_i32:
    VMOVQ  X0, R8
    VPSHUFD $0xEE, X0, X1
    VMOVQ  X1, AX
    ADDQ   AX, R8
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_i32

scalar_eucl_i32:
    MOVL   (SI), AX
    MOVL   (DI), BX
    SUBL   BX, AX
    SHLQ   $32, AX
    SARQ   $32, AX
    IMULQ  AX, AX
    ADDQ   AX, R8

    ADDQ $4, SI
    ADDQ $4, DI
    DECQ CX
    JNZ  scalar_eucl_i32

done_eucl_i32:
    CVTSQ2SS R8, X0
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// dotUint32AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sum(a[i]*b[i]) for n uint32 elements.
// XMM: 4 elements per iteration, VPMULUDQ for unsigned int64 accumulation.
// ============================================================================
TEXT ·dotUint32AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VPXOR X0, X0, X0

loop4_dot_u32:
    CMPQ CX, $4
    JL   reduce_dot_u32

    VMOVDQU (SI), X1
    VMOVDQU (DI), X2

    VPMULUDQ X2, X1, X3
    VPADDQ   X3, X0, X0

    VPALIGNR $4, X1, X1, X4
    VPALIGNR $4, X2, X2, X5
    VPMULUDQ X5, X4, X6
    VPADDQ   X6, X0, X0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $4, CX
    JMP  loop4_dot_u32

reduce_dot_u32:
    VMOVQ  X0, R8
    VPSHUFD $0xEE, X0, X1
    VMOVQ  X1, AX
    ADDQ   AX, R8
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_dot_u32

scalar_dot_u32:
    MOVL   (SI), AX
    MOVL   (DI), BX
    MULQ   BX
    ADDQ   AX, R8

    ADDQ $4, SI
    ADDQ $4, DI
    DECQ CX
    JNZ  scalar_dot_u32

done_dot_u32:
    CVTSQ2SS R8, X0
    MOVSS X0, ret+24(FP)
    RET

// ============================================================================
// euclideanUint32AVX2Kernel(a, b uintptr, n int) float32
//
// Computes sqrt(sum((a[i]-b[i])^2)) for n uint32 elements.
// XMM: 4 elements per iteration.
// Uses VPMAXUD + VPMINUD for unsigned diff, then VPMULDQ for squaring.
// ============================================================================
TEXT ·euclideanUint32AVX2Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VPXOR X0, X0, X0

loop4_eucl_u32:
    CMPQ CX, $4
    JL   reduce_eucl_u32

    VMOVDQU (SI), X1
    VMOVDQU (DI), X2

    VPMAXUD X2, X1, X3
    VPMINUD X2, X1, X1
    VPSUBD  X1, X3, X1

    VPMULDQ X1, X1, X3
    VPADDQ  X3, X0, X0

    VPALIGNR $4, X1, X1, X4
    VPMULDQ X4, X4, X6
    VPADDQ  X6, X0, X0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $4, CX
    JMP  loop4_eucl_u32

reduce_eucl_u32:
    VMOVQ  X0, R8
    VPSHUFD $0xEE, X0, X1
    VMOVQ  X1, AX
    ADDQ   AX, R8
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_eucl_u32

scalar_eucl_u32:
    MOVL   (SI), AX
    MOVL   (DI), BX
    SUBL   BX, AX
    JAE    abs_eucl_u32
    NEGL   AX
abs_eucl_u32:
    MULQ   AX
    ADDQ   AX, R8

    ADDQ $4, SI
    ADDQ $4, DI
    DECQ CX
    JNZ  scalar_eucl_u32

done_eucl_u32:
    CVTSQ2SS R8, X0
    VSQRTSS X0, X0, X0
    MOVSS X0, ret+24(FP)
    RET
