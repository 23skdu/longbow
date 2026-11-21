//go:build arm64

#include "textflag.h"
#include "simd_arm64_macros.h"

TEXT ·euclideanInt16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16   // acc0_2D
    VEOR    V1.B16, V1.B16, V1.B16   // acc1_2D

    CMP     $8, R1
    BLT     ei16_tail

ei16_loop:
    VLD1.P  16(R0), [V2.H8]           // 8 int16 from a
    VLD1.P  16(R2), [V3.H8]           // 8 int16 from b

    // Lower 4: sign-extend int16 → int32
    VMOV    V2.D[1], V10.D[0]
    VMOV    V3.D[1], V11.D[0]
    VSSHLL_4S(2, 4)                   // a lower 4 → 4S
    VSSHLL_4S(3, 6)                   // b lower 4 → 4S
    VSSHLL_4S(10, 5)                  // a upper 4 → 4S
    VSSHLL_4S(11, 7)                  // b upper 4 → 4S

    VSUB_4S(6, 4, 4)                  // diff_l = a_l - b_l
    VSUB_4S(7, 5, 5)                  // diff_u = a_u - b_u

    VMUL_4S(4, 4, 4)                  // sq_l = diff_l * diff_l
    VMUL_4S(5, 5, 5)                  // sq_u = diff_u * diff_u

    VSADDLP_2D(4, 8)                  // acc partial lower → 2D
    VSADDLP_2D(5, 9)                  // acc partial upper → 2D

    VADD_2D(8, 0, 0)                  // accumulate
    VADD_2D(9, 1, 1)

    SUB     $8, R1
    CMP     $8, R1
    BGE     ei16_loop

ei16_tail:
    CMP     $4, R1
    BLT     ei16_scalar

    VLD1.P  8(R0), [V2.H4]            // 4 int16 from a
    VLD1.P  8(R2), [V3.H4]            // 4 int16 from b

    VSSHLL_4S(2, 4)
    VSSHLL_4S(3, 6)
    VSUB_4S(6, 4, 4)
    VMUL_4S(4, 4, 4)
    VSADDLP_2D(4, 8)
    VADD_2D(8, 0, 0)

    SUB     $4, R1

ei16_scalar:
    CBZ     R1, ei16_reduce

    MOVD    $0, R3                     // scalar accumulator (int64)

ei16_scalar_loop:
    MOVH.P  2(R0), R4
    MOVH.P  2(R2), R5
    SXTH    R4, R4
    SXTH    R5, R5
    SUB     R5, R4, R6
    MUL     R6, R6, R6
    ADD     R6, R3
    SUB     $1, R1
    CBNZ    R1, ei16_scalar_loop

    VEOR    V10.B16, V10.B16, V10.B16
    VMOV    R3, V10.D[0]
    VADD_2D(10, 0, 0)

ei16_reduce:
    VADD_2D(0, 1, 2)       // sum both accumulators

    VMOV    V2.D[0], R3
    VMOV    V2.D[1], R4
    ADD     R4, R3, R3

    SCVTFD  R3, F0
    FSQRTD  F0, F0
    FCVTDS  F0, F0

    FMOVS   F0, ret+48(FP)
    RET

// func dotInt16NEONKernel(a, b []int16) float32
TEXT ·dotInt16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16   // acc0_2D
    VEOR    V1.B16, V1.B16, V1.B16   // acc1_2D

    CMP     $8, R1
    BLT     di16_tail

di16_loop:
    VLD1.P  16(R0), [V2.H8]
    VLD1.P  16(R2), [V3.H8]

    VMOV    V2.D[1], V10.D[0]
    VMOV    V3.D[1], V11.D[0]
    VSSHLL_4S(2, 4)
    VSSHLL_4S(3, 6)
    VSSHLL_4S(10, 5)
    VSSHLL_4S(11, 7)

    VMUL_4S(6, 4, 4)                  // prod_l = a_l * b_l
    VMUL_4S(7, 5, 5)                  // prod_u = a_u * b_u

    VSADDLP_2D(4, 8)
    VSADDLP_2D(5, 9)

    VADD_2D(8, 0, 0)
    VADD_2D(9, 1, 1)

    SUB     $8, R1
    CMP     $8, R1
    BGE     di16_loop

di16_tail:
    CMP     $4, R1
    BLT     di16_scalar

    VLD1.P  8(R0), [V2.H4]
    VLD1.P  8(R2), [V3.H4]

    VSSHLL_4S(2, 4)
    VSSHLL_4S(3, 6)
    VMUL_4S(6, 4, 4)
    VSADDLP_2D(4, 8)
    VADD_2D(8, 0, 0)

    SUB     $4, R1

di16_scalar:
    CBZ     R1, di16_reduce

    MOVD    $0, R3

di16_scalar_loop:
    MOVH.P  2(R0), R4
    MOVH.P  2(R2), R5
    SXTH    R4, R4
    SXTH    R5, R5
    MUL     R4, R5, R6
    ADD     R6, R3
    SUB     $1, R1
    CBNZ    R1, di16_scalar_loop

    VEOR    V10.B16, V10.B16, V10.B16
    VMOV    R3, V10.D[0]
    VADD_2D(10, 0, 0)

di16_reduce:
    VADD_2D(0, 1, 2)

    VMOV    V2.D[0], R3
    VMOV    V2.D[1], R4
    ADD     R4, R3, R3

    SCVTFD  R3, F0
    FCVTDS  F0, F0

    FMOVS   F0, ret+48(FP)
    RET

// func euclideanInt8NEONKernel(a, b []int8) float32
TEXT ·euclideanInt8NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16   // acc0_2D
    VEOR    V1.B16, V1.B16, V1.B16   // acc1_2D

    CMP     $8, R1
    BLT     ei8_scalar

ei8_loop:
    VLD1.P  8(R0), [V2.B8]            // 8 int8 from a
    VLD1.P  8(R2), [V3.B8]            // 8 int8 from b

    // Sign-extend int8 → int16
    VSSHLL_8H(2, 4)                   // 8 int16 from a
    VSSHLL_8H(3, 5)                   // 8 int16 from b

    // Lower 4 int16 → int32
    VMOV    V4.D[1], V10.D[0]
    VMOV    V5.D[1], V11.D[0]
    VSSHLL_4S(4, 6)                   // a lower 4 → 4S
    VSSHLL_4S(5, 7)                   // b lower 4 → 4S
    VSSHLL_4S(10, 8)                  // a upper 4 → 4S
    VSSHLL_4S(11, 9)                  // b upper 4 → 4S

    VSUB_4S(7, 6, 6)                  // diff_l
    VSUB_4S(9, 8, 8)                  // diff_u

    VMUL_4S(6, 6, 6)                  // sq_l
    VMUL_4S(8, 8, 8)                  // sq_u

    VSADDLP_2D(6, 12)                 // acc partial
    VSADDLP_2D(8, 13)

    VADD_2D(12, 0, 0)                 // accumulate
    VADD_2D(13, 1, 1)

    SUB     $8, R1
    CMP     $8, R1
    BGE     ei8_loop

ei8_scalar:
    CBZ     R1, ei8_reduce

    MOVD    $0, R3                     // scalar accumulator (int64)

ei8_scalar_loop:
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R2), R5
    SXTB    R4, R4
    SXTB    R5, R5
    SUB     R5, R4, R6
    MUL     R6, R6, R6
    ADD     R6, R3
    SUB     $1, R1
    CBNZ    R1, ei8_scalar_loop

    VEOR    V10.B16, V10.B16, V10.B16
    VMOV    R3, V10.D[0]
    VADD_2D(10, 0, 0)

ei8_reduce:
    VADD_2D(0, 1, 2)

    VMOV    V2.D[0], R3
    VMOV    V2.D[1], R4
    ADD     R4, R3, R3

    SCVTFD  R3, F0
    FSQRTD  F0, F0
    FCVTDS  F0, F0

    FMOVS   F0, ret+48(FP)
    RET

// func dotInt8NEONKernel(a, b []int8) float32
TEXT ·dotInt8NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16   // acc0_2D
    VEOR    V1.B16, V1.B16, V1.B16   // acc1_2D

    CMP     $8, R1
    BLT     di8_scalar

di8_loop:
    VLD1.P  8(R0), [V2.B8]
    VLD1.P  8(R2), [V3.B8]

    VSSHLL_8H(2, 4)
    VSSHLL_8H(3, 5)

    VMOV    V4.D[1], V10.D[0]
    VMOV    V5.D[1], V11.D[0]
    VSSHLL_4S(4, 6)
    VSSHLL_4S(5, 7)
    VSSHLL_4S(10, 8)
    VSSHLL_4S(11, 9)

    VMUL_4S(7, 6, 6)                  // prod_l
    VMUL_4S(9, 8, 8)                  // prod_u

    VSADDLP_2D(6, 12)
    VSADDLP_2D(8, 13)

    VADD_2D(12, 0, 0)
    VADD_2D(13, 1, 1)

    SUB     $8, R1
    CMP     $8, R1
    BGE     di8_loop

di8_scalar:
    CBZ     R1, di8_reduce

    MOVD    $0, R3

di8_scalar_loop:
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R2), R5
    SXTB    R4, R4
    SXTB    R5, R5
    MUL     R4, R5, R6
    ADD     R6, R3
    SUB     $1, R1
    CBNZ    R1, di8_scalar_loop

    VEOR    V10.B16, V10.B16, V10.B16
    VMOV    R3, V10.D[0]
    VADD_2D(10, 0, 0)

di8_reduce:
    VADD_2D(0, 1, 2)

    VMOV    V2.D[0], R3
    VMOV    V2.D[1], R4
    ADD     R4, R3, R3

    SCVTFD  R3, F0
    FCVTDS  F0, F0

    FMOVS   F0, ret+48(FP)
    RET

// func euclideanUint8NEONKernel(a, b []uint8) float32
// Computes sqrt(sum (a-b)²) using UABDL + UMLAL for unsigned widening.
TEXT ·euclideanUint8NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V4.B16, V4.B16, V4.B16   // 4S accumulator
    MOVD    $0, R3                     // scalar tail accumulator

    CMP     $8, R1
    BLT     euc_u8_tail

euc_u8_loop:
    VLD1.P  8(R0), [V1.B8]            // 8 uint8 from a
    VLD1.P  8(R2), [V2.B8]            // 8 uint8 from b

    VUABDL_8H(2, 1, 3)                // V3.8H = |a - b| as 8× uint16
    VUMLAL_4S(3, 3, 4)                // V4.4S += lower 4 squares
    VUMLAL2_4S(3, 3, 4)               // V4.4S += upper 4 squares

    SUB     $8, R1
    CMP     $8, R1
    BGE     euc_u8_loop

euc_u8_tail:
    CBZ     R1, euc_u8_reduce
    MOVD    $0, R3

euc_u8_tail_loop:
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R2), R5
    SUB     R5, R4, R6                // diff
    MUL     R6, R6, R6                // square (fits in int32)
    ADD     R6, R3                    // accumulate
    SUB     $1, R1
    CBNZ    R1, euc_u8_tail_loop

euc_u8_reduce:
    // Reduce V4.4S → 2D (pairwise add)
    VSADDLP_2D(4, 10)                // V10.2D = {V4[0]+V4[1], V4[2]+V4[3]}

    // Add scalar tail from R3 if present
    CBZ     R3, euc_u8_reduce_vadd
    VEOR    V1.B16, V1.B16, V1.B16
    VMOV    R3, V1.D[0]
    VADD_2D(1, 10, 10)               // V10.2D += tail sum

euc_u8_reduce_vadd:
    // Sum the two 64-bit lanes of V10
    VMOV    V10.D[1], V11.D[0]
    VADD_2D(11, 10, 10)              // V10 = V10 + V11 → V10.D[0] = total

    VMOV    V10.D[0], R3             // R3 = total sum (int64)
    SCVTFD  R3, F0                   // int64 → float64
    FSQRTD  F0, F0                   // sqrt
    FCVTDS  F0, F0                   // float64 → float32

    FMOVS   F0, ret+48(FP)
    RET

// func dotUint8NEONKernel(a, b []uint8) float32
// Computes dot product using UMULL + UADDW for unsigned widening.
TEXT ·dotUint8NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V4.B16, V4.B16, V4.B16   // 4S accumulator
    MOVD    $0, R3                     // scalar tail accumulator

    CMP     $8, R1
    BLT     dot_u8_tail

dot_u8_loop:
    VLD1.P  8(R0), [V1.B8]            // 8 uint8 from a
    VLD1.P  8(R2), [V2.B8]            // 8 uint8 from b

    VUMULL_8H(2, 1, 3)                // V3.8H = a * b as 8× uint16
    VUADDW_4S(3, 4, 4)                // V4.4S += V3 lower 4 (widening)
    VUADDW2_4S(3, 4, 4)               // V4.4S += V3 upper 4 (widening)

    SUB     $8, R1
    CMP     $8, R1
    BGE     dot_u8_loop

dot_u8_tail:
    CBZ     R1, dot_u8_reduce
    MOVD    $0, R3

dot_u8_tail_loop:
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R2), R5
    MUL     R4, R5, R6                // product (fits in 16-bit)
    ADD     R6, R3
    SUB     $1, R1
    CBNZ    R1, dot_u8_tail_loop

dot_u8_reduce:
    VSADDLP_2D(4, 10)                // V10.2D = {V4[0]+V4[1], V4[2]+V4[3]}

    // Add scalar tail
    CBZ     R3, dot_u8_reduce_vadd
    VEOR    V1.B16, V1.B16, V1.B16
    VMOV    R3, V1.D[0]
    VADD_2D(1, 10, 10)

dot_u8_reduce_vadd:
    VMOV    V10.D[1], V11.D[0]
    VADD_2D(11, 10, 10)

    VMOV    V10.D[0], R3
    SCVTFD  R3, F0                   // int64 → float64
    FCVTDS  F0, F0                   // float64 → float32
    FMOVS   F0, ret+48(FP)
    RET

// func euclideanFloat64NEONKernel(a, b []float64) float32
// Computes sqrt(sum (a-b)²) using float64 NEON (2×float64 per instruction).

// func cosineInt8NEONKernel(a, b []int8) float32
// Computes 1 - dot(a,b) / sqrt(dot(a,a) * dot(b,b)).
// Uses sign-extension (int8→int16→int32) and computes all three products.
TEXT ·cosineInt8NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16   // dot_ab_lo (2× int64)
    VEOR    V1.B16, V1.B16, V1.B16   // dot_ab_hi
    VEOR    V2.B16, V2.B16, V2.B16   // dot_aa_lo (2× int64)
    VEOR    V3.B16, V3.B16, V3.B16   // dot_aa_hi
    VEOR    V4.B16, V4.B16, V4.B16   // dot_bb_lo (2× int64)
    VEOR    V5.B16, V5.B16, V5.B16   // dot_bb_hi

    CMP     $8, R1
    BLT     ci8_scalar

ci8_loop:
    VLD1.P  8(R0), [V6.B8]            // 8 int8 from a
    VLD1.P  8(R2), [V7.B8]            // 8 int8 from b

    // Sign-extend int8 → int16
    VSSHLL_8H(6, 8)                   // V8: 8 int16 from a
    VSSHLL_8H(7, 9)                   // V9: 8 int16 from b

    // Extract upper halves for widening to int32
    VMOV    V8.D[1], V10.D[0]
    VMOV    V9.D[1], V11.D[0]

    // Widen lower 4 of each to int32
    VSSHLL_4S(8, 12)                  // V12: a_lower 4 → 4× int32
    VSSHLL_4S(9, 13)                  // V13: b_lower 4 → 4× int32

    // Widen upper 4 of each to int32
    VSSHLL_4S(10, 14)                 // V14: a_upper 4 → 4× int32
    VSSHLL_4S(11, 15)                 // V15: b_upper 4 → 4× int32

    // dot_ab: a × b
    VMUL_4S(13, 12, 16)               // V16 = a_lower * b_lower (4× int32)
    VMUL_4S(15, 14, 17)               // V17 = a_upper * b_upper (4× int32)

    // dot_aa: a × a
    VMUL_4S(12, 12, 18)               // V18 = a_lower * a_lower (4× int32)
    VMUL_4S(14, 14, 19)               // V19 = a_upper * a_upper (4× int32)

    // dot_bb: b × b
    VMUL_4S(13, 13, 20)               // V20 = b_lower * b_lower (4× int32)
    VMUL_4S(15, 15, 21)               // V21 = b_upper * b_upper (4× int32)

    // Pairwise add each 4× int32 → 2× int64
    VSADDLP_2D(16, 22)                // V22: pair-add V16 → 2× int64
    VSADDLP_2D(17, 23)                // V23: pair-add V17 → 2× int64
    VSADDLP_2D(18, 24)                // V24: pair-add V18 → 2× int64
    VSADDLP_2D(19, 25)                // V25: pair-add V19 → 2× int64
    VSADDLP_2D(20, 26)                // V26: pair-add V20 → 2× int64
    VSADDLP_2D(21, 27)                // V27: pair-add V21 → 2× int64

    // Accumulate into dot_ab / dot_aa / dot_bb
    VADD_2D(22, 0, 0)                 // V0 += V22
    VADD_2D(23, 1, 1)                 // V1 += V23
    VADD_2D(24, 2, 2)                 // V2 += V24
    VADD_2D(25, 3, 3)                 // V3 += V25
    VADD_2D(26, 4, 4)                 // V4 += V26
    VADD_2D(27, 5, 5)                 // V5 += V27

    SUB     $8, R1
    CMP     $8, R1
    BGE     ci8_loop

ci8_scalar:
    CBZ     R1, ci8_combine

    MOVD    $0, R3                     // scalar dot_ab
    MOVD    $0, R4                     // scalar dot_aa
    MOVD    $0, R5                     // scalar dot_bb

ci8_scalar_loop:
    MOVBU.P 1(R0), R6
    MOVBU.P 1(R2), R7
    SXTB    R6, R6
    SXTB    R7, R7
    MUL     R6, R7, R8
    ADD     R8, R3
    MUL     R6, R6, R8
    ADD     R8, R4
    MUL     R7, R7, R8
    ADD     R8, R5

    SUB     $1, R1
    CBNZ    R1, ci8_scalar_loop

    // Merge scalar into vector accumulators
    VEOR    V10.B16, V10.B16, V10.B16
    VEOR    V11.B16, V11.B16, V11.B16
    VEOR    V12.B16, V12.B16, V12.B16
    VMOV    R3, V10.D[0]
    VMOV    R4, V11.D[0]
    VMOV    R5, V12.D[0]
    VADD_2D(10, 0, 0)
    VADD_2D(11, 2, 2)
    VADD_2D(12, 4, 4)

ci8_combine:
    // Reduce 2-lane int64 accumulators to scalar int64
    VADD_2D(1, 0, 0)                  // V0 = V0 + V1  (dot_ab)
    VADD_2D(3, 2, 2)                  // V2 = V2 + V3  (dot_aa)
    VADD_2D(5, 4, 4)                  // V4 = V4 + V5  (dot_bb)

    VMOV    V0.D[0], R3
    VMOV    V0.D[1], R4
    ADD     R4, R3, R3                // R3 = dot_ab (int64)

    VMOV    V2.D[0], R4
    VMOV    V2.D[1], R5
    ADD     R5, R4, R4                // R4 = dot_aa (int64)

    VMOV    V4.D[0], R5
    VMOV    V4.D[1], R6
    ADD     R6, R5, R5                // R5 = dot_bb (int64)

    // Convert to float64 and compute cosine distance
    SCVTFD  R3, F0                    // F0 = float64(dot_ab)
    SCVTFD  R4, F1                    // F1 = float64(dot_aa)
    SCVTFD  R5, F2                    // F2 = float64(dot_bb)

    FMULD   F2, F1, F1                // F1 = dot_aa * dot_bb
    FSQRTD  F1, F1                    // F1 = sqrt(dot_aa * dot_bb)
    FDIVD   F1, F0, F0                // F0 = dot_ab / norm_product
    FMOVD   $1.0, F1
    FSUBD   F0, F1, F0                // F0 = 1 - cosine_similarity
    FCVTDS  F0, F0                    // float64 → float32
    FMOVS   F0, ret+48(FP)
    RET
