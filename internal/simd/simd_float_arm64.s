//go:build arm64

#include "textflag.h"
#include "simd_arm64_macros.h"

// func euclideanNEONKernel(a, b []float32) float32
TEXT ·euclideanNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    FMOVS   $0.0, F1
    MOVD    $0, R3

    CMP     $4, R1
    BLT     tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x4ea2d423

    // Accumulate diff^2: V0 += V3 * V3
    VFMLA   V3.S4, V3.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

tail_loop:
    CBZ     R1, done
    
    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FSUBS   F2, F1, F3
    FMULS   F3, F3, F3
    FADDS   F3, F0, F0
    
    SUB     $1, R1
    B       tail_loop

done:
    FSQRTS  F0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func euclideanHighDimNEONKernel(a, b []float32) float32
TEXT ·euclideanHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V16.B16, V16.B16, V16.B16
    VEOR    V17.B16, V17.B16, V17.B16
    VEOR    V18.B16, V18.B16, V18.B16
    VEOR    V19.B16, V19.B16, V19.B16

    CMP     $32, R1
    BLT     hd_tail_loop

hd_loop_32x:
    // Interleave 8 streams of (load, sub, fmla)
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V13.S4]
    VFSUB_V(12, 4, 4)
    VFSUB_V(13, 5, 5)

    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V15.S4]
    VFSUB_V(14, 6, 6)
    VFSUB_V(15, 7, 7)

    VFMLA   V4.S4, V4.S4, V0.S4
    VFMLA   V5.S4, V5.S4, V1.S4
    VFMLA   V6.S4, V6.S4, V2.S4
    VFMLA   V7.S4, V7.S4, V3.S4

    VLD1.P  16(R0), [V8.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V9.S4]
    VLD1.P  16(R2), [V13.S4]
    VFSUB_V(12, 8, 8)
    VFSUB_V(13, 9, 9)

    VLD1.P  16(R0), [V10.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V11.S4]
    VLD1.P  16(R2), [V15.S4]
    VFSUB_V(14, 10, 10)
    VFSUB_V(15, 11, 11)

    VFMLA   V8.S4, V8.S4, V16.S4
    VFMLA   V9.S4, V9.S4, V17.S4
    VFMLA   V10.S4, V10.S4, V18.S4
    VFMLA   V11.S4, V11.S4, V19.S4

    SUB     $32, R1
    CMP     $32, R1
    BGE     hd_loop_32x

    // Sum accumulators
    VFADD_V(16, 0, 0)
    VFADD_V(17, 1, 1)
    VFADD_V(18, 2, 2)
    VFADD_V(19, 3, 3)
    
    VFADD_V(1, 0, 0)
    VFADD_V(3, 2, 2)
    VFADD_V(2, 0, 0)

hd_tail_loop:
    CMP     $4, R1
    BLT     hd_scalar_reduction

hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFSUB_V(8, 4, 12)
    VFMLA   V12.S4, V12.S4, V0.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     hd_tail_4x

hd_scalar_reduction:
    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1     // F1 = F1 + F0 (accumulate V0[0])
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

hd_scalar_tail:
    CBZ     R1, hd_done
    
    FMOVS.P 4(R0), F5
    FMOVS.P 4(R2), F6
    
    FSUBS   F6, F5, F7
    FMULS   F7, F7, F7
    FADDS   F7, F1, F1
    
    SUB     $1, R1
    B       hd_scalar_tail

hd_done:
    FSQRTS  F1, F1
    FMOVS   F1, ret+48(FP)
    RET

// func dotNEONKernel(a, b []float32) float32
TEXT ·dotNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $4, R1
    BLT     dot_tail

dot_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]
    
    VFMLA   V2.S4, V1.S4, V0.S4
    
    SUB     $4, R1
    CMP     $4, R1
    BGE     dot_loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

dot_tail:
    CBZ     R1, dot_done

    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FMULS   F2, F1, F3
    FADDS   F3, F0, F0

    SUB     $1, R1
    B       dot_tail

dot_done:
    FMOVS   F0, ret+48(FP)
    RET

// func dotHighDimNEONKernel(a, b []float32) float32
TEXT ·dotHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V16.B16, V16.B16, V16.B16
    VEOR    V17.B16, V17.B16, V17.B16
    VEOR    V18.B16, V18.B16, V18.B16
    VEOR    V19.B16, V19.B16, V19.B16

    CMP     $32, R1
    BLT     dot_hd_tail_loop

dot_hd_loop_32x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V13.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V15.S4]

    VFMLA   V12.S4, V4.S4, V0.S4
    VFMLA   V13.S4, V5.S4, V1.S4
    VFMLA   V14.S4, V6.S4, V2.S4
    VFMLA   V15.S4, V7.S4, V3.S4

    VLD1.P  16(R0), [V8.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V9.S4]
    VLD1.P  16(R2), [V13.S4]
    VLD1.P  16(R0), [V10.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V11.S4]
    VLD1.P  16(R2), [V15.S4]

    VFMLA   V12.S4, V8.S4, V16.S4
    VFMLA   V13.S4, V9.S4, V17.S4
    VFMLA   V14.S4, V10.S4, V18.S4
    VFMLA   V15.S4, V11.S4, V19.S4

    SUB     $32, R1
    CMP     $32, R1
    BGE     dot_hd_loop_32x

    // Final sum of 8 accumulators
    VFADD_V(16, 0, 0)
    VFADD_V(17, 1, 1)
    VFADD_V(18, 2, 2)
    VFADD_V(19, 3, 3)
    
    VFADD_V(1, 0, 0)
    VFADD_V(3, 2, 2)
    VFADD_V(2, 0, 0)

dot_hd_tail_loop:
    CMP     $4, R1
    BLT     dot_hd_scalar_reduction

dot_hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFMLA   V8.S4, V4.S4, V0.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     dot_hd_tail_4x

dot_hd_scalar_reduction:
    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

dot_hd_scalar_tail:
    CBZ     R1, dot_hd_done
    
    FMOVS.P 4(R0), F2
    FMOVS.P 4(R2), F3
    
    FMULS   F3, F2, F4
    FADDS   F4, F1, F1
    
    SUB     $1, R1
    B       dot_hd_scalar_tail

dot_hd_done:
    FMOVS   F1, ret+48(FP)
    RET


// func l2SquaredNEONKernel(a, b []float32) float32
TEXT ·l2SquaredNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    FMOVS   $0.0, F1
    MOVD    $0, R3

    CMP     $4, R1
    BLT     l2_tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

l2_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x4ea2d423

    // Accumulate diff^2: V0 += V3 * V3
    VFMLA   V3.S4, V3.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     l2_loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

l2_tail_loop:
    CBZ     R1, l2_done
    
    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FSUBS   F2, F1, F3
    FMULS   F3, F3, F3
    FADDS   F3, F0, F0
    
    SUB     $1, R1
    B       l2_tail_loop

l2_done:
    // No FSQRTS for L2 Squared
    FMOVS   F0, ret+48(FP)
    RET

// ============================================================================
// FLOAT32 COSINE KERNELS
// ============================================================================

// func cosineNEONKernel(a, b []float32) float32
TEXT ·cosineNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16  // Dot accumulator
    VEOR    V10.B16, V10.B16, V10.B16 // NormA accumulator
    VEOR    V11.B16, V11.B16, V11.B16 // NormB accumulator

    FMOVS   $0.0, F0             // Init accumulators
    FMOVS   $0.0, F4
    FMOVS   $0.0, F5

    CMP     $4, R1
    BLT     cos_tail

cos_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    VFMLA   V2.S4, V1.S4, V0.S4     // Dot += A * B
    VFMLA   V1.S4, V1.S4, V10.S4   // NormA += A * A
    VFMLA   V2.S4, V2.S4, V11.S4   // NormB += B * B

    SUB     $4, R1
    CMP     $4, R1
    BGE     cos_loop_4x

    // Reduction for Dot (V0) - accumulate 4 lanes to F0
    VMOV    V0.S[1], V4.S[0]
    VMOV    V0.S[2], V5.S[0]
    VMOV    V0.S[3], V6.S[0]
    FADDS   F4, F0, F0     // F0 += other lanes
    FADDS   F5, F0, F0
    FADDS   F6, F0, F0
    // F0 now has total dot

    // Reduction for NormA to F4
    VMOV    V10.S[1], V5.S[0]
    VMOV    V10.S[2], V6.S[0]
    VMOV    V10.S[3], V7.S[0]
    FMOVS   F10, F4
    FADDS   F5, F4, F4
    FADDS   F6, F4, F4
    FADDS   F7, F4, F4
    // F4 now has normA (move V0 result first)

    // Reduction for NormB to F5
    VMOV    V11.S[1], V6.S[0]
    VMOV    V11.S[2], V7.S[0]
    VMOV    V11.S[3], V8.S[0]
    FMOVS   F11, F5
    FADDS   F6, F5, F5
    FADDS   F7, F5, F5
    FADDS   F8, F5, F5
    // F5 now has normB

cos_tail:
    CBZ     R1, cos_calc

    FMOVS.P 4(R0), F10
    FMOVS.P 4(R2), F11

    FMULS   F11, F10, F12        // a*b
    FADDS   F12, F0, F0         // Add to dot (F0)

    FMULS   F10, F10, F10       // a*a
    FADDS   F10, F4, F4        // Add to normA (F4)

    FMULS   F11, F11, F11      // b*b
    FADDS   F11, F5, F5       // Add to normB (F5)

    SUB     $1, R1
    B       cos_tail

cos_calc:
    FMOVS   $0.0, F6
    FCMPS   F4, F6           // Check normA != 0
    BEQ     cos_ret_one
    FCMPS   F5, F6           // Check normB != 0
    BEQ     cos_ret_one

    FSQRTS  F4, F4           // sqrt(normA)
    FSQRTS  F5, F5           // sqrt(normB)
    FMULS   F5, F4, F4       // denominator = normA * normB

    FDIVS   F4, F0, F0       // cosine_dist = dot / denominator

    FMOVS   $1.0, F6
    FSUBS   F0, F6, F0       // 1 - cosine_dist

    FMOVS   F0, ret+48(FP)
    RET

cos_ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func cosineHighDimNEONKernel(a, b []float32) float32
TEXT ·cosineHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V10.B16, V10.B16, V10.B16
    VEOR    V11.B16, V11.B16, V11.B16
    VEOR    V12.B16, V12.B16, V12.B16
    VEOR    V13.B16, V13.B16, V13.B16

    CMP     $16, R1
    BLT     cos_hd_tail_loop

cos_hd_loop_16x:
    WORD    $0xf8804000  // PLDL1KEEP [R0, #128]
    WORD    $0xf8804040  // PLDL1KEEP [R2, #128]

    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    VFMLA   V4.S4, V4.S4, V10.S4
    VFMLA   V5.S4, V5.S4, V11.S4
    VFMLA   V6.S4, V6.S4, V12.S4
    VFMLA   V7.S4, V7.S4, V13.S4

    VFMLA   V8.S4, V8.S4, V10.S4
    VFMLA   V9.S4, V9.S4, V11.S4
    VFMLA   V10.S4, V10.S4, V12.S4
    VFMLA   V11.S4, V11.S4, V13.S4

    SUB     $16, R1
    CMP     $16, R1
    BGE     cos_hd_loop_16x

    // Reduction
    WORD    $0x4e21d400  // FADD V1.4S, V0.4S, V0.4S
    WORD    $0x4e22d400  // FADD V2.4S, V0.4S, V0.4S
    WORD    $0x4e23d400  // FADD V3.4S, V0.4S, V0.4S

    FMOVS   $0.0, F20
    VMOV    V10.S[1], V21.S[0]
    VMOV    V10.S[2], V22.S[0]
    VMOV    V10.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F2

    FMOVS   $0.0, F20
    VMOV    V11.S[1], V21.S[0]
    VMOV    V11.S[2], V22.S[0]
    VMOV    V11.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F3

    FMOVS   $0.0, F20
    VMOV    V12.S[1], V21.S[0]
    VMOV    V12.S[2], V22.S[0]
    VMOV    V12.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F4

    FMOVS   $0.0, F20
    VMOV    V13.S[1], V21.S[0]
    VMOV    V13.S[2], V22.S[0]
    VMOV    V13.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F5

    FADDS   F4, F2, F2
    FADDS   F5, F3, F3
    FMOVS   F2, F1
    FMOVS   F3, F2

cos_hd_tail_loop:
    CMP     $4, R1
    BLT     cos_hd_scalar_reduction

cos_hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V4.S4, V4.S4, V10.S4
    VFMLA   V8.S4, V8.S4, V10.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     cos_hd_tail_4x

cos_hd_scalar_reduction:
    FMOVS   $0.0, F1
    FMOVS   $0.0, F2
    FMOVS   $0.0, F3
    VMOV    V0.S[1], V4.S[0]
    VMOV    V0.S[2], V5.S[0]
    VMOV    V0.S[3], V6.S[0]
    FADDS   F0, F1, F1
    FADDS   F4, F1, F1
    FADDS   F5, F1, F1
    FADDS   F6, F1, F1
    FMOVS   F1, F0
    VMOV    V10.S[1], V4.S[0]
    VMOV    V10.S[2], V5.S[0]
    VMOV    V10.S[3], V6.S[0]
    FADDS   F4, F2, F2
    FADDS   F5, F2, F2
    FADDS   F6, F2, F2
    VMOV    V11.S[1], V4.S[0]
    VMOV    V11.S[2], V5.S[0]
    VMOV    V11.S[3], V6.S[0]
    FADDS   F4, F3, F3
    FADDS   F5, F3, F3
    FADDS   F6, F3, F3

cos_hd_scalar_tail:
    CBZ     R1, cos_hd_calc

    FMOVS.P 4(R0), F4
    FMOVS.P 4(R2), F5

    FMULS   F5, F4, F6
    FADDS   F6, F0, F0
    FMULS   F4, F4, F4
    FADDS   F4, F2, F2
    FMULS   F5, F5, F5
    FADDS   F5, F3, F3

    SUB     $1, R1
    B       cos_hd_scalar_tail

cos_hd_calc:
    FMOVS   $0.0, F4
    FCMPS   F2, F4
    BEQ     cos_hd_ret_one
    FCMPS   F3, F4
    BEQ     cos_hd_ret_one

    FSQRTS  F2, F2
    FSQRTS  F3, F3
    FMULS   F3, F2, F2
    FDIVS   F2, F0, F0

    FMOVS   $1.0, F4
    FSUBS   F0, F4, F0

    FMOVS   F0, ret+48(FP)
    RET

cos_hd_ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET
