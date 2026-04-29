//go:build arm64

#include "textflag.h"

// func euclideanNEONKernel(a, b []float32) float32
TEXT ·euclideanNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    MOVD    $0, R3

    CMP     $4, R1
    BLT     tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x6ea2d423

    // Accumulate diff^2: V0 += V3 * V3
    VFMLA   V3.S4, V3.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F0, F1, F1     // F1 = F1 + F0 (accumulate V0[0])
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1

tail_loop:
    CBZ     R1, done
    
    FMOVS.P 4(R0), F4
    FMOVS.P 4(R2), F5
    
    FSUBS   F5, F4, F6
    FMULS   F6, F6, F6
    FADDS   F6, F1, F1
    
    SUB     $1, R1
    B       tail_loop

done:
    FSQRTS  F1, F1
    FMOVS   F1, ret+48(FP)
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

    CMP     $16, R1
    BLT     hd_tail_loop

hd_loop_16x:
    // Prefetch PLDL1KEEP, [x0, #128] and [x2, #128]
    WORD    $0xf8804000
    WORD    $0xf8804040

    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $16, R1
    CMP     $16, R1
    BGE     hd_loop_16x

    // FADD V1.4S, V0.4S, V0.4S
    WORD    $0x4e21d400
    // FADD V2.4S, V0.4S, V0.4S
    WORD    $0x4e22d400
    // FADD V3.4S, V0.4S, V0.4S
    WORD    $0x4e23d400

hd_tail_loop:
    CMP     $4, R1
    BLT     hd_scalar_reduction

hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VSUB    V8.S4, V4.S4, V12.S4
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

    CMP     $16, R1
    BLT     dot_hd_tail_loop

dot_hd_loop_16x:
    // Prefetch PLDL1KEEP, [x0, #128] and [x2, #128]
    WORD    $0xf8804000
    WORD    $0xf8804040

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

    SUB     $16, R1
    CMP     $16, R1
    BGE     dot_hd_loop_16x

    // FADD V1.4S, V0.4S, V0.4S
    WORD    $0x4e21d400
    // FADD V2.4S, V0.4S, V0.4S
    WORD    $0x4e22d400
    // FADD V3.4S, V0.4S, V0.4S
    WORD    $0x4e23d400

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
    MOVD    $0, R3

    CMP     $4, R1
    BLT     l2_tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

l2_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x6ea2d423

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
// FP16 OPTIMIZED IMPLEMENTATIONS
// ============================================================================

// func euclideanF16NEONKernel(a, b []float16.Num) float32
TEXT ·euclideanF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator

    CMP     $8, R1
    BLT     euc_f16_tail

euc_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8] // Load 8x FP16 (128 bits)
    VLD1.P  16(R2), [V2.H8] // Load 8x FP16 (128 bits)

    // Convert lower 4
    WORD    $0x0e217823
    WORD    $0x0e217844

    VSUB    V4.S4, V3.S4, V5.S4
    VFMLA   V5.S4, V5.S4, V0.S4

    // Convert upper 4
    WORD    $0x4e217823
    WORD    $0x4e217844

    VSUB    V4.S4, V3.S4, V5.S4
    VFMLA   V5.S4, V5.S4, V0.S4

    SUB     $8, R1
    CMP     $8, R1
    BGE     euc_f16_loop_8x

    // Final reduction of V0.S4 into F0
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

euc_f16_tail:
    CBZ     R1, euc_f16_done
    
    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    
    // Convert R3, R4 to float32
    WORD    $0x0e217823 
    WORD    $0x0e217844
    
    FSUBS   F4, F3, F5
    FMULS   F5, F5, F5
    FADDS   F5, F0, F0
    
    SUB     $1, R1
    B       euc_f16_tail

euc_f16_done:
    FSQRTS  F0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func dotF16NEONKernel(a, b []float16.Num) float32
TEXT ·dotF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $8, R1
    BLT     dot_f16_tail

dot_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8]
    VLD1.P  16(R2), [V2.H8]

    // Lower 4
    WORD    $0x0e217823
    WORD    $0x0e217844
    VFMLA   V4.S4, V3.S4, V0.S4

    // Upper 4
    WORD    $0x4e217823
    WORD    $0x4e217844
    VFMLA   V4.S4, V3.S4, V0.S4

    SUB     $8, R1
    CMP     $8, R1
    BGE     dot_f16_loop_8x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

dot_f16_tail:
    CBZ     R1, dot_f16_done

    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    WORD    $0x0e217823
    WORD    $0x0e217844
    
    FMULS   F4, F3, F5
    FADDS   F5, F0, F0

    SUB     $1, R1
    B       dot_f16_tail

dot_f16_done:
    FMOVS   F0, ret+48(FP)
    RET

// func cosineF16NEONKernel(a, b []float16.Num) float32
TEXT ·cosineF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    // Accumulators
    VEOR    V0.B16, V0.B16, V0.B16 // Dot
    VEOR    V10.B16, V10.B16, V10.B16 // NormA
    VEOR    V11.B16, V11.B16, V11.B16 // NormB
    
    // Initialize scalar accumulators for tail case (Dim < 8)
    FMOVS   $0.0, F1
    FMOVS   $0.0, F2

    CMP     $8, R1
    BLT     cos_f16_tail

cos_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8]
    VLD1.P  16(R2), [V2.H8]

    // --- Lower 4 ---
    WORD    $0x0e217823
    WORD    $0x0e217844
    
    VFMLA   V4.S4, V3.S4, V0.S4   // Dot += A * B
    VFMLA   V3.S4, V3.S4, V10.S4  // NormA += A * A
    VFMLA   V4.S4, V4.S4, V11.S4  // NormB += B * B

    // --- Upper 4 ---
    WORD    $0x4e217823
    WORD    $0x4e217844

    VFMLA   V4.S4, V3.S4, V0.S4   // Dot += A * B
    VFMLA   V3.S4, V3.S4, V10.S4  // NormA += A * A
    VFMLA   V4.S4, V4.S4, V11.S4  // NormB += B * B

    SUB     $8, R1
    CMP     $8, R1
    BGE     cos_f16_loop_8x

    // Reduction for Dot (V0)
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

    // Reduction for NormA (V10)
    VMOV    V10.S[0], V20.S[0] // Copy to V20/F20
    VMOV    V10.S[1], V21.S[0]
    VMOV    V10.S[2], V22.S[0]
    VMOV    V10.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F1 // Store in F1 for tail calc

    // Reduction for NormB (V11)
    VMOV    V11.S[0], V24.S[0]
    VMOV    V11.S[1], V25.S[0]
    VMOV    V11.S[2], V26.S[0]
    VMOV    V11.S[3], V27.S[0]
    FADDS   F25, F24, F24
    FADDS   F26, F24, F24
    FADDS   F27, F24, F24
    FMOVS   F24, F2 // Store in F2 for tail calc

cos_f16_tail:
    CBZ     R1, cos_f16_calc

    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    WORD    $0x0e217823
    WORD    $0x0e217844
    
    // Dot
    FMULS   F4, F3, F5
    FADDS   F5, F0, F0
    
    // NormA
    FMULS   F3, F3, F3
    FADDS   F3, F1, F1
    
    // NormB
    FMULS   F4, F4, F4
    FADDS   F4, F2, F2

    SUB     $1, R1
    B       cos_f16_tail

cos_f16_calc:
    FMOVS   $0.0, F3
    FCMPS   F1, F3
    BEQ     ret_one
    FCMPS   F2, F3
    BEQ     ret_one

    FSQRTS  F1, F1
    FSQRTS  F2, F2
    FMULS   F2, F1, F3
    FDIVS   F3, F0, F0
    
    FMOVS   $1.0, F4
    FSUBS   F0, F4, F0
    FMOVS   F0, ret+48(FP)
    RET

ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET

// Macros to resolve Go assembler issues with vector floating-point instructions
#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_V(m, n, d) WORD $(0x4ea0d400 | ((m) << 16) | ((n) << 5) | (d))

// func fastWalshHadamardTransform32NEONKernel(a []float32)
TEXT ·fastWalshHadamardTransform32NEONKernel(SB), NOSPLIT, $0-24
    MOVD    a_base+0(FP), R0
    
    // Load 32 floats into V0-V7 (128 bytes total)
    VLD1.P  16(R0), [V0.S4]
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R0), [V2.S4]
    VLD1.P  16(R0), [V3.S4]
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R0), [V7.S4]
    
    // Stage 1 (h=16): Butterflies (V0,V4), (V1,V5), (V2,V6), (V3,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0) // V0 = V0 + V4
    VFSUB_V(4, 16, 4) // V4 = V16 - V4
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1) 
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3) 
    VFSUB_V(7, 16, 7)

    // Stage 2 (h=8): (V0,V2), (V1,V3), (V4,V6), (V5,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(2, 0, 0)
    VFSUB_V(2, 16, 2)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(3, 1, 1)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(6, 4, 4)
    VFSUB_V(6, 16, 6)
    
    VORR    V5.B16, V5.B16, V16.B16
    VFADD_V(7, 5, 5)
    VFSUB_V(7, 16, 7)

    // Stage 3 (h=4): (V0,V1), (V2,V3), (V4,V5), (V6,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 16, 1)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(3, 2, 2)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(5, 4, 4)
    VFSUB_V(5, 16, 5)
    
    VORR    V6.B16, V6.B16, V16.B16
    VFADD_V(7, 6, 6)
    VFSUB_V(7, 16, 7)

    // Stage 4 (h=2): Use VEXT and intra-register butterfly
    VEXT    $8, V0.B16, V0.B16, V8.B16
    VFADD_V(8, 0, 9)
    VFSUB_V(8, 0, 10)
    VMOV    V9.D[0], V0.D[0]
    VMOV    V10.D[0], V0.D[1]
    
    VEXT    $8, V1.B16, V1.B16, V8.B16
    VFADD_V(8, 1, 9)
    VFSUB_V(8, 1, 10)
    VMOV    V9.D[0], V1.D[0]
    VMOV    V10.D[0], V1.D[1]
    
    VEXT    $8, V2.B16, V2.B16, V8.B16
    VFADD_V(8, 2, 9)
    VFSUB_V(8, 2, 10)
    VMOV    V9.D[0], V2.D[0]
    VMOV    V10.D[0], V2.D[1]
    
    VEXT    $8, V3.B16, V3.B16, V8.B16
    VFADD_V(8, 3, 9)
    VFSUB_V(8, 3, 10)
    VMOV    V9.D[0], V3.D[0]
    VMOV    V10.D[0], V3.D[1]
    
    VEXT    $8, V4.B16, V4.B16, V8.B16
    VFADD_V(8, 4, 9)
    VFSUB_V(8, 4, 10)
    VMOV    V9.D[0], V4.D[0]
    VMOV    V10.D[0], V4.D[1]
    
    VEXT    $8, V5.B16, V5.B16, V8.B16
    VFADD_V(8, 5, 9)
    VFSUB_V(8, 5, 10)
    VMOV    V9.D[0], V5.D[0]
    VMOV    V10.D[0], V5.D[1]
    
    VEXT    $8, V6.B16, V6.B16, V8.B16
    VFADD_V(8, 6, 9)
    VFSUB_V(8, 6, 10)
    VMOV    V9.D[0], V6.D[0]
    VMOV    V10.D[0], V6.D[1]
    
    VEXT    $8, V7.B16, V7.B16, V8.B16
    VFADD_V(8, 7, 9)
    VFSUB_V(8, 7, 10)
    VMOV    V9.D[0], V7.D[0]
    VMOV    V10.D[0], V7.D[1]

    // Stage 5 (h=1): Use VREV64 and VMOV
    VREV64  V0.S4, V11.S4
    VFADD_V(11, 0, 12)
    VFSUB_V(11, 0, 8) 
    VMOV    V12.S[0], V0.S[0]
    VMOV    V8.S[0], V0.S[1]
    VMOV    V12.S[2], V0.S[2]
    VMOV    V8.S[2], V0.S[3]
    
    VREV64  V1.S4, V11.S4
    VFADD_V(11, 1, 12)
    VFSUB_V(11, 1, 8) 
    VMOV    V12.S[0], V1.S[0]
    VMOV    V8.S[0], V1.S[1]
    VMOV    V12.S[2], V1.S[2]
    VMOV    V8.S[2], V1.S[3]

    VREV64  V2.S4, V11.S4
    VFADD_V(11, 2, 12)
    VFSUB_V(11, 2, 13)
    VMOV    V12.S[0], V2.S[0]
    VMOV    V13.S[0], V2.S[1]
    VMOV    V12.S[2], V2.S[2]
    VMOV    V13.S[2], V2.S[3]
    
    VREV64  V3.S4, V11.S4
    VFADD_V(11, 3, 12)
    VFSUB_V(11, 3, 13)
    VMOV    V12.S[0], V3.S[0]
    VMOV    V13.S[0], V3.S[1]
    VMOV    V12.S[2], V3.S[2]
    VMOV    V13.S[2], V3.S[3]

    VREV64  V4.S4, V11.S4
    VFADD_V(11, 4, 12)
    VFSUB_V(11, 4, 13)
    VMOV    V12.S[0], V4.S[0]
    VMOV    V13.S[0], V4.S[1]
    VMOV    V12.S[2], V4.S[2]
    VMOV    V13.S[2], V4.S[3]

    VREV64  V5.S4, V11.S4
    VFADD_V(11, 5, 12)
    VFSUB_V(11, 5, 13)
    VMOV    V12.S[0], V5.S[0]
    VMOV    V13.S[0], V5.S[1]
    VMOV    V12.S[2], V5.S[2]
    VMOV    V13.S[2], V5.S[3]

    VREV64  V6.S4, V11.S4
    VFADD_V(11, 6, 12)
    VFSUB_V(11, 6, 13)
    VMOV    V12.S[0], V6.S[0]
    VMOV    V13.S[0], V6.S[1]
    VMOV    V12.S[2], V6.S[2]
    VMOV    V13.S[2], V6.S[3]

    VREV64  V7.S4, V11.S4
    VFADD_V(11, 7, 12)
    VFSUB_V(11, 7, 13)
    VMOV    V12.S[0], V7.S[0]
    VMOV    V13.S[0], V7.S[1]
    VMOV    V12.S[2], V7.S[2]
    VMOV    V13.S[2], V7.S[3]

    // Store 32 floats back to memory
    MOVD    a_base+0(FP), R0
    VST1.P  [V0.S4], 16(R0)
    VST1.P  [V1.S4], 16(R0)
    VST1.P  [V2.S4], 16(R0)
    VST1.P  [V3.S4], 16(R0)
    VST1.P  [V4.S4], 16(R0)
    VST1.P  [V5.S4], 16(R0)
    VST1.P  [V6.S4], 16(R0)
    VST1.P  [V7.S4], 16(R0)
    RET

// func vectorButterflyNEONKernel(a, b []float32)
TEXT ·vectorButterflyNEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Individual loads
    VLD1    (R0), [V0.S4]
    VLD1    (R1), [V1.S4]
    
    VORR    V0.B16, V0.B16, V2.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 2, 1)
    
    VST1    [V0.S4], (R0)
    VST1    [V1.S4], (R1)
    RET

// func vectorButterfly16NEONKernel(a, b []float32)
TEXT ·vectorButterfly16NEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Load 16 floats (4x 128-bit registers) from a and b
    VLD1    (R0), [V0.S4, V1.S4, V2.S4, V3.S4]
    VLD1    (R1), [V4.S4, V5.S4, V6.S4, V7.S4]
    
    // Butterfly operations: a' = a + b, b' = a - b
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0)
    VFSUB_V(4, 16, 4)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1)
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3)
    VFSUB_V(7, 16, 7)
    
    // Store results back
    VST1    [V0.S4, V1.S4, V2.S4, V3.S4], (R0)
    VST1    [V4.S4, V5.S4, V6.S4, V7.S4], (R1)
    RET

// func randomSignFlipNEONKernel(a []float32, seed int64)
TEXT ·randomSignFlipNEONKernel(SB), NOSPLIT, $0-32
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    seed+24(FP), R2

    MOVD    $0x5851f42d4c957f2d, R3 // Constant

    CMP     $4, R1
    BLT     sign_flip_tail

sign_flip_loop:
    // Element 0
    MUL     R3, R2, R8      // h = seed * c
    LSR     $63, R8, R8     // h >> 63
    LSL     $31, R8, R8     // mask = (h >> 63) << 31
    ADD     $1, R2, R5      // next_seed = seed + 1

    // Element 1
    MUL     R3, R5, R9
    LSR     $63, R9, R9
    LSL     $31, R9, R9
    ADD     $2, R2, R6

    // Element 2
    MUL     R3, R6, R10
    LSR     $63, R10, R10
    LSL     $31, R10, R10
    ADD     $3, R2, R7

    // Element 3
    MUL     R3, R7, R11
    LSR     $63, R11, R11
    LSL     $31, R11, R11

    // Pack into V1.S4
    VMOV    R8, V1.S[0]
    VMOV    R9, V1.S[1]
    VMOV    R10, V1.S[2]
    VMOV    R11, V1.S[3]

    // Load 4 floats
    VLD1    (R0), [V0.S4]

    // XOR with sign mask
    VEOR    V1.B16, V0.B16, V0.B16

    // Store back and increment pointer
    VST1.P  [V0.S4], 16(R0)

    ADD     $4, R2          // seed += 4
    SUB     $4, R1          // len -= 4
    CMP     $4, R1
    BGE     sign_flip_loop

sign_flip_tail:
    CBZ     R1, sign_flip_done
    
    // Tail case for 1-3 elements
    MUL     R3, R2, R8
    LSR     $63, R8, R8
    LSL     $31, R8, R8
    
    FMOVS.P 4(R0), F0
    VMOV    R8, V1.S[0]
    VEOR    V1.B16, V0.B16, V0.B16
    FMOVS   F0, -4(R0)
    
    ADD     $1, R2
    SUB     $1, R1
    B       sign_flip_tail

sign_flip_done:
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
    VMOV    V10.S[1], V4.S[0]
    VMOV    V10.S[2], V5.S[0]
    VMOV    V10.S[3], V6.S[0]
    FMOVS   $0.0, F4
    FADDS   F0, F4, F4     // Add V10[0]
    FADDS   F5, F4, F4
    FADDS   F6, F4, F4
    // F4 now has normA (move V0 result first)

    // Reduction for NormB to F5  
    VMOV    V11.S[1], V4.S[0]
    VMOV    V11.S[2], V5.S[0]
    VMOV    V11.S[3], V6.S[0]
    FMOVS   $0.0, F5
    FADDS   F0, F5, F5     // Add V11[0], using original F0 is wrong
    FADDS   F4, F5, F5
    FADDS   F6, F5, F5
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

// ============================================================================
// FIXED-DIMENSION DOT KERNELS
// ============================================================================

// func dot128NEONKernel(a, b []float32) float32
TEXT ·dot128NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $8, R1   // 128/16 = 8 iterations

dot128_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot128_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot384NEONKernel(a, b []float32) float32
TEXT ·dot384NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $24, R1  // 384/16 = 24 iterations

dot384_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot384_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot768NEONKernel(a, b []float32) float32
TEXT ·dot768NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $48, R1  // 768/16 = 48 iterations

dot768_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot768_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot1024NEONKernel(a, b []float32) float32
TEXT ·dot1024NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $64, R1  // 1024/16 = 64 iterations

dot1024_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot1024_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot1536NEONKernel(a, b []float32) float32
TEXT ·dot1536NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $96, R1  // 1536/16 = 96 iterations

dot1536_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot1536_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot3072NEONKernel(a, b []float32) float32
TEXT ·dot3072NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $192, R1  // 3072/16 = 192 iterations

dot3072_loop:
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

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot3072_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// ============================================================================
// FIXED-DIMENSION L2SQUARED KERNELS
// ============================================================================

// func l2Squared128NEONKernel(a, b []float32) float32
TEXT ·l2Squared128NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $32, R1

l2sq128_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq128_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared384NEONKernel(a, b []float32) float32
TEXT ·l2Squared384NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $96, R1

l2sq384_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq384_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared768NEONKernel(a, b []float32) float32
TEXT ·l2Squared768NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $192, R1

l2sq768_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq768_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared1024NEONKernel(a, b []float32) float32
TEXT ·l2Squared1024NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $256, R1

l2sq1024_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq1024_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared1536NEONKernel(a, b []float32) float32
TEXT ·l2Squared1536NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $384, R1

l2sq1536_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq1536_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared3072NEONKernel(a, b []float32) float32
TEXT ·l2Squared3072NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $768, R1

l2sq3072_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VSUB    V8.S4, V4.S4, V12.S4
    VSUB    V9.S4, V5.S4, V13.S4
    VSUB    V10.S4, V6.S4, V14.S4
    VSUB    V11.S4, V7.S4, V15.S4

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq3072_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// ============================================================================
// TURBOQUANT (INT4/INT2) KERNELS - stubs, use Go fallback
// ============================================================================

// func dotInt4NeonKernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   $0.0, F0
    FMOVS   F0, ret+24(FP)
    RET

// func dotInt2NeonKernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt2NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   $0.0, F0
    FMOVS   F0, ret+24(FP)
    RET
