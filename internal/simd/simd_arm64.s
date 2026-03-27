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
    RET
