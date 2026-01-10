//go:build arm64

#include "textflag.h"

// func euclideanNEON(a, b []float32) float32
TEXT ·euclideanNEON(SB), NOSPLIT, $0-52
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

// func dotNEON(a, b []float32) float32
TEXT ·dotNEON(SB), NOSPLIT, $0-52
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

// ============================================================================
// FP16 OPTIMIZED IMPLEMENTATIONS
// ============================================================================

// func euclideanF16NEON(a, b []float16.Num) float32
TEXT ·euclideanF16NEON(SB), NOSPLIT, $0-52
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

    // Reduction
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
    
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    WORD    $0x0e217823 
    WORD    $0x0e217844
    
    VMOV    V3.S[0], V5.S[0]
    VMOV    V4.S[0], V6.S[0]
    FSUBS   F4, F3, F5
    FMULS   F5, F5, F5
    FADDS   F5, F0, F0
    
    SUB     $1, R1
    B       euc_f16_tail

euc_f16_done:
    FSQRTS  F0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func dotF16NEON(a, b []float16.Num) float32
TEXT ·dotF16NEON(SB), NOSPLIT, $0-52
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

// func cosineF16NEON(a, b []float16.Num) float32
TEXT ·cosineF16NEON(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    // Accumulators
    VEOR    V0.B16, V0.B16, V0.B16 // Dot
    VEOR    V10.B16, V10.B16, V10.B16 // NormA
    VEOR    V11.B16, V11.B16, V11.B16 // NormB

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
