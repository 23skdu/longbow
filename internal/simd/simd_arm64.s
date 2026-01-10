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
    // Opcode: 0x6e... (FSUB)
    // Size 4S = 10 (bits 23-22 = 10)
    // Rm = 2 (00010)
    // Byte 2 = 10100010 = A2
    // Full: 0x6ea2d423
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

// func euclideanF16NEON(a, b []float16.Num) float32
TEXT ·euclideanF16NEON(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $4, R1
    BLT     euc_f16_tail

euc_f16_loop_4x:
    VLD1.P  8(R0), [V1.H4]
    VLD1.P  8(R2), [V2.H4]

    // FCVTL V1.4H, V3.4S -> 0x0e217823
    // FCVTL V2.4H, V4.4S -> 0x0e217844
    WORD    $0x0e217823
    WORD    $0x0e217844

    // VSUB V4.4S, V3.4S, V5.4S
    VSUB    V4.S4, V3.S4, V5.S4
    VFMLA   V5.S4, V5.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     euc_f16_loop_4x

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
    
    // FCVTL V1.4H, V3.4S -> 0x0e217823
    // FCVTL V2.4H, V4.4S -> 0x0e217844
    WORD    $0x0e217823
    WORD    $0x0e217844
    FMOVS   F3, F1
    FMOVS   F4, F2

    FSUBS   F2, F1, F3
    FMULS   F3, F3, F3
    FADDS   F3, F0, F0

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

    CMP     $4, R1
    BLT     dot_f16_tail

dot_f16_loop_4x:
    VLD1.P  8(R0), [V1.H4]
    VLD1.P  8(R2), [V2.H4]

    // FCVTL V1.4H, V3.4S -> 0x0e217823
    // FCVTL V2.4H, V4.4S -> 0x0e217844
    WORD    $0x0e217823
    WORD    $0x0e217844

    VFMLA   V3.S4, V4.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     dot_f16_loop_4x

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
    
    // FCVTL V1.4H, V3.4S -> 0x0e217823
    // FCVTL V2.4H, V4.4S -> 0x0e217844
    WORD    $0x0e217823
    WORD    $0x0e217844
    FMOVS   F3, F1
    FMOVS   F4, F2

    FMULS   F2, F1, F3
    FADDS   F3, F0, F0

    SUB     $1, R1
    B       dot_f16_tail

dot_f16_done:
    FMOVS   F0, ret+48(FP)
    RET
