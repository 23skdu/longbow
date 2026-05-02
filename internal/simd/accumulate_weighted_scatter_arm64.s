//go:build arm64

#include "textflag.h"

// func accumulateWeightedScatterNEONKernel(dst, targets, weights unsafe.Pointer, factor float32, n int)
TEXT ·accumulateWeightedScatterNEONKernel(SB), NOSPLIT, $0-40
    MOVD    dst+0(FP), R0
    MOVD    targets+8(FP), R1
    MOVD    weights+16(FP), R2
    FMOVS   factor+24(FP), F0
    MOVD    n+32(FP), R3

    CBZ     R3, done

    // Broadcast factor to V0.S4
    VDUP    V0.S[0], V0.S4

loop_4x:
    CMP     $4, R3
    BLT     tail

    // Load 4 targets (uint32)
    VLD1.P  16(R1), [V1.S4]
    
    // Load 4 weights (float32)
    VLD1.P  16(R2), [V2.S4]

    // V3 = weights * factor (V3.4S = V2.4S * V0.4S)
    VEOR    V3.B16, V3.B16, V3.B16
    VFMLA   V2.S4, V0.S4, V3.S4

    // Scatter-add (Scalar part as NEON doesn't support scatter-store)
    VMOV    V1.S[0], R4 // target 0
    VMOV    V1.S[1], R5 // target 1
    VMOV    V1.S[2], R6 // target 2
    VMOV    V1.S[3], R7 // target 3

    VMOV    V3.S[0], V4.S[0] // val 0
    VMOV    V3.S[1], V5.S[0] // val 1
    VMOV    V3.S[2], V6.S[0] // val 2
    VMOV    V3.S[3], V7.S[0] // val 3

    // dst[target0] += val0
    LSL     $2, R4      // offset = index * 4
    FMOVS   (R0)(R4), F8
    FADDS   F4, F8, F8
    FMOVS   F8, (R0)(R4)

    // dst[target1] += val1
    LSL     $2, R5
    FMOVS   (R0)(R5), F8
    FADDS   F5, F8, F8
    FMOVS   F8, (R0)(R5)

    // dst[target2] += val2
    LSL     $2, R6
    FMOVS   (R0)(R6), F8
    FADDS   F6, F8, F8
    FMOVS   F8, (R0)(R6)

    // dst[target3] += val3
    LSL     $2, R7
    FMOVS   (R0)(R7), F8
    FADDS   F7, F8, F8
    FMOVS   F8, (R0)(R7)

    SUB     $4, R3
    B       loop_4x

tail:
    CBZ     R3, done

scalar_loop:
    MOVWU.P 4(R1), R4 // Load 1 target and increment
    FMOVS.P 4(R2), F4 // Load 1 weight and increment
    
    FMULS   F0, F4, F5 // product = factor * weight
    
    LSL     $2, R4
    FMOVS   (R0)(R4), F6
    FADDS   F5, F6, F6
    FMOVS   F6, (R0)(R4)
    
    SUB     $1, R3
    CBNZ    R3, scalar_loop

done:
    RET
