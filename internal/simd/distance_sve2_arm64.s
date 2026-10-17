//go:build arm64

#include "textflag.h"

// SVE2 Predicate-Driven Vector Distance Kernels for ARM64 (Graviton 3/4, Grace Hopper)
// Instruction encodings:
// PTRUE P1.S, ALL                   = WORD $0x2518e3e1
// WHILELT P0.S, R3, R1              = WORD $0x25610460
// LD1W Z1.S, P0/Z, [R0, R3, LSL #2] = WORD $0x85834001
// LD1W Z2.S, P0/Z, [R2, R3, LSL #2] = WORD $0x85834042
// FSUB Z3.S, Z1.S, Z2.S             = WORD $0x65a28023
// FMLA Z0.S, P0/M, Z3.S, Z3.S       = WORD $0x65a30060
// FMLA Z0.S, P0/M, Z1.S, Z2.S       = WORD $0x65a20020
// INCW R3                           = WORD $0x04e0e3e3
// FADDV S0, P1, Z0.S                = WORD $0x65a22400

// func l2SquaredSVE2Kernel(a, b []float32) float32
TEXT ·l2SquaredSVE2Kernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    CBZ     R1, zero_ret_l2

    // Zero out Z0 accumulator
    VEOR    V0.B16, V0.B16, V0.B16

    // PTRUE P1.S, ALL
    WORD    $0x2518e3e1

    // R3 = element loop index (0)
    MOVD    $0, R3

sve_l2_loop:
    // WHILELT P0.S, R3, R1
    WORD    $0x25610460
    BEQ     sve_l2_reduce

    // LD1W Z1.S, P0/Z, [R0, R3, LSL #2]
    WORD    $0x85834001
    // LD1W Z2.S, P0/Z, [R2, R3, LSL #2]
    WORD    $0x85834042

    // FSUB Z3.S, Z1.S, Z2.S
    WORD    $0x65a28023

    // FMLA Z0.S, P0/M, Z3.S, Z3.S
    WORD    $0x65a30060

    // INCW R3
    WORD    $0x04e0e3e3
    B       sve_l2_loop

sve_l2_reduce:
    // FADDV S0, P1, Z0.S
    WORD    $0x65a22400
    FMOVS   F0, ret+48(FP)
    RET

zero_ret_l2:
    FMOVS   $0.0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func dotSVE2Kernel(a, b []float32) float32
TEXT ·dotSVE2Kernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    CBZ     R1, zero_ret_dot

    // Zero out Z0 accumulator
    VEOR    V0.B16, V0.B16, V0.B16

    // PTRUE P1.S, ALL
    WORD    $0x2518e3e1

    // R3 = element loop index (0)
    MOVD    $0, R3

sve_dot_loop:
    // WHILELT P0.S, R3, R1
    WORD    $0x25610460
    BEQ     sve_dot_reduce

    // LD1W Z1.S, P0/Z, [R0, R3, LSL #2]
    WORD    $0x85834001
    // LD1W Z2.S, P0/Z, [R2, R3, LSL #2]
    WORD    $0x85834042

    // FMLA Z0.S, P0/M, Z1.S, Z2.S
    WORD    $0x65a20020

    // INCW R3
    WORD    $0x04e0e3e3
    B       sve_dot_loop

sve_dot_reduce:
    // FADDV S0, P1, Z0.S
    WORD    $0x65a22400
    FMOVS   F0, ret+48(FP)
    RET

zero_ret_dot:
    FMOVS   $0.0, F0
    FMOVS   F0, ret+48(FP)
    RET
