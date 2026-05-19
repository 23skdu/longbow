//go:build arm64
#include "textflag.h"

// Macros for instructions not supported by all Go assembler versions
#define UDOT_V(m, n, d) WORD $(0x6e809400 | ((m) << 16) | ((n) << 5) | (d))
#define VABD_V(m, n, d) WORD $(0x6e207400 | ((m) << 16) | ((n) << 5) | (d))

// func dotSQ8NEONKernel(a, b unsafe.Pointer, n int) int32
TEXT ·dotSQ8NEONKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16 // Total accumulator (4x32-bit)

    CMP     $16, R2
    BLT     dot_tail

dot_loop_16x:
    VLD1.P  16(R0), [V1.B16]
    VLD1.P  16(R1), [V2.B16]
    
    // UDOT V0.4S, V1.16B, V2.16B
    UDOT_V(2, 1, 0)

    SUB     $16, R2
    CMP     $16, R2
    BGE     dot_loop_16x

    // Final reduction
    VADDV   V0.S4, V0
    VMOV    V0.S[0], R3
    B       dot_done

dot_tail:
    MOVD    $0, R3
dot_tail_loop:
    CBZ     R2, dot_done
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    MUL     R5, R4, R6
    ADD     R6, R3
    SUB     $1, R2
    B       dot_tail_loop

dot_done:
    MOVW    R3, ret+24(FP)
    RET

// func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32
TEXT ·euclideanSQ8NEONKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16 // 32-bit acc

    CMP     $16, R2
    BLT     tail_loop

loop_16x:
    VLD1.P  16(R0), [V1.B16]
    VLD1.P  16(R1), [V2.B16]

    // Absolute difference: VABD V2.B16, V1.B16, V3.B16
    VABD_V(2, 1, 3)

    // UDOT V0.4S, V3.16B, V3.16B (Square and accumulate into 32-bit)
    UDOT_V(3, 3, 0)

    SUB     $16, R2
    CMP     $16, R2
    BGE     loop_16x

    // Reduction
    VADDV   V0.S4, V0
    VMOV    V0.S[0], R3
    B       tail_loop_init

tail_loop:
    MOVD    $0, R3
tail_loop_init:
    CBZ     R2, done
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R6
    MUL     R6, R6, R6
    ADD     R6, R3

    SUB     $1, R2
    B       tail_loop_init

done:
    MOVW    R3, ret+24(FP)
    RET
