//go:build arm64
#include "textflag.h"

// Stub file to prevent build error
// func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32
TEXT ·euclideanSQ8NEONKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2

    MOVD    $0, R3                 // Total accumulator

    CMP     $8, R2
    BLT     tail_loop

loop_8x:
    // Unroll 8x scalar for stability
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R12
    MUL     R12, R12, R12
    ADD     R12, R3

    SUB     $8, R2
    CMP     $8, R2
    BGE     loop_8x

tail_loop:
    CBZ     R2, done
    MOVBU.P 1(R0), R4
    MOVBU.P 1(R1), R5
    SUB     R5, R4, R6
    MUL     R6, R6, R6
    ADD     R6, R3

    SUB     $1, R2
    B       tail_loop

done:
    MOVW    R3, ret+24(FP)
    RET
