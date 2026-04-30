//go:build arm64

#include "textflag.h"

// func memcpyNTA(dst, src unsafe.Pointer, n int)
TEXT ·memcpyNTA(SB), NOSPLIT, $0
    MOVD dst+0(FP), R0
    MOVD src+8(FP), R1
    MOVD n+16(FP), R2

    CMP $32, R2
    BLT tail_memcpy

loop32:
    VLD1.P 32(R1), [V0.B16, V1.B16]
    // STNP Q0, Q1, [R0]
    WORD $0x28000400
    ADD $32, R0
    
    SUB $32, R2
    CMP $32, R2
    BGE loop32

tail_memcpy:
    CBZ R2, done_memcpy
    MOVBU.P 1(R1), R3
    MOVBU.P R3, 1(R0)
    SUB $1, R2
    B tail_memcpy

done_memcpy:
    RET
