//go:build amd64
#include "textflag.h"

// func hammingAVX2Kernel(a, b unsafe.Pointer, n int) int
TEXT ·hammingAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), CX

    XORQ    AX, AX        // Accumulator

    // Check header
    CMPQ    CX, $4
    JB      tail_check

loop_4:
    MOVQ    (SI), BX
    MOVQ    (DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    8(SI), BX
    MOVQ    8(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    16(SI), BX
    MOVQ    16(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    24(SI), BX
    MOVQ    24(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $4, CX
    CMPQ    CX, $4
    JAE     loop_4

tail_check:
    CMPQ    CX, $0
    JE      done

tail_loop:
    MOVQ    (SI), BX
    MOVQ    (DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    ADDQ    $8, SI
    ADDQ    $8, DI
    DECQ    CX
    JNZ     tail_loop

done:
    MOVQ    AX, ret+24(FP)
    RET
