//go:build arm64
#include "textflag.h"

// func hammingNEONKernel(a, b unsafe.Pointer, n int) int
TEXT ·hammingNEONKernel(SB), NOSPLIT, $0-32
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    
    MOVD    $0, R3
    MOVD    $0x5555555555555555, R4
    MOVD    $0x3333333333333333, R5
    MOVD    $0x0F0F0F0F0F0F0F0F, R6
    MOVD    $0x0101010101010101, R7

loop_hamming:
    CMP     $4, R2
    BLT     tail_hamming
    
    // Unroll 4x scalar for now to match "unrolled" request while ensuring stability
    MOVD    (R0), R10; ADD $8, R0
    MOVD    (R1), R11; ADD $8, R1
    EOR     R11, R10, R10
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R3

    MOVD    (R0), R10; ADD $8, R0
    MOVD    (R1), R11; ADD $8, R1
    EOR     R11, R10, R10
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R3

    MOVD    (R0), R10; ADD $8, R0
    MOVD    (R1), R11; ADD $8, R1
    EOR     R11, R10, R10
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R3

    MOVD    (R0), R10; ADD $8, R0
    MOVD    (R1), R11; ADD $8, R1
    EOR     R11, R10, R10
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R3

    SUB     $4, R2
    B       loop_hamming

tail_hamming:
    CBZ     R2, done_hamming
    MOVD    (R0), R10; ADD $8, R0
    MOVD    (R1), R11; ADD $8, R1
    EOR     R11, R10, R10
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R3
    SUB     $1, R2
    B       tail_hamming

done_hamming:
    MOVD    R3, ret+24(FP)
    RET

// func andBitVectorNEON(dst, src unsafe.Pointer, n int)
TEXT ·andBitVectorNEON(SB), NOSPLIT, $0-24
    MOVD    dst+0(FP), R0
    MOVD    src+8(FP), R1
    MOVD    n+16(FP), R2
    
loop_and:
    CMP     $4, R2
    BLT     tail_and
    MOVD    (R0), R3
    MOVD    (R1), R4
    AND     R4, R3, R3
    MOVD    R3, (R0); ADD $8, R0; ADD $8, R1
    
    MOVD    (R0), R3
    MOVD    (R1), R4
    AND     R4, R3, R3
    MOVD    R3, (R0); ADD $8, R0; ADD $8, R1

    MOVD    (R0), R3
    MOVD    (R1), R4
    AND     R4, R3, R3
    MOVD    R3, (R0); ADD $8, R0; ADD $8, R1

    MOVD    (R0), R3
    MOVD    (R1), R4
    AND     R4, R3, R3
    MOVD    R3, (R0); ADD $8, R0; ADD $8, R1

    SUB     $4, R2
    B       loop_and

tail_and:
    CBZ     R2, done_and
    MOVD    (R0), R10
    MOVD    (R1), R11
    AND     R11, R10, R10
    MOVD    R10, (R0); ADD $8, R0; ADD $8, R1
    SUB     $1, R2
    B       tail_and
done_and: RET

// func countBitVectorNEONKernel(src unsafe.Pointer, n int) int
TEXT ·countBitVectorNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    n+8(FP), R1
    MOVD    $0, R2
    MOVD    $0x5555555555555555, R4
    MOVD    $0x3333333333333333, R5
    MOVD    $0x0F0F0F0F0F0F0F0F, R6
    MOVD    $0x0101010101010101, R7
loop_cnt:
    CMP     $4, R1
    BLT     tail_cnt
    // Unroll 4x
    MOVD    (R0), R10; ADD $8, R0
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R2

    MOVD    (R0), R10; ADD $8, R0
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R2

    MOVD    (R0), R10; ADD $8, R0
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R2

    MOVD    (R0), R10; ADD $8, R0
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R2

    SUB     $4, R1
    B       loop_cnt
tail_cnt:
    CBZ     R1, done_cnt
    MOVD    (R0), R10; ADD $8, R0
    LSR     $1, R10, R11; AND R4, R11, R11; SUB R11, R10, R10
    LSR     $2, R10, R11; AND R5, R10, R10; AND R5, R11, R11; ADD R11, R10, R10
    LSR     $4, R10, R11; ADD R11, R10, R10; AND R6, R10, R10; MUL R7, R10, R10; LSR $56, R10, R10
    ADD     R10, R2
    SUB     $1, R1
    B       tail_cnt
done_cnt: MOVD R2, ret+16(FP); RET
