//go:build arm64
#include "textflag.h"

#define VADDV_B16(n, d)  WORD $(0x4e31ba00 | ((n) << 5) | (d))
#define VADDV_B8(n, d)   WORD $(0x0e31ba00 | ((n) << 5) | (d))

// func hammingNEONKernel(a, b unsafe.Pointer, n int) int
TEXT ·hammingNEONKernel(SB), NOSPLIT, $0-32
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2

    MOVD    $0, R3
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $2, R2
    BLT     tail_check

loop_vec:
    VLD1.P  16(R0), [V1.B16]
    VLD1.P  16(R1), [V2.B16]
    
    VEOR    V1.B16, V2.B16, V3.B16
    VCNT    V3.B16, V3.B16
    VADD    V3.B16, V0.B16, V0.B16

    SUB     $2, R2
    CMP     $2, R2
    BGE     loop_vec

tail_check:
    CBZ     R2, reduce

tail_loop:
    MOVD    (R0), R4
    MOVD    (R1), R5
    EOR     R5, R4, R4
    
    // bitcount logic using scalar popcount (SWAR loop)
    MOVD    $0, R6
bit_loop:
    CBZ     R4, bit_done
    AND     R4, R4, R5
    SUB     $1, R4
    AND     R4, R5, R4 // R4 = R4 & (R4-1)
    ADD     $1, R6
    B       bit_loop
bit_done:
    ADD     R6, R3
    
    ADD     $8, R0
    ADD     $8, R1
    SUB     $1, R2
    CBNZ    R2, tail_loop

reduce:
    // Manual reduction of V0 bits if VADDV is suspicious
    // Sum V0.B16 elements into R3
    VMOV    V0.D[0], R4
    VMOV    V0.D[1], R5
    
    // Sum bytes of R4 and R5
    // Each byte in R4/R5 is a count of bits.
    // Summing them without overflow is tricky if many iterations.
    // But for < 32 iters, sum is < 256. 
    // We can use bit-masking to sum bytes in R4.
    
    MOVD    $0x00FF00FF00FF00FF, R6
    AND     R6, R4, R7 // Bytes 0, 2, 4, 6
    LSR     $8, R4, R4
    AND     R6, R4, R4 // Bytes 1, 3, 5, 7
    ADD     R7, R4, R4 // Pairs: halfwords 0, 1, 2, 3
    
    MOVD    $0x0000FFFF0000FFFF, R6
    AND     R6, R4, R7
    LSR     $16, R4, R4
    AND     R6, R4, R4
    ADD     R7, R4, R4 // Quads: words 0, 1
    
    LSR     $32, R4, R7
    ADD     R7, R4, R4 // R4[31:0] = sum of bytes in R4
    AND     $0xFFFFFFFF, R4
    ADD     R4, R3
    
    // Repeat for R5
    MOVD    $0x00FF00FF00FF00FF, R6
    AND     R6, R5, R7
    LSR     $8, R5, R5
    AND     R6, R5, R5
    ADD     R7, R5, R5
    
    MOVD    $0x0000FFFF0000FFFF, R6
    AND     R6, R5, R7
    LSR     $16, R5, R5
    AND     R6, R5, R5
    ADD     R7, R5, R5
    
    LSR     $32, R5, R7
    ADD     R7, R5, R5
    AND     $0xFFFFFFFF, R5
    ADD     R5, R3

    MOVD    R3, ret+24(FP)
    RET

// func andBitVectorNEON(dst, src unsafe.Pointer, n int)
TEXT ·andBitVectorNEON(SB), NOSPLIT, $0-24
    MOVD    dst+0(FP), R0
    MOVD    src+8(FP), R1
    MOVD    n+16(FP), R2

    CBZ     R2, and_done
    
loop_and:
    CMP     $2, R2
    BLT     and_tail
    
    VLD1    (R0), [V0.B16]
    VLD1.P  16(R1), [V1.B16]
    VAND    V1.B16, V0.B16, V0.B16
    VST1.P  [V0.B16], 16(R0)
    
    SUB     $2, R2
    B       loop_and

and_tail:
    CBZ     R2, and_done
    MOVD    (R0), R3
    MOVD    (R1), R4
    AND     R4, R3, R3
    MOVD    R3, (R0)

and_done:
    RET

// func countBitVectorNEONKernel(src unsafe.Pointer, n int) int
TEXT ·countBitVectorNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    n+8(FP), R1
    
    MOVD    $0, R2 // Total count
    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator
    
loop_cnt:
    CMP     $2, R1
    BLT     cnt_tail
    
    VLD1.P  16(R0), [V1.B16]
    VCNT    V1.B16, V1.B16
    VADD    V1.B16, V0.B16, V0.B16
    
    SUB     $2, R1
    // Potential overflow if n is very large (> 256 words)
    // To be safe, we should periodically reduce V0
    B       loop_cnt

cnt_tail:
    // Reduction logic
    VADDV_B16(0, 1)
    VMOV    V1.B[0], R3
    ADD     R3, R2
    
    CBZ     R1, cnt_done
cnt_tail_loop:
    MOVD    (R0), R3
    FMOVD   R3, F1
    // Popcount using scalar
    VCNT    V1.B8, V1.B8
    VADDV_B8(1, 1)
    VMOV    V1.B[0], R4
    ADD     R4, R2
    
    ADD     $8, R0
    SUB     $1, R1
    B       cnt_tail_loop

cnt_done:
    MOVD    R2, ret+16(FP)
    RET

