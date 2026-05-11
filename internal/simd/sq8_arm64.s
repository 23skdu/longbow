//go:build arm64
#include "textflag.h"

// Stub file to prevent build error
// Macros for instructions not supported by all Go assembler versions
#define UDOT_V(m, n, d) WORD $(0x6e809400 | ((m) << 16) | ((n) << 5) | (d))
#define VABD_V(m, n, d) WORD $(0x6e207400 | ((m) << 16) | ((n) << 5) | (d))
#define VMLAL_V(m, n, d) WORD $(0x2e208400 | ((m) << 16) | ((n) << 5) | (d))
#define VMLAL2_V(m, n, d) WORD $(0x6e208400 | ((m) << 16) | ((n) << 5) | (d))
#define VADAL_V(n, d) WORD $(0x4e606000 | ((n) << 5) | (d))
#define VADDP_V(n, d) WORD $(0x4e60bd00 | ((n) << 5) | (d))

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
    VADDP_V(0, 0) // VADDP V0.4S, V0.4S, V0.4S
    VADDP_V(0, 0) 
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

    MOVD    $0, R3                 // Total accumulator

    CMP     $16, R2
    BLT     tail_loop

    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator (lower)
    VEOR    V1.B16, V1.B16, V1.B16 // Accumulator (upper)
    VEOR    V10.B16, V10.B16, V10.B16 // 32-bit acc
    VEOR    V11.B16, V11.B16, V11.B16 // 32-bit acc

loop_16x:
    VLD1.P  16(R0), [V2.B16]
    VLD1.P  16(R1), [V3.B16]

    // Absolute difference: VABD V3.B16, V2.B16, V4.B16
    VABD_V(3, 2, 4)

    // Square and accumulate into 16-bit: VMLAL V4.B8, V4.B8, V0.H8
    VMLAL_V(4, 4, 0)
    VMLAL2_V(4, 4, 1)

    SUB     $16, R2
    CMP     $16, R2
    BGE     loop_16x

    // Accumulate 16-bit into 32-bit: VADAL V0.H8, V10.S4
    VADAL_V(0, 10)
    VADAL_V(1, 11)
    
    // VADD V10.S4, V11.S4, V0.S4
    VADD    V10.S4, V11.S4, V0.S4
    
    // Reduction
    VADDP_V(0, 0)
    VADDP_V(0, 0)
    VMOV    V0.S[0], R3

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
