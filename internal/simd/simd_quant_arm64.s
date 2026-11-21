//go:build arm64

#include "textflag.h"
#include "simd_arm64_macros.h"

TEXT ·fastWalshHadamardTransform32NEONKernel(SB), NOSPLIT, $0-24
    MOVD    a_base+0(FP), R0
    
    // Load 32 floats into V0-V7 (128 bytes total)
    VLD1.P  16(R0), [V0.S4]
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R0), [V2.S4]
    VLD1.P  16(R0), [V3.S4]
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R0), [V7.S4]
    
    // Stage 1 (h=16): Butterflies (V0,V4), (V1,V5), (V2,V6), (V3,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0) // V0 = V0 + V4
    VFSUB_V(4, 16, 4) // V4 = V16 - V4
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1) 
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3) 
    VFSUB_V(7, 16, 7)

    // Stage 2 (h=8): (V0,V2), (V1,V3), (V4,V6), (V5,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(2, 0, 0)
    VFSUB_V(2, 16, 2)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(3, 1, 1)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(6, 4, 4)
    VFSUB_V(6, 16, 6)
    
    VORR    V5.B16, V5.B16, V16.B16
    VFADD_V(7, 5, 5)
    VFSUB_V(7, 16, 7)

    // Stage 3 (h=4): (V0,V1), (V2,V3), (V4,V5), (V6,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 16, 1)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(3, 2, 2)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(5, 4, 4)
    VFSUB_V(5, 16, 5)
    
    VORR    V6.B16, V6.B16, V16.B16
    VFADD_V(7, 6, 6)
    VFSUB_V(7, 16, 7)

    // Stage 4 (h=2): Use VEXT and intra-register butterfly
    VEXT    $8, V0.B16, V0.B16, V8.B16
    VFADD_V(8, 0, 9)
    VFSUB_V(8, 0, 10)
    VMOV    V9.D[0], V0.D[0]
    VMOV    V10.D[0], V0.D[1]
    
    VEXT    $8, V1.B16, V1.B16, V8.B16
    VFADD_V(8, 1, 9)
    VFSUB_V(8, 1, 10)
    VMOV    V9.D[0], V1.D[0]
    VMOV    V10.D[0], V1.D[1]
    
    VEXT    $8, V2.B16, V2.B16, V8.B16
    VFADD_V(8, 2, 9)
    VFSUB_V(8, 2, 10)
    VMOV    V9.D[0], V2.D[0]
    VMOV    V10.D[0], V2.D[1]
    
    VEXT    $8, V3.B16, V3.B16, V8.B16
    VFADD_V(8, 3, 9)
    VFSUB_V(8, 3, 10)
    VMOV    V9.D[0], V3.D[0]
    VMOV    V10.D[0], V3.D[1]
    
    VEXT    $8, V4.B16, V4.B16, V8.B16
    VFADD_V(8, 4, 9)
    VFSUB_V(8, 4, 10)
    VMOV    V9.D[0], V4.D[0]
    VMOV    V10.D[0], V4.D[1]
    
    VEXT    $8, V5.B16, V5.B16, V8.B16
    VFADD_V(8, 5, 9)
    VFSUB_V(8, 5, 10)
    VMOV    V9.D[0], V5.D[0]
    VMOV    V10.D[0], V5.D[1]
    
    VEXT    $8, V6.B16, V6.B16, V8.B16
    VFADD_V(8, 6, 9)
    VFSUB_V(8, 6, 10)
    VMOV    V9.D[0], V6.D[0]
    VMOV    V10.D[0], V6.D[1]
    
    VEXT    $8, V7.B16, V7.B16, V8.B16
    VFADD_V(8, 7, 9)
    VFSUB_V(8, 7, 10)
    VMOV    V9.D[0], V7.D[0]
    VMOV    V10.D[0], V7.D[1]

    // Stage 5 (h=1): Use VREV64 and VMOV
    VREV64  V0.S4, V11.S4
    VFADD_V(11, 0, 12)
    VFSUB_V(11, 0, 8) 
    VMOV    V12.S[0], V0.S[0]
    VMOV    V8.S[0], V0.S[1]
    VMOV    V12.S[2], V0.S[2]
    VMOV    V8.S[2], V0.S[3]
    
    VREV64  V1.S4, V11.S4
    VFADD_V(11, 1, 12)
    VFSUB_V(11, 1, 8) 
    VMOV    V12.S[0], V1.S[0]
    VMOV    V8.S[0], V1.S[1]
    VMOV    V12.S[2], V1.S[2]
    VMOV    V8.S[2], V1.S[3]

    VREV64  V2.S4, V11.S4
    VFADD_V(11, 2, 12)
    VFSUB_V(11, 2, 13)
    VMOV    V12.S[0], V2.S[0]
    VMOV    V13.S[0], V2.S[1]
    VMOV    V12.S[2], V2.S[2]
    VMOV    V13.S[2], V2.S[3]
    
    VREV64  V3.S4, V11.S4
    VFADD_V(11, 3, 12)
    VFSUB_V(11, 3, 13)
    VMOV    V12.S[0], V3.S[0]
    VMOV    V13.S[0], V3.S[1]
    VMOV    V12.S[2], V3.S[2]
    VMOV    V13.S[2], V3.S[3]

    VREV64  V4.S4, V11.S4
    VFADD_V(11, 4, 12)
    VFSUB_V(11, 4, 13)
    VMOV    V12.S[0], V4.S[0]
    VMOV    V13.S[0], V4.S[1]
    VMOV    V12.S[2], V4.S[2]
    VMOV    V13.S[2], V4.S[3]

    VREV64  V5.S4, V11.S4
    VFADD_V(11, 5, 12)
    VFSUB_V(11, 5, 13)
    VMOV    V12.S[0], V5.S[0]
    VMOV    V13.S[0], V5.S[1]
    VMOV    V12.S[2], V5.S[2]
    VMOV    V13.S[2], V5.S[3]

    VREV64  V6.S4, V11.S4
    VFADD_V(11, 6, 12)
    VFSUB_V(11, 6, 13)
    VMOV    V12.S[0], V6.S[0]
    VMOV    V13.S[0], V6.S[1]
    VMOV    V12.S[2], V6.S[2]
    VMOV    V13.S[2], V6.S[3]

    VREV64  V7.S4, V11.S4
    VFADD_V(11, 7, 12)
    VFSUB_V(11, 7, 13)
    VMOV    V12.S[0], V7.S[0]
    VMOV    V13.S[0], V7.S[1]
    VMOV    V12.S[2], V7.S[2]
    VMOV    V13.S[2], V7.S[3]

    // Store 32 floats back to memory
    MOVD    a_base+0(FP), R0
    VST1.P  [V0.S4], 16(R0)
    VST1.P  [V1.S4], 16(R0)
    VST1.P  [V2.S4], 16(R0)
    VST1.P  [V3.S4], 16(R0)
    VST1.P  [V4.S4], 16(R0)
    VST1.P  [V5.S4], 16(R0)
    VST1.P  [V6.S4], 16(R0)
    VST1.P  [V7.S4], 16(R0)
    RET

// func vectorButterflyNEONKernel(a, b []float32)
TEXT ·vectorButterflyNEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Individual loads
    VLD1    (R0), [V0.S4]
    VLD1    (R1), [V1.S4]
    
    VORR    V0.B16, V0.B16, V2.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 2, 1)
    
    VST1    [V0.S4], (R0)
    VST1    [V1.S4], (R1)
    RET

// func vectorButterfly16NEONKernel(a, b []float32)
TEXT ·vectorButterfly16NEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Load 16 floats (4x 128-bit registers) from a and b
    VLD1    (R0), [V0.S4, V1.S4, V2.S4, V3.S4]
    VLD1    (R1), [V4.S4, V5.S4, V6.S4, V7.S4]
    
    // Butterfly operations: a' = a + b, b' = a - b
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0)
    VFSUB_V(4, 16, 4)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1)
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3)
    VFSUB_V(7, 16, 7)
    
    // Store results back
    VST1    [V0.S4, V1.S4, V2.S4, V3.S4], (R0)
    VST1    [V4.S4, V5.S4, V6.S4, V7.S4], (R1)
    RET

// func randomSignFlipNEONKernel(a []float32, seed int64)
TEXT ·randomSignFlipNEONKernel(SB), NOSPLIT, $0-32
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    seed+24(FP), R2

    MOVD    $0x5851f42d4c957f2d, R3 // Constant

    CMP     $4, R1
    BLT     sign_flip_tail

sign_flip_loop:
    // Element 0
    MUL     R3, R2, R8      // h = seed * c
    LSR     $63, R8, R8     // h >> 63
    LSL     $31, R8, R8     // mask = (h >> 63) << 31
    ADD     $1, R2, R5      // next_seed = seed + 1

    // Element 1
    MUL     R3, R5, R9
    LSR     $63, R9, R9
    LSL     $31, R9, R9
    ADD     $2, R2, R6

    // Element 2
    MUL     R3, R6, R10
    LSR     $63, R10, R10
    LSL     $31, R10, R10
    ADD     $3, R2, R7

    // Element 3
    MUL     R3, R7, R11
    LSR     $63, R11, R11
    LSL     $31, R11, R11

    // Pack into V1.S4
    VMOV    R8, V1.S[0]
    VMOV    R9, V1.S[1]
    VMOV    R10, V1.S[2]
    VMOV    R11, V1.S[3]

    // Load 4 floats
    VLD1    (R0), [V0.S4]

    // XOR with sign mask
    VEOR    V1.B16, V0.B16, V0.B16

    // Store back and increment pointer
    VST1.P  [V0.S4], 16(R0)

    ADD     $4, R2          // seed += 4
    SUB     $4, R1          // len -= 4
    CMP     $4, R1
    BGE     sign_flip_loop

sign_flip_tail:
    CBZ     R1, sign_flip_done
    
    // Tail case for 1-3 elements
    MUL     R3, R2, R8
    LSR     $63, R8, R8
    LSL     $31, R8, R8
    
    FMOVS.P 4(R0), F0
    VMOV    R8, V1.S[0]
    VEOR    V1.B16, V0.B16, V0.B16
    FMOVS   F0, -4(R0)
    
    ADD     $1, R2
    SUB     $1, R1
    B       sign_flip_tail

sign_flip_done:
    RET

TEXT ·dotInt4NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    
    MOVD    $0, R3                 // Total accumulator
    MOVD    $0x0F0F0F0F0F0F0F0F, R4 // Low nibble mask
    
    VEOR    V0.B16, V0.B16, V0.B16 // Clear vector accumulator (16-bit lanes)

    CMP     $16, R2
    BLT     dot4_tail

dot4_loop_16x:
    VLD1.P  16(R0), [V1.B16]       // Load 16 bytes (32x 4-bit)
    VLD1.P  16(R1), [V2.B16]
    
    VMOV    R4, V10.D[0]
    VMOV    R4, V10.D[1]
    
    // Extract low 4 bits (low nibble)
    VAND    V1.B16, V10.B16, V3.B16 
    VAND    V2.B16, V10.B16, V4.B16 
    
    // Extract high 4 bits (high nibble)
    VUSHR   $4, V1.B16, V5.B16      
    VUSHR   $4, V2.B16, V6.B16      
    
    // Multiply and accumulate: (a_low * b_low) + (a_high * b_high)
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    // VMLAL V5.8B, V6.8B, V0.8H
    WORD    $0x2e2680a0
    // VMLAL2 V5.16B, V6.16B, V0.8H
    WORD    $0x6e2680a0

    SUB     $16, R2
    CMP     $16, R2
    BGE     dot4_loop_16x

    // Manual reduction of V0.8H into R3
    VMOV    V0.H[0], R3
    VMOV    V0.H[1], R10
    ADD     R10, R3
    VMOV    V0.H[2], R10
    ADD     R10, R3
    VMOV    V0.H[3], R10
    ADD     R10, R3
    VMOV    V0.H[4], R10
    ADD     R10, R3
    VMOV    V0.H[5], R10
    ADD     R10, R3
    VMOV    V0.H[6], R10
    ADD     R10, R3
    VMOV    V0.H[7], R10
    ADD     R10, R3

dot4_tail:
    CBZ     R2, dot4_done
    MOVBU.P 1(R0), R10
    MOVBU.P 1(R1), R11
    
    // Low nibble
    AND     $0x0F, R10, R12
    AND     $0x0F, R11, R13
    MUL     R12, R13, R12
    ADD     R12, R3
    
    // High nibble
    LSR     $4, R10, R10
    LSR     $4, R11, R11
    MUL     R10, R11, R10
    ADD     R10, R3
    
    SUB     $1, R2
    B       dot4_tail

dot4_done:
    MOVW    R3, ret+24(FP)
    RET

// func dotInt2NeonKernel(a, b unsafe.Pointer, n int) int32
TEXT ·dotInt2NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    
    MOVD    $0, R3                 // Total accumulator
    MOVD    $0x0303030303030303, R4 // 2-bit mask
    
    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator

    CMP     $16, R2
    BLT     dot2_tail

dot2_loop_16x:
    VLD1.P  16(R0), [V1.B16]
    VLD1.P  16(R1), [V2.B16]
    
    VMOV    R4, V10.D[0]
    VMOV    R4, V10.D[1]
    
    // 2-bit extraction (4 elements per byte)
    // Element 0: bits 0-1
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 1: bits 2-3
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 2: bits 4-5
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 3: bits 6-7
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060

    SUB     $16, R2
    CMP     $16, R2
    BGE     dot2_loop_16x

    // Manual reduction of V0.8H into R3
    VMOV    V0.H[0], R3
    VMOV    V0.H[1], R10
    ADD     R10, R3
    VMOV    V0.H[2], R10
    ADD     R10, R3
    VMOV    V0.H[3], R10
    ADD     R10, R3
    VMOV    V0.H[4], R10
    ADD     R10, R3
    VMOV    V0.H[5], R10
    ADD     R10, R3
    VMOV    V0.H[6], R10
    ADD     R10, R3
    VMOV    V0.H[7], R10
    ADD     R10, R3

dot2_tail:
    CBZ     R2, dot2_done
    MOVBU.P 1(R0), R10
    MOVBU.P 1(R1), R11
    
    // 4 elements per byte
    MOVD    $4, R14
dot2_scalar_inner:
    AND     $0x03, R10, R12
    AND     $0x03, R11, R13
    MUL     R12, R13, R12
    ADD     R12, R3
    LSR     $2, R10, R10
    LSR     $2, R11, R11
    SUB     $1, R14
    CBNZ    R14, dot2_scalar_inner
    
    SUB     $1, R2
    B       dot2_tail

dot2_done:
    MOVW    R3, ret+24(FP)
    RET

// func euclideanInt16NEONKernel(a, b []int16) float32
