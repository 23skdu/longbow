//go:build arm64
#include "textflag.h"

#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_V(m, n, d) WORD $(0x4ea0d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMUL_V(m, n, d) WORD $(0x6e20dc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLA_V(m, n, d) WORD $(0x4e20cc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFCVTZS_V(n, d)  WORD $(0x4e21b800 | ((n) << 5) | (d))
#define VSCVTF_V(n, d)   WORD $(0x4e21d800 | ((n) << 5) | (d))
#define VFMAX_V(m, n, d) WORD $(0x4e20f400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMIN_V(m, n, d) WORD $(0x4e21f400 | ((m) << 16) | ((n) << 5) | (d))

// Constants
DATA tq_pi_arm<>+0x00(SB)/4, $3.14159265
DATA tq_inv2pi_arm<>+0x00(SB)/4, $0.15915494
DATA tq_half_arm<>+0x00(SB)/4, $0.5
DATA tq_max8_arm<>+0x00(SB)/4, $255.0
DATA tq_max4_arm<>+0x00(SB)/4, $15.0
DATA tq_max2_arm<>+0x00(SB)/4, $3.0

GLOBL tq_pi_arm<>(SB), RODATA, $4
GLOBL tq_inv2pi_arm<>(SB), RODATA, $4
GLOBL tq_half_arm<>(SB), RODATA, $4
GLOBL tq_max8_arm<>(SB), RODATA, $4
GLOBL tq_max4_arm<>(SB), RODATA, $4
GLOBL tq_max2_arm<>(SB), RODATA, $4

// func packTQ8NEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ8NEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    FMOVS   tq_pi_arm<>(SB), F0
    VDUP    V0.S[0], V0.S4 // V0 = PI
    FMOVS   tq_inv2pi_arm<>(SB), F1
    VDUP    V1.S[0], V1.S4 // V1 = INV_2PI
    FMOVS   tq_max8_arm<>(SB), F2
    VDUP    V2.S[0], V2.S4 // V2 = 255.0
    FMOVS   tq_half_arm<>(SB), F3
    VDUP    V3.S[0], V3.S4 // V3 = 0.5
    FMOVS   $1.0, F4
    VDUP    V4.S[0], V4.S4 // V4 = 1.0

    VEOR    V5.B16, V5.B16, V5.B16 // V5 = 0.0

loop_pack8:
    CMP     $4, R2
    BLT     tail_pack8
    
    VLD1.P  16(R0), [V6.S4]
    
    VFADD_V(0, 6, 6) // + PI
    VFMUL_V(1, 6, 6) // * INV_2PI
    
    // Clamp to [0, 1]
    VFMAX_V(5, 6, 6)
    VFMIN_V(4, 6, 6)
    
    VFMUL_V(2, 6, 6) // * 255.0
    VFADD_V(3, 6, 6) // + 0.5
    
    // Convert to int32
    VFCVTZS_V(6, 6)
    
    VMOV    V6.S[0], R3
    MOVB    R3, (R1)
    VMOV    V6.S[1], R3
    MOVB    R3, 1(R1)
    VMOV    V6.S[2], R3
    MOVB    R3, 2(R1)
    VMOV    V6.S[3], R3
    MOVB    R3, 3(R1)
    
    ADD     $4, R1
    SUB     $4, R2
    B       loop_pack8

tail_pack8:
    CBZ     R2, done_pack8
    
    FMOVS.P 4(R0), F6
    FADDS   F0, F6, F6
    FMULS   F1, F6, F6
    
    FMAXS   F5, F6, F6
    FMINS   F4, F6, F6
    
    FMULS   F2, F6, F6
    FADDS   F3, F6, F6
    
    FCVTZSS F6, R3
    MOVB    R3, (R1)
    ADD     $1, R1
    
    SUB     $1, R2
    B       tail_pack8

done_pack8:
    RET

// func unpackTQ8NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ8NEONKernel(SB), NOSPLIT, $0-32
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   scale+24(FP), F0
    VDUP    V0.S[0], V0.S4 // V0 = scale
    FMOVS   bias+28(FP), F1
    VDUP    V1.S[0], V1.S4 // V1 = bias

loop_unpack8:
    CMP     $4, R2
    BLT     tail_unpack8
    
    MOVBU   (R0), R3
    VMOV    R3, V4.B[0]
    MOVBU   1(R0), R3
    VMOV    R3, V4.B[1]
    MOVBU   2(R0), R3
    VMOV    R3, V4.B[2]
    MOVBU   3(R0), R3
    VMOV    R3, V4.B[3]
    
    VUSHLL  $0, V4.B8, V5.H8
    VUSHLL  $0, V5.H4, V4.S4
    
    VSCVTF_V(4, 4)
    VFMUL_V(0, 4, 4)
    VFADD_V(1, 4, 4)
    
    VST1.P  [V4.S4], 16(R1)
    
    ADD     $4, R0
    SUB     $4, R2
    B       loop_unpack8

tail_unpack8:
    CBZ     R2, done_unpack8
    MOVBU   (R0), R3
    SCVTFS  R3, F2
    FMULS   F0, F2, F2
    FADDS   F1, F2, F2
    FMOVS   F2, (R1)
    ADD     $4, R1
    ADD     $1, R0
    SUB     $1, R2
    B       tail_unpack8

done_unpack8:
    RET

// func packTQ4NEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ4NEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    FMOVS   tq_pi_arm<>(SB), F0
    VDUP    V0.S[0], V0.S4
    FMOVS   tq_inv2pi_arm<>(SB), F1
    VDUP    V1.S[0], V1.S4
    FMOVS   tq_max4_arm<>(SB), F2
    VDUP    V2.S[0], V2.S4
    FMOVS   tq_half_arm<>(SB), F3
    VDUP    V3.S[0], V3.S4
    FMOVS   $1.0, F4
    VDUP    V4.S[0], V4.S4
    VEOR    V5.B16, V5.B16, V5.B16

loop_pack4:
    CMP     $8, R2
    BLT     tail_pack4
    
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R0), [V7.S4]
    
    VFADD_V(0, 6, 6)
    VFMUL_V(1, 6, 6)
    VFMAX_V(5, 6, 6)
    VFMIN_V(4, 6, 6)
    VFMUL_V(2, 6, 6)
    VFADD_V(3, 6, 6)
    VFCVTZS_V(6, 6)
    
    VFADD_V(0, 7, 7)
    VFMUL_V(1, 7, 7)
    VFMAX_V(5, 7, 7)
    VFMIN_V(4, 7, 7)
    VFMUL_V(2, 7, 7)
    VFADD_V(3, 7, 7)
    VFCVTZS_V(7, 7)
    
    VMOV    V6.S[0], R4
    VMOV    V6.S[1], R5
    LSL     $4, R5, R7
    ORR     R7, R4, R6
    MOVB    R6, (R1)
    
    VMOV    V6.S[2], R4
    VMOV    V6.S[3], R5
    LSL     $4, R5, R7
    ORR     R7, R4, R6
    MOVB    R6, 1(R1)
    
    VMOV    V7.S[0], R4
    VMOV    V7.S[1], R5
    LSL     $4, R5, R7
    ORR     R7, R4, R6
    MOVB    R6, 2(R1)
    
    VMOV    V7.S[2], R4
    VMOV    V7.S[3], R5
    LSL     $4, R5, R7
    ORR     R7, R4, R6
    MOVB    R6, 3(R1)
    
    ADD     $4, R1
    SUB     $8, R2
    B       loop_pack4

tail_pack4:
    CBZ     R2, done_pack4
    FMOVS.P 4(R0), F6
    FADDS   F0, F6, F6
    FMULS   F1, F6, F6
    FMAXS   F5, F6, F6
    FMINS   F4, F6, F6
    FMULS   F2, F6, F6
    FADDS   F3, F6, F6
    FCVTZSS F6, R4
    
    SUB     $1, R2
    CBZ     R2, last_e0
    
    FMOVS.P 4(R0), F6
    FADDS   F0, F6, F6
    FMULS   F1, F6, F6
    FMAXS   F5, F6, F6
    FMINS   F4, F6, F6
    FMULS   F2, F6, F6
    FADDS   F3, F6, F6
    FCVTZSS F6, R5
    
    LSL     $4, R5, R7
    ORR     R7, R4, R6
    MOVB    R6, (R1)
    ADD     $1, R1
    SUB     $1, R2
    B       tail_pack4

last_e0:
    MOVB    R4, (R1)
    ADD     $1, R1
    RET

done_pack4:
    RET

// func unpackTQ4NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ4NEONKernel(SB), NOSPLIT, $0-32
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   scale+24(FP), F0
    VDUP    V0.S[0], V0.S4
    FMOVS   bias+28(FP), F1
    VDUP    V1.S[0], V1.S4

    MOVD    $0x0F, R4
    VMOV    R4, V2.S[0]
    VDUP    V2.S[0], V2.S4 // V2 = 0x0F mask

loop_unpack4:
    CMP     $8, R2
    BLT     tail_unpack4
    
    MOVBU   (R0), R3
    MOVBU   1(R0), R4
    MOVBU   2(R0), R5
    MOVBU   3(R0), R6
    
    // Byte 0 -> V4.B[0], V4.B[1]
    AND     $0x0F, R3, R7
    VMOV    R7, V4.B[0]
    LSR     $4, R3, R7
    VMOV    R7, V4.B[1]
    
    // Byte 1 -> V4.B[2], V4.B[3]
    AND     $0x0F, R4, R7
    VMOV    R7, V4.B[2]
    LSR     $4, R4, R7
    VMOV    R7, V4.B[3]
    
    VUSHLL  $0, V4.B8, V8.H8
    VUSHLL  $0, V8.H4, V9.S4 // [e3, e2, e1, e0] as int32
    
    VSCVTF_V(9, 9)
    VFMUL_V(0, 9, 9)
    VFADD_V(1, 9, 9)
    VST1.P  [V9.S4], 16(R1)
    
    // Byte 2 -> V5.B[0], V5.B[1]
    AND     $0x0F, R5, R7
    VMOV    R7, V5.B[0]
    LSR     $4, R5, R7
    VMOV    R7, V5.B[1]
    
    // Byte 3 -> V5.B[2], V5.B[3]
    AND     $0x0F, R6, R7
    VMOV    R7, V5.B[2]
    LSR     $4, R6, R7
    VMOV    R7, V5.B[3]
    
    VUSHLL  $0, V5.B8, V8.H8
    VUSHLL  $0, V8.H4, V9.S4 // [e7, e6, e5, e4] as int32
    
    VSCVTF_V(9, 9)
    VFMUL_V(0, 9, 9)
    VFADD_V(1, 9, 9)
    VST1.P  [V9.S4], 16(R1)
    
    ADD     $4, R0
    SUB     $8, R2
    B       loop_unpack4

tail_unpack4:
    CBZ     R2, done_unpack4
    MOVBU   (R0), R3
    
    AND     $0x0F, R3, R4
    SCVTFS  R4, F2
    FMULS   F0, F2, F2
    FADDS   F1, F2, F2
    FMOVS   F2, (R1)
    ADD     $4, R1
    SUB     $1, R2
    CBZ     R2, unpack4_next_byte
    
    LSR     $4, R3, R4
    SCVTFS  R4, F2
    FMULS   F0, F2, F2
    FADDS   F1, F2, F2
    FMOVS   F2, (R1)
    ADD     $4, R1
    SUB     $1, R2

unpack4_next_byte:
    ADD     $1, R0
    B       tail_unpack4

done_unpack4:
    RET

// func packTQ2NEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2NEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    FMOVS   tq_pi_arm<>(SB), F0
    VDUP    V0.S[0], V0.S4
    FMOVS   tq_inv2pi_arm<>(SB), F1
    VDUP    V1.S[0], V1.S4
    FMOVS   tq_max2_arm<>(SB), F2
    VDUP    V2.S[0], V2.S4
    FMOVS   tq_half_arm<>(SB), F3
    VDUP    V3.S[0], V3.S4
    FMOVS   $1.0, F4
    VDUP    V4.S[0], V4.S4
    VEOR    V5.B16, V5.B16, V5.B16

loop_pack2:
    CMP     $4, R2
    BLT     tail_pack2
    
    VLD1.P  16(R0), [V6.S4]
    
    VFADD_V(0, 6, 6)
    VFMUL_V(1, 6, 6)
    VFMAX_V(5, 6, 6)
    VFMIN_V(4, 6, 6)
    VFMUL_V(2, 6, 6)
    VFADD_V(3, 6, 6)
    VFCVTZS_V(6, 6)
    
    VMOV    V6.S[0], R3
    VMOV    V6.S[1], R4
    VMOV    V6.S[2], R5
    VMOV    V6.S[3], R6
    
    AND     $0x03, R3, R3
    AND     $0x03, R4, R4
    AND     $0x03, R5, R5
    AND     $0x03, R6, R6
    
    LSL     $2, R4, R7
    ORR     R7, R3, R3
    LSL     $4, R5, R7
    ORR     R7, R3, R3
    LSL     $6, R6, R7
    ORR     R7, R3, R3
    
    MOVB    R3, (R1)
    ADD     $1, R1
    SUB     $4, R2
    B       loop_pack2

tail_pack2:
    CBZ     R2, done_pack2
    // Simplified tail: process up to 3 remaining elements
    MOVBU   $0, R3 // Accumulator for byte
    MOVBU   $0, R8 // Bit shift
    
tail_pack2_loop:
    FMOVS.P 4(R0), F6
    FADDS   F0, F6, F6
    FMULS   F1, F6, F6
    FMAXS   F5, F6, F6
    FMINS   F4, F6, F6
    FMULS   F2, F6, F6
    FADDS   F3, F6, F6
    FCVTZSS F6, R4
    AND     $0x03, R4, R4
    
    LSL     R8, R4, R7
    ORR     R7, R3, R3
    ADD     $2, R8
    
    SUB     $1, R2
    CBZ     R2, tail_pack2_flush
    CMP     $8, R8
    BLT     tail_pack2_loop

tail_pack2_flush:
    MOVB    R3, (R1)
    ADD     $1, R1
    CBZ     R2, done_pack2
    B       tail_pack2

done_pack2:
    RET

// func unpackTQ2NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ2NEONKernel(SB), NOSPLIT, $0-32
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2
    FMOVS   scale+24(FP), F0
    VDUP    V0.S[0], V0.S4
    FMOVS   bias+28(FP), F1
    VDUP    V1.S[0], V1.S4

loop_unpack2:
    CMP     $4, R2
    BLT     tail_unpack2
    
    MOVBU   (R0), R3
    
    AND     $0x03, R3, R4
    VMOV    R4, V4.B[0]
    LSR     $2, R3, R4
    AND     $0x03, R4, R5
    VMOV    R5, V4.B[1]
    LSR     $4, R3, R4
    AND     $0x03, R4, R5
    VMOV    R5, V4.B[2]
    LSR     $6, R3, R4
    VMOV    R4, V4.B[3]
    
    VUSHLL  $0, V4.B8, V8.H8
    VUSHLL  $0, V8.H4, V9.S4
    
    VSCVTF_V(9, 9)
    VFMUL_V(0, 9, 9)
    VFADD_V(1, 9, 9)
    VST1.P  [V9.S4], 16(R1)
    
    ADD     $1, R0
    SUB     $4, R2
    B       loop_unpack2

tail_unpack2:
    CBZ     R2, done_unpack2
    MOVBU   (R0), R3
    MOVBU   $0, R8 // Bit shift

tail_unpack2_loop:
    LSR     R8, R3, R4
    AND     $0x03, R4, R4
    SCVTFS  R4, F2
    FMULS   F0, F2, F2
    FADDS   F1, F2, F2
    FMOVS   F2, (R1)
    ADD     $4, R1
    ADD     $2, R8
    SUB     $1, R2
    CBZ     R2, done_unpack2
    CMP     $8, R8
    BLT     tail_unpack2_loop
    
    ADD     $1, R0
    B       tail_unpack2

done_unpack2:
    RET
