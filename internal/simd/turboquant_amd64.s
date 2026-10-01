//go:build amd64
#include "textflag.h"

// Mask for extracting 2 bits
DATA mask2bits<>+0x00(SB)/4, $0x03
DATA mask2bits<>+0x04(SB)/4, $0x03
DATA mask2bits<>+0x08(SB)/4, $0x03
DATA mask2bits<>+0x0c(SB)/4, $0x03
DATA mask2bits<>+0x10(SB)/4, $0x03
DATA mask2bits<>+0x14(SB)/4, $0x03
DATA mask2bits<>+0x18(SB)/4, $0x03
DATA mask2bits<>+0x1c(SB)/4, $0x03
GLOBL mask2bits<>(SB), RODATA, $32
 
// VBMI control masks for 2-bit unpacking (8 elements per qword lane)
DATA vbmi_tq2_ctrl<>+0x00(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x08(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x10(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x18(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x20(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x28(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x30(SB)/8, $0x0604020006040200
DATA vbmi_tq2_ctrl<>+0x38(SB)/8, $0x0604020006040200
GLOBL vbmi_tq2_ctrl<>(SB), RODATA, $64

DATA mask2bits_avx512<>+0x00(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x08(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x10(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x18(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x20(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x28(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x30(SB)/8, $0x0303030303030303
DATA mask2bits_avx512<>+0x38(SB)/8, $0x0303030303030303
GLOBL mask2bits_avx512<>(SB), RODATA, $64

DATA tq_pi<>+0x00(SB)/4, $3.14159265
DATA tq_inv2pi<>+0x00(SB)/4, $0.15915494
DATA tq_half<>+0x00(SB)/4, $0.5
DATA tq_max8<>+0x00(SB)/4, $255.0
DATA tq_max4<>+0x00(SB)/4, $15.0
DATA tq_max2<>+0x00(SB)/4, $3.0
DATA tq_one<>+0x00(SB)/4, $1.0

GLOBL tq_pi<>(SB), RODATA, $4
GLOBL tq_inv2pi<>(SB), RODATA, $4
GLOBL tq_half<>(SB), RODATA, $4
GLOBL tq_max8<>(SB), RODATA, $4
GLOBL tq_max4<>(SB), RODATA, $4
GLOBL tq_max2<>(SB), RODATA, $4
GLOBL tq_one<>(SB), RODATA, $4

// Control masks for VPMULTISHIFTQB packing (TQ2)
// Each qword lane has 8 bytes. We want to extract 2 bits from each and pack into 2 bytes.
// pack_ctrl_0: extracts bits into position 0 (e0 and e4)
DATA vbmi_tq2_pack_ctrl_0<>+0x00(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x08(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x10(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x18(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x20(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x28(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x30(SB)/8, $0x0000000000002000
DATA vbmi_tq2_pack_ctrl_0<>+0x38(SB)/8, $0x0000000000002000
GLOBL vbmi_tq2_pack_ctrl_0<>(SB), RODATA, $64

// pack_ctrl_1: extracts bits into position 2 (e1 and e5)
DATA vbmi_tq2_pack_ctrl_1<>+0x00(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x08(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x10(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x18(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x20(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x28(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x30(SB)/8, $0x0000000000002606
DATA vbmi_tq2_pack_ctrl_1<>+0x38(SB)/8, $0x0000000000002606
GLOBL vbmi_tq2_pack_ctrl_1<>(SB), RODATA, $64

// pack_ctrl_2: extracts bits into position 4 (e2 and e6)
DATA vbmi_tq2_pack_ctrl_2<>+0x00(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x08(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x10(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x18(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x20(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x28(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x30(SB)/8, $0x0000000000002c0c
DATA vbmi_tq2_pack_ctrl_2<>+0x38(SB)/8, $0x0000000000002c0c
GLOBL vbmi_tq2_pack_ctrl_2<>(SB), RODATA, $64

// pack_ctrl_3: extracts bits into position 6 (e3 and e7)
DATA vbmi_tq2_pack_ctrl_3<>+0x00(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x08(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x10(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x18(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x20(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x28(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x30(SB)/8, $0x0000000000003212
DATA vbmi_tq2_pack_ctrl_3<>+0x38(SB)/8, $0x0000000000003212
GLOBL vbmi_tq2_pack_ctrl_3<>(SB), RODATA, $64

// collect_mask: selects byte 0 and 1 from each qword
DATA vbmi_tq2_collect_mask<>+0x00(SB)/8, $0x0b0a090803020100
DATA vbmi_tq2_collect_mask<>+0x08(SB)/8, $0x1b1a191813121110
DATA vbmi_tq2_collect_mask<>+0x10(SB)/8, $0x2b2a292823222120
DATA vbmi_tq2_collect_mask<>+0x18(SB)/8, $0x3b3a393833323130
DATA vbmi_tq2_collect_mask<>+0x20(SB)/8, $0x0000000000000000
DATA vbmi_tq2_collect_mask<>+0x28(SB)/8, $0x0000000000000000
DATA vbmi_tq2_collect_mask<>+0x30(SB)/8, $0x0000000000000000
DATA vbmi_tq2_collect_mask<>+0x38(SB)/8, $0x0000000000000000
GLOBL vbmi_tq2_collect_mask<>(SB), RODATA, $64

DATA mask_pos0<>+0x00(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x08(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x10(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x18(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x20(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x28(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x30(SB)/8, $0x0000000000000303
DATA mask_pos0<>+0x38(SB)/8, $0x0000000000000303
GLOBL mask_pos0<>(SB), RODATA, $64

DATA mask_pos2<>+0x00(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x08(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x10(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x18(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x20(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x28(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x30(SB)/8, $0x0000000000000c0c
DATA mask_pos2<>+0x38(SB)/8, $0x0000000000000c0c
GLOBL mask_pos2<>(SB), RODATA, $64

DATA mask_pos4<>+0x00(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x08(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x10(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x18(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x20(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x28(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x30(SB)/8, $0x0000000000003030
DATA mask_pos4<>+0x38(SB)/8, $0x0000000000003030
GLOBL mask_pos4<>(SB), RODATA, $64

DATA mask_pos6<>+0x00(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x08(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x10(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x18(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x20(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x28(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x30(SB)/8, $0x000000000000c0c0
DATA mask_pos6<>+0x38(SB)/8, $0x000000000000c0c0
GLOBL mask_pos6<>(SB), RODATA, $64

DATA pack2_weights_0<>+0x00(SB)/8, $0x0401040104010401
DATA pack2_weights_0<>+0x08(SB)/8, $0x0401040104010401
GLOBL pack2_weights_0<>(SB), RODATA, $16

DATA pack2_weights_1<>+0x00(SB)/8, $0x0010000100100001
DATA pack2_weights_1<>+0x08(SB)/8, $0x0010000100100001
GLOBL pack2_weights_1<>(SB), RODATA, $16

// tq4_maddubs weights the adjacent byte pairs [1,16] so VPMADDUBSW sums
// c[2k] + 16*c[2k+1], which is one 4-bit output byte with the low nibble
// holding the earlier element.
DATA tq4_maddubs<>+0x00(SB)/8, $0x1001100110011001
DATA tq4_maddubs<>+0x08(SB)/8, $0x1001100110011001
GLOBL tq4_maddubs<>(SB), RODATA, $16

// tq4_evenbytes keeps byte 0 of each word for VPSHUFB.
DATA tq4_evenbytes<>+0x00(SB)/8, $0x0E0C0A0806040200
DATA tq4_evenbytes<>+0x08(SB)/8, $0xFFFFFFFFFFFFFFFF
GLOBL tq4_evenbytes<>(SB), RODATA, $16

// tq2_maddubs repeats the field weights [1,4,16,64], so each
// VPMADDUBSW destination word is one complete 2-bit output byte.
DATA tq2_maddubs<>+0x00(SB)/8, $0x4010040140100401
DATA tq2_maddubs<>+0x08(SB)/8, $0x4010040140100401
GLOBL tq2_maddubs<>(SB), RODATA, $16

// tq2_maddwd adds each pair of half-bytes with unit weights.
DATA tq2_maddwd<>+0x00(SB)/8, $0x0001000100010001
DATA tq2_maddwd<>+0x08(SB)/8, $0x0001000100010001
GLOBL tq2_maddwd<>(SB), RODATA, $16

// tq2_evenbytes keeps byte 0 of each dword for VPSHUFB.
DATA tq2_evenbytes<>+0x00(SB)/8, $0x000000000C080400
DATA tq2_evenbytes<>+0x08(SB)/8, $0xFFFFFFFFFFFFFFFF
GLOBL tq2_evenbytes<>(SB), RODATA, $16

// func unpackTQ2AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ2AVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    VMOVSS  scale+24(FP), X0
    VMOVSS  bias+28(FP), X1
    
    VBROADCASTSS X0, Y0 // Y0 = scale
    VBROADCASTSS X1, Y1 // Y1 = bias
    VMOVDQU mask2bits<>(SB), Y2 // Y2 = 0x03 mask
    
loop_tq2:
    CMPQ    CX, $32
    JL      tail_tq2
    
    // Load 8 bytes (32 elements)
    MOVQ    (SI), AX
    VMOVQ   AX, X3
    VPMOVZXBD X3, Y4    // Y4 = [b7, b6, b5, b4, b3, b2, b1, b0] as int32s
    
    // Unpack e0 (bits 0-1)
    VPSRLD  $0, Y4, Y5
    VPAND   Y2, Y5, Y5
    VCVTDQ2PS Y5, Y5
    VFMADD213PS Y1, Y0, Y5 // Y5 = Y5 * scale + bias
    VMOVDQU Y5, (DI)
    
    // Unpack e1 (bits 2-3)
    VPSRLD  $2, Y4, Y6
    VPAND   Y2, Y6, Y6
    VCVTDQ2PS Y6, Y6
    VFMADD213PS Y1, Y0, Y6 // Y6 = Y6 * scale + bias
    VMOVDQU Y6, 32(DI)
    
    // Unpack e2 (bits 4-5)
    VPSRLD  $4, Y4, Y7
    VPAND   Y2, Y7, Y7
    VCVTDQ2PS Y7, Y7
    VFMADD213PS Y1, Y0, Y7 // Y7 = Y7 * scale + bias
    VMOVDQU Y7, 64(DI)
    
    // Unpack e3 (bits 6-7)
    VPSRLD  $6, Y4, Y8
    VPAND   Y2, Y8, Y8
    VCVTDQ2PS Y8, Y8
    VFMADD213PS Y1, Y0, Y8 // Y8 = Y8 * scale + bias
    VMOVDQU Y8, 96(DI)
    
    ADDQ    $8, SI
    ADDQ    $128, DI
    SUBQ    $32, CX
    JMP     loop_tq2
    
tail_tq2:
    TESTQ   CX, CX
    JZ      done_tq2
    
    // Scalar tail
    MOVB    (SI), AL
    MOVQ    $4, BX
    CMPQ    CX, BX
    CMOVQGT CX, BX // min(4, CX)
    
tail_inner:
    MOVB    AL, BL
    ANDB    $0x03, BL
    MOVBQZX BL, BX
    VMOVQ   BX, X3
    VCVTDQ2PS X3, X3
    VFMADD213SS X1, X0, X3
    VMOVSS  X3, (DI)
    
    SHRB    $2, AL
    ADDQ    $4, DI
    DECQ    CX
    JZ      done_tq2
    DECQ    BX
    JNZ     tail_inner
    
    INCQ    SI
    JMP     tail_tq2
    
done_tq2:
    VZEROUPPER
    RET

// func unpackTQ4AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
// Logic: 2 elements per byte, each 4 bits.
// Load 16 bytes (32 elements) -> Expand to 16 int32s (2 YMMs)
// Wait! 16 bytes -> 16 int32s (2 YMMs).
// Byte [e1:e0]
TEXT ·unpackTQ4AVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    VMOVSS  scale+24(FP), X0
    VMOVSS  bias+28(FP), X1
    
    VBROADCASTSS X0, Y0
    VBROADCASTSS X1, Y1
    VPXOR   Y2, Y2, Y2
    MOVQ    $0x0F, AX
    VMOVQ   AX, X3
    VPBROADCASTD X3, Y3 // 0x0F mask
   loop_tq4:
    CMPQ    CX, $32
    JL      tail_tq4
    
    // Correct way to load 16 bytes and expand to 2 YMMs
    VMOVDQU (SI), X4    // Load 16 bytes
    VPMOVZXBD X4, Y5    // Expansion of bytes 0-7
    // To get 8-15, we need to shift X4
    VPSRLDQ $8, X4, X6
    VPMOVZXBD X6, Y7    // Expansion of bytes 8-15
    
    // Y5 and Y7 contain 8 bytes each, expanded to int32.
    // Each int32 has [e1:e0].
    
    // Unpack e0
    VPAND   Y3, Y5, Y8
    VCVTDQ2PS Y8, Y8
    VFMADD213PS Y1, Y0, Y8
    VMOVDQU Y8, (DI)
    
    VPAND   Y3, Y7, Y9
    VCVTDQ2PS Y9, Y9
    VFMADD213PS Y1, Y0, Y9
    VMOVDQU Y9, 32(DI)
    
    // Unpack e1
    VPSRLD  $4, Y5, Y8
    VPAND   Y3, Y8, Y8
    VCVTDQ2PS Y8, Y8
    VFMADD213PS Y1, Y0, Y8
    VMOVDQU Y8, 64(DI)
    
    VPSRLD  $4, Y7, Y9
    VPAND   Y3, Y9, Y9
    VCVTDQ2PS Y9, Y9
    VFMADD213PS Y1, Y0, Y9
    VMOVDQU Y9, 96(DI)
    
    ADDQ    $16, SI
    ADDQ    $128, DI
    SUBQ    $32, CX
    JMP     loop_tq4

tail_tq4:
    // Simple scalar fallback for tail
    TESTQ   CX, CX
    JZ      done_tq4
    MOVB    (SI), AL
    
    // e0
    MOVBQZX AL, BX
    ANDB    $0x0F, BL
    VMOVQ   BX, X4
    VCVTDQ2PS X4, X4
    VFMADD213SS X1, X0, X4
    VMOVSS  X4, (DI)
    ADDQ    $4, DI
    DECQ    CX
    JZ      done_tq4
    
    // e1
    SHRB    $4, AL
    MOVBQZX AL, AX
    VMOVQ   AX, X4
    VCVTDQ2PS X4, X4
    VFMADD213SS X1, X0, X4
    VMOVSS  X4, (DI)
    ADDQ    $4, DI
    DECQ    CX
    
    INCQ    SI
    JMP     tail_tq4
    
done_tq4:
    VZEROUPPER
    RET

// func unpackTQ8AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
// 1 element per byte. Load 32 bytes -> 4 YMMs.
TEXT ·unpackTQ8AVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    VMOVSS  scale+24(FP), X0
    VMOVSS  bias+28(FP), X1
    
    VBROADCASTSS X0, Y0
    VBROADCASTSS X1, Y1
    
loop_tq8:
    CMPQ    CX, $8
    JL      tail_tq8
    
    MOVQ    (SI), AX
    VMOVQ   AX, X2
    VPMOVZXBD X2, Y3
    VCVTDQ2PS Y3, Y3
    VFMADD213PS Y1, Y0, Y3
    VMOVDQU Y3, (DI)
    
    ADDQ    $8, SI
    ADDQ    $32, DI
    SUBQ    $8, CX
    JMP     loop_tq8
    
tail_tq8:
    TESTQ   CX, CX
    JZ      done_tq8
    MOVBQZX (SI), AX
    VMOVQ   AX, X2
    VCVTDQ2PS X2, X2
    VFMADD213SS X1, X0, X2
    VMOVSS  X2, (DI)
    INCQ    SI
    ADDQ    $4, DI
    DECQ    CX
    JMP     tail_tq8
    
done_tq8:
    VZEROUPPER
    RET

// func packTQ8AVX2Kernel(src, dst unsafe.Pointer, n int)
//
// q = byte(clamp((v + PI) * (1/2PI), 0, 1) * 255 + 0.5), one byte per element.
//
// Two register-aliasing traps are worth spelling out, because both silently
// corrupt the codes rather than faulting:
//
//   - The constants are broadcast straight from memory. The two-operand
//     "VMOVSS m32, Xn" form is assembled with VEX.L=1, i.e. as the 256-bit
//     variant, which zeroes bits [255:32] of the destination YMM. Chaining
//     "load into X0, broadcast X0 into Yk" therefore destroys the broadcast
//     the previous instruction installed, and the quantizer loses its +PI
//     step on every lane but the first.
//   - The VEXTRACTI128 scratch is X5, never X0..X6: Xk is the low half of Yk,
//     so extracting into X6 would replace the upper half of the clamp
//     constant Y6 and collapse every later element to code 0.
//
// VROUNDPS makes the conversion an explicit floor so the kernel is
// bit-identical to PackTQ8Generic's byte(x + 0.5); VCVTPS2DQ alone would round
// to nearest and skew almost every code by +1.
TEXT ·packTQ8AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX

    VBROADCASTSS tq_pi<>(SB), Y0
    VBROADCASTSS tq_inv2pi<>(SB), Y1
    VBROADCASTSS tq_max8<>(SB), Y2
    VBROADCASTSS tq_half<>(SB), Y3
    VPXOR   Y4, Y4, Y4
    VBROADCASTSS tq_one<>(SB), Y6

loop_pack8:
    CMPQ    CX, $16
    JL      tail_pack8_small

    // 16 elements (two 8-lane groups) -> 16 bytes
    VMOVDQU (SI), Y7
    VMOVDQU 32(SI), Y8

    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VROUNDPS $1, Y7, Y7
    VCVTPS2DQ Y7, Y7

    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VROUNDPS $1, Y8, Y8
    VCVTPS2DQ Y8, Y8

    // Narrow 8 codes to 8 bytes, in element order: the extracted upper lane
    // feeds the high half of VPACKUSDW, and VPACKUSWB then reads the low 4
    // codes of each half.
    VEXTRACTI128 $1, Y7, X5
    VPACKUSDW  Y5, Y7, Y7
    VPACKUSWB  X7, X7, X7
    VMOVQ      X7, (DI)

    VEXTRACTI128 $1, Y8, X5
    VPACKUSDW  Y5, Y8, Y8
    VPACKUSWB  X8, X8, X8
    VMOVQ      X8, 8(DI)

    ADDQ    $64, SI
    ADDQ    $16, DI
    SUBQ    $16, CX
    JMP     loop_pack8

tail_pack8_small:
    CMPQ    CX, $8
    JL      tail_pack8
    VMOVDQU (SI), Y7
    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VROUNDPS $1, Y7, Y7
    VCVTPS2DQ Y7, Y7
    VEXTRACTI128 $1, Y7, X5
    VPACKUSDW  Y5, Y7, Y7
    VPACKUSWB  X7, X7, X7
    VMOVQ      X7, (DI)
    ADDQ    $32, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     tail_pack8_small

tail_pack8:
    TESTQ   CX, CX
    JZ      done_pack8
    // Y0..Y6 are dead from here, so the constants can be reloaded as scalars.
    VMOVSS  tq_pi<>(SB), X0
    VMOVSS  tq_inv2pi<>(SB), X1
    VMOVSS  tq_max8<>(SB), X2
    VMOVSS  tq_half<>(SB), X3
    VPXOR   X4, X4, X4
    VMOVSS  tq_one<>(SB), X6
loop_tail_pack8:
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VROUNDPS $1, X5, X5 // VEX.128 VROUNDPS: lane 0 is all the tail needs
    VCVTSS2SI X5, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JNZ     loop_tail_pack8

done_pack8:
    VZEROUPPER
    RET

// func unpackTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ2AVX512VBMIKernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    VMOVSS  scale+24(FP), X0
    VMOVSS  bias+28(FP), X1
    
    VBROADCASTSS X0, Z0
    VBROADCASTSS X1, Z1
    VMOVDQU64 vbmi_tq2_ctrl<>(SB), Z2
    VMOVDQU64 mask2bits_avx512<>(SB), Z3
    
loop_tq2_vbmi:
    CMPQ    CX, $16
    JL      tail_tq2_vbmi
    
    // Load 4 bytes (16 elements)
    MOVL    (SI), AX
    VMOVQ   AX, X4
    VPMOVZXBQ X4, Z4 // 4 bytes -> 4 qwords
    VPMULTISHIFTQB Z4, Z2, Z5
    VPANDQ  Z3, Z5, Z5 // Mask to 2 bits
    
    // Z5 has 4 lanes, each has 8 elements as bytes.
    // We need 16 float32s (one ZMM).
    VPMOVZXBD X5, Z6 // First 16 bytes to Z6
    VCVTDQ2PS Z6, Z6
    VFMADD213PS Z1, Z0, Z6
    VMOVDQU64 Z6, (DI)
    
    ADDQ    $4, SI
    ADDQ    $64, DI
    SUBQ    $16, CX
    JMP     loop_tq2_vbmi
    
tail_tq2_vbmi:
    TESTQ   CX, CX
    JZ      done_tq2_vbmi
    // Fallback to scalar or AVX2 tail
    JMP     done_tq2_vbmi

done_tq2_vbmi:
    VZEROUPPER
    RET

// func packTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2AVX512VBMIKernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Z0 // PI
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Z1 // 1/2PI
    VMOVSS  tq_max2<>(SB), X0
    VBROADCASTSS X0, Z2 // 3.0
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Z3 // 0.5
    
    VPXORQ  Z4, Z4, Z4  // 0.0
    VMOVSS  $1.0, X5
    VBROADCASTSS X5, Z5 // 1.0
    
loop_pack2_vbmi:
    CMPQ    CX, $64
    JL      tail_pack2_vbmi
    
    // Load 64 floats
    VMOVDQU32 (SI), Z6
    VMOVDQU32 64(SI), Z7
    VMOVDQU32 128(SI), Z8
    VMOVDQU32 192(SI), Z9
    
    // Quantize 
    VADDPS  Z0, Z6, Z6; VMULPS  Z1, Z6, Z6; VMAXPS  Z4, Z6, Z6; VMINPS  Z5, Z6, Z6; VMULPS  Z2, Z6, Z6; VADDPS  Z3, Z6, Z6; VCVTPS2DQ Z6, Z6
    VADDPS  Z0, Z7, Z7; VMULPS  Z1, Z7, Z7; VMAXPS  Z4, Z7, Z7; VMINPS  Z5, Z7, Z7; VMULPS  Z2, Z7, Z7; VADDPS  Z3, Z7, Z7; VCVTPS2DQ Z7, Z7
    VADDPS  Z0, Z8, Z8; VMULPS  Z1, Z8, Z8; VMAXPS  Z4, Z8, Z8; VMINPS  Z5, Z8, Z8; VMULPS  Z2, Z8, Z8; VADDPS  Z3, Z8, Z8; VCVTPS2DQ Z8, Z8
    VADDPS  Z0, Z9, Z9; VMULPS  Z1, Z9, Z9; VMAXPS  Z4, Z9, Z9; VMINPS  Z5, Z9, Z9; VMULPS  Z2, Z9, Z9; VADDPS  Z3, Z9, Z9; VCVTPS2DQ Z9, Z9
    
    // Narrow to bytes
    VPMOVDB Z6, X6
    VPMOVDB Z7, X7
    VPMOVDB Z8, X8
    VPMOVDB Z9, X9
    
    // Combine 4 XMMs into 1 ZMM (64 bytes)
    VINSERTI32X4 $1, X7, Z6, Z6
    VINSERTI32X4 $2, X8, Z6, Z6
    VINSERTI32X4 $3, X9, Z6, Z6
    
    // Use VPMULTISHIFTQB to align bits.
    VMOVDQU64 vbmi_tq2_pack_ctrl_0<>(SB), Z10
    VMOVDQU64 vbmi_tq2_pack_ctrl_1<>(SB), Z11
    VMOVDQU64 vbmi_tq2_pack_ctrl_2<>(SB), Z12
    VMOVDQU64 vbmi_tq2_pack_ctrl_3<>(SB), Z13

    VPMULTISHIFTQB Z6, Z10, Z14
    VPMULTISHIFTQB Z6, Z11, Z15
    VPMULTISHIFTQB Z6, Z12, Z16
    VPMULTISHIFTQB Z6, Z13, Z17
    
    // Mask to positions
    VMOVDQU64 mask_pos0<>(SB), Z18
    VPANDQ  Z18, Z14, Z14
    VMOVDQU64 mask_pos2<>(SB), Z18
    VPANDQ  Z18, Z15, Z15
    VMOVDQU64 mask_pos4<>(SB), Z18
    VPANDQ  Z18, Z16, Z16
    VMOVDQU64 mask_pos6<>(SB), Z18
    VPANDQ  Z18, Z17, Z17
    
    // OR together
    VPTERNLOGD $0xFE, Z15, Z16, Z14 // Z14 = Z14 | Z15 | Z16
    VPORQ   Z17, Z14, Z14
    
    // Collect the 16 packed bytes using VPERMB
    VMOVDQU64 vbmi_tq2_collect_mask<>(SB), Z15
    VPERMB  Z14, Z15, Z14
    
    // Store 16 bytes
    VMOVDQU X14, (DI)
    
    ADDQ    $256, SI
    ADDQ    $16, DI
    SUBQ    $64, CX
    JMP     loop_pack2_vbmi

tail_pack2_vbmi:
    TESTQ   CX, CX
    JZ      done_pack2_vbmi
    // Fallback to AVX2 kernel for tail
    JMP     ·packTQ2AVX2Kernel(SB)

done_pack2_vbmi:
    VZEROUPPER
    RET
// func packTQ4AVX2Kernel(src, dst unsafe.Pointer, n int)
// func packTQ4AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ4AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX

    // Constants are broadcast straight from memory. Chaining
    // "VMOVSS tq_x<>(SB), X0" into "VBROADCASTSS X0, Y0" does not work: Go
    // assembles the two-operand VMOVSS m32, Xn with VEX.L=1, so the CPU
    // zeroes bits [255:32] of the destination YMM, and the next reload of X0
    // wipes lanes 4-7 of the vector that was just built.
    VBROADCASTSS tq_pi<>(SB), Y0
    VBROADCASTSS tq_inv2pi<>(SB), Y1
    VBROADCASTSS tq_max4<>(SB), Y2
    VBROADCASTSS tq_half<>(SB), Y3
    VPXOR   Y4, Y4, Y4
    VBROADCASTSS tq_one<>(SB), Y6

loop_pack4:
    CMPQ    CX, $16
    JL      tail_pack4

    VMOVDQU (SI), Y7
    VMOVDQU 32(SI), Y8

    // (src + pi) * inv2pi, clamped to [0,1], scaled to [0,max4], plus half,
    // then floored. The explicit VROUNDPS matters: VCVTPS2DQ rounds half to
    // even, while the scalar reference computes floor(norm*max + 0.5), which
    // skews nearly every code by one.
    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VROUNDPS $1, Y7, Y7
    VCVTPS2DQ Y7, Y7

    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VROUNDPS $1, Y8, Y8
    VCVTPS2DQ Y8, Y8

    // Narrow eight int32 codes to eight bytes in element order.
    // VEXTRACTI128 lands the upper four in X5, VPACKUSDW merges them below
    // the lower four, VPACKUSWB then narrows the eight words. X5 is the
    // scratch because X0-X3 and X6 still hold the live constants, and Xk is
    // the low half of Yk, so extracting into one of those would corrupt it.
    VEXTRACTI128 $1, Y7, X5
    VPACKUSDW  Y5, Y7, Y7
    VPACKUSWB  X7, X7, X7

    VEXTRACTI128 $1, Y8, X5
    VPACKUSDW  Y5, Y8, Y8
    VPACKUSWB  X8, X8, X8

    // Gather the sixteen codes into one vector as [c0..c15].
    MOVQ    X7, AX
    MOVQ    X8, BX
    MOVQ    AX, X9
    PINSRQ  $1, BX, X9

    // Two codes per output byte, low nibble first, matching the reference's
    // dst[i/2] = q1 | q2<<4. VPMADDUBSW multiplies adjacent unsigned bytes
    // by these weights and sums each pair, so weight 1 for the low nibble and
    // 16 for the high one assembles the byte directly.
    VMOVDQU tq4_maddubs<>(SB), X10
    VPMADDUBSW X10, X9, X9

    // Each word now holds one output byte in its low 8 bits; keep the even
    // byte of each word.
    VMOVDQU tq4_evenbytes<>(SB), X10
    VPSHUFB  X10, X9, X9
    VMOVQ    X9, (DI)

    ADDQ    $64, SI
    ADDQ    $8, DI
    SUBQ    $16, CX
    JMP     loop_pack4

tail_pack4:
    TESTQ   CX, CX
    JZ      done_pack4
    // Reload the scalars the tail needs from memory rather than reading them
    // out of the broadcast vectors.
    VMOVSS  tq_pi<>(SB), X0
    VMOVSS  tq_inv2pi<>(SB), X1
    VMOVSS  tq_max4<>(SB), X2
    VMOVSS  tq_half<>(SB), X3
    VPXOR   X4, X4, X4
    VMOVSS  tq_one<>(SB), X6

    XORL    R8, R8       // byte under construction
    MOVQ    $2, R13      // elements still wanted in that byte

tail_elem4:
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VROUNDPS $1, X5, X5
    VCVTSS2SI X5, R11
    ANDL    $0x0F, R11
    ADDQ    $4, SI

    DECQ    R13
    JNZ     tail_low4
    // Second element of the byte: high nibble.
    SHLL    $4, R11
    ORL     R11, R8
    MOVB    R8, (DI)
    INCQ    DI
    XORL    R8, R8
    MOVQ    $2, R13
    JMP     tail_next4

tail_low4:
    MOVL    R11, R8

tail_next4:
    DECQ    CX
    JNZ     tail_elem4

    // R13 == 1 means a low nibble was placed and never completed. The
    // reference leaves the matching high nibble zero in exactly the same way.
    CMPQ    R13, $1
    JNE     done_pack4
    MOVB    R8, (DI)
    INCQ    DI

done_pack4:
    VZEROUPPER
    RET

// func packTQ2AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX

    VBROADCASTSS tq_pi<>(SB), Y0
    VBROADCASTSS tq_inv2pi<>(SB), Y1
    VBROADCASTSS tq_max2<>(SB), Y2
    VBROADCASTSS tq_half<>(SB), Y3
    VPXOR   Y4, Y4, Y4
    VBROADCASTSS tq_one<>(SB), Y6

loop_pack2:
    CMPQ    CX, $16
    JL      tail_pack2

    VMOVDQU (SI), Y7
    VMOVDQU 32(SI), Y8

    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VROUNDPS $1, Y7, Y7
    VCVTPS2DQ Y7, Y7

    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VROUNDPS $1, Y8, Y8
    VCVTPS2DQ Y8, Y8

    VEXTRACTI128 $1, Y7, X5
    VPACKUSDW  Y5, Y7, Y7
    VPACKUSWB  X7, X7, X7

    VEXTRACTI128 $1, Y8, X5
    VPACKUSDW  Y5, Y8, Y8
    VPACKUSWB  X8, X8, X8

    MOVQ    X7, AX
    MOVQ    X8, BX
    MOVQ    AX, X9
    PINSRQ  $1, BX, X9

    // Four codes per output byte, field 0 in the low bits, matching the
    // reference's b |= q << (2*j). With the weight vector repeating
    // [1,4,16,64], each VPMADDUBSW destination word already holds a whole
    // output byte: word j = c[2j]*w[2j] + c[2j+1]*w[2j+1], so words 0 and 1
    // carry codes 0-3 and codes 2-3, words 2 and 3 carry codes 4-7, and so on.
    // The top is 3 + 12 + 48 + 192 = 255, which still fits the low byte.
    VMOVDQU tq2_maddubs<>(SB), X10
    VPMADDUBSW X10, X9, X9

    // Each word holds one half of an output byte: with the [1,4,16,64]
    // weights, word 0 is c[0] + 4*c[1] and word 1 is 16*c[2] + 64*c[3].
    // Adding the two with unit weights folds them into the single byte the
    // reference builds, so dst[0] = c[0] + 4*c[1] + 16*c[2] + 64*c[3].
    VMOVDQU tq2_maddwd<>(SB), X10
    VPMADDWD  X10, X9, X9

    // Keep byte 0 of each dword.
    VMOVDQU tq2_evenbytes<>(SB), X10
    VPSHUFB  X10, X9, X9
    VMOVD    X9, (DI)

    ADDQ    $64, SI
    ADDQ    $4, DI
    SUBQ    $16, CX
    JMP     loop_pack2

tail_pack2:
    TESTQ   CX, CX
    JZ      done_pack2
    VMOVSS  tq_pi<>(SB), X0
    VMOVSS  tq_inv2pi<>(SB), X1
    VMOVSS  tq_max2<>(SB), X2
    VMOVSS  tq_half<>(SB), X3
    VPXOR   X4, X4, X4
    VMOVSS  tq_one<>(SB), X6

    XORL    R8, R8       // byte under construction
    MOVQ    $4, R13      // elements still wanted in that byte

tail_elem2:
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VROUNDPS $1, X5, X5
    VCVTSS2SI X5, R11
    ANDL    $0x03, R11
    ADDQ    $4, SI

    // Place the code in field (4 - R13) of the byte under construction.
    CMPQ    R13, $4
    JE      tq2_or
    CMPQ    R13, $3
    JE      tq2_f1
    CMPQ    R13, $2
    JE      tq2_f2
    SHLL    $6, R11
    JMP     tq2_or
tq2_f2:
    SHLL    $4, R11
    JMP     tq2_or
tq2_f1:
    SHLL    $2, R11
tq2_or:
    ORL     R11, R8
    DECQ    R13
    JZ      tq2_flush
    JMP     tail_next2

tq2_flush:
    MOVB    R8, (DI)
    INCQ    DI
    XORL    R8, R8
    MOVQ    $4, R13

tail_next2:
    DECQ    CX
    JNZ     tail_elem2

    // R13 == 4 means the last byte was flushed and none is half-built; any
    // smaller value means the run ended mid-byte and the reference leaves the
    // remaining fields zero, which is what R8 holds.
    CMPQ    R13, $4
    JE      done_pack2
    MOVB    R8, (DI)
    INCQ    DI

done_pack2:
    VZEROUPPER
    RET


// func packTQ8AVX512Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ8AVX512Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Z0
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Z1
    VMOVSS  tq_max8<>(SB), X0
    VBROADCASTSS X0, Z2
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Z3
    VPXORD  Z4, Z4, Z4
    VBROADCASTSS tq_one<>(SB), Z6

loop_pack8_512:
    CMPQ    CX, $16
    JL      tail_pack8_512
    
    VMOVDQU32 (SI), Z7
    VADDPS  Z0, Z7, Z7
    VMULPS  Z1, Z7, Z7
    VMAXPS  Z4, Z7, Z7
    VMINPS  Z6, Z7, Z7
    VMULPS  Z2, Z7, Z7
    VADDPS  Z3, Z7, Z7
    VCVTPS2DQ Z7, Z7
    
    VPMOVDB Z7, X7 // 16 dwords -> 16 bytes
    VMOVDQU X7, (DI)
    
    ADDQ    $64, SI
    ADDQ    $16, DI
    SUBQ    $16, CX
    JMP     loop_pack8_512

tail_pack8_512:
    TESTQ   CX, CX
    JZ      done_pack8_512
    // The broadcasts above live in Z0..Z6, so the scalar tail has to reload
    // the constants into X registers of its own.
    VMOVSS  tq_pi<>(SB), X0
    VMOVSS  tq_inv2pi<>(SB), X1
    VMOVSS  tq_max8<>(SB), X2
    VMOVSS  tq_half<>(SB), X3
    VPXOR   X4, X4, X4
    VMOVSS  tq_one<>(SB), X6
loop_tail_pack8_512:
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VROUNDPS $1, X5, X5
    VCVTSS2SI X5, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JNZ     loop_tail_pack8_512
    
done_pack8_512:
    VZEROUPPER
    RET

// func packTQ4AVX512Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ4AVX512Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Z0
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Z1
    VMOVSS  tq_max4<>(SB), X0
    VBROADCASTSS X0, Z2
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Z3
    VPXORD  Z4, Z4, Z4
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Z6

loop_pack4_512:
    CMPQ    CX, $16
    JL      tail_pack4_512
    
    VMOVDQU32 (SI), Z7
    VADDPS  Z0, Z7, Z7
    VMULPS  Z1, Z7, Z7
    VMAXPS  Z4, Z7, Z7
    VMINPS  Z6, Z7, Z7
    VMULPS  Z2, Z7, Z7
    VADDPS  Z3, Z7, Z7
    VCVTPS2DQ Z7, Z7
    
    VPMOVDB Z7, X7 // 16 bytes (low nibbles)
    
    // Combine nibbles: [e1:e0], [e3:e2], ...
    VPSRLW  $8, X7, X8
    VPSLLW  $4, X8, X8
    MOVQ    $0x00FF00FF00FF00FF, AX
    VMOVQ   AX, X9
    VPAND   X7, X9, X7
    VPOR    X8, X7, X7
    
    VPACKUSWB X7, X7, X7
    VMOVQ   X7, (DI)
    
    ADDQ    $64, SI
    ADDQ    $8, DI
    SUBQ    $16, CX
    JMP     loop_pack4_512

tail_pack4_512:
    JMP ·packTQ4AVX2Kernel+0(SB) // Reuse tail

// func packTQ2AVX512Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2AVX512Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Z0
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Z1
    VMOVSS  tq_max2<>(SB), X0
    VBROADCASTSS X0, Z2
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Z3
    VPXORD  Z4, Z4, Z4
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Z6

loop_pack2_512:
    CMPQ    CX, $16
    JL      tail_pack2_512
    
    VMOVDQU32 (SI), Z7
    VADDPS  Z0, Z7, Z7
    VMULPS  Z1, Z7, Z7
    VMAXPS  Z4, Z7, Z7
    VMINPS  Z6, Z7, Z7
    VMULPS  Z2, Z7, Z7
    VADDPS  Z3, Z7, Z7
    VCVTPS2DQ Z7, Z7
    
    VPMOVDB Z7, X7 // 16 bytes (low bits)
    
    // Combine 4x2 bits
    VPSRLW  $8, X7, X8
    VPSLLW  $2, X8, X8
    MOVQ    $0x00FF00FF00FF00FF, AX
    VMOVQ   AX, X9
    VPAND   X7, X9, X7
    VPOR    X8, X7, X7 // 8 words, each e1:e0
    
    VMOVDQU X7, X8
    VPSRLD  $16, X8, X8
    VPSLLD  $4, X8, X8
    MOVQ    $0x0000FFFF0000FFFF, AX
    VMOVQ   AX, X9
    VPAND   X7, X9, X7
    VPOR    X8, X7, X7 // 4 dwords, each e3:e2:e1:e0
    
    VPACKUSWB X7, X7, X7
    VPACKUSDW X7, X7, X7
    VMOVD   X7, (DI)
    
    ADDQ    $64, SI
    ADDQ    $4, DI
    SUBQ    $16, CX
    JMP     loop_pack2_512

tail_pack2_512:
    JMP ·packTQ2AVX2Kernel+0(SB)
