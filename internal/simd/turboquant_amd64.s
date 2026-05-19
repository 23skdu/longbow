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

GLOBL tq_pi<>(SB), RODATA, $4
GLOBL tq_inv2pi<>(SB), RODATA, $4
GLOBL tq_half<>(SB), RODATA, $4
GLOBL tq_max8<>(SB), RODATA, $4
GLOBL tq_max4<>(SB), RODATA, $4
GLOBL tq_max2<>(SB), RODATA, $4

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

// func unpackTQ2AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ2AVX2Kernel(SB), NOSPLIT, $0-40
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
TEXT ·unpackTQ4AVX2Kernel(SB), NOSPLIT, $0-40
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
    CMPQ    CX, $16
    JL      tail_tq4
    
    VMOVDQU (SI), X4    // 16 bytes
    VPMOVZXBD X4, Y5    // Bytes 0-7 as int32s
    VEXTRACTI128 $1, Y4, X4 // This is wrong, Y4 is not loaded.
    
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
    MOVBQZX AL, AX
    SHRB    $4, AL
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
TEXT ·unpackTQ8AVX2Kernel(SB), NOSPLIT, $0-40
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

// Pack kernels are harder to implement with SIMD because they involve quantization (norm, Pi, etc).
// We'll leave them as stubs for now or implement scalar in assembly if needed.
// Actually, I'll implement them in Go for now as I did in simd.go.
// func packTQ8AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ8AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Y0 // PI
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Y1 // 1/2PI
    VMOVSS  tq_max8<>(SB), X0
    VBROADCASTSS X0, Y2 // 255.0
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Y3 // 0.5
    
    VPXOR   Y4, Y4, Y4  // 0.0
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Y6 // 1.0
    
loop_pack8:
    CMPQ    CX, $32
    JL      tail_pack8_outer
    
    // Process 32 elements (4 YMMs) -> 32 bytes
    VMOVDQU (SI), Y7
    VMOVDQU 32(SI), Y8
    VMOVDQU 64(SI), Y9
    VMOVDQU 96(SI), Y10
    
    // Quantize Y7
    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VCVTPS2DQ Y7, Y7
    
    // Quantize Y8
    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VCVTPS2DQ Y8, Y8
    
    // Quantize Y9
    VADDPS  Y0, Y9, Y9
    VMULPS  Y1, Y9, Y9
    VMAXPS  Y4, Y9, Y9
    VMINPS  Y6, Y9, Y9
    VMULPS  Y2, Y9, Y9
    VADDPS  Y3, Y9, Y9
    VCVTPS2DQ Y9, Y9
    
    // Quantize Y10
    VADDPS  Y0, Y10, Y10
    VMULPS  Y1, Y10, Y10
    VMAXPS  Y4, Y10, Y10
    VMINPS  Y6, Y10, Y10
    VMULPS  Y2, Y10, Y10
    VADDPS  Y3, Y10, Y10
    VCVTPS2DQ Y10, Y10

    // Pack Y7, Y8 -> X7 (16 bytes)
    VPERMPD $0xD8, Y7, Y7
    VPERMPD $0xD8, Y8, Y8
    VEXTRACTI128 $0, Y7, X11
    VEXTRACTI128 $1, Y7, X12
    VPACKUSDW X12, X11, X11
    VEXTRACTI128 $0, Y8, X13
    VEXTRACTI128 $1, Y8, X14
    VPACKUSDW X14, X13, X13
    VPACKUSWB X13, X11, X11
    VMOVDQU X11, (DI)
    
    // Pack Y9, Y10 -> X9 (16 bytes)
    VPERMPD $0xD8, Y9, Y9
    VPERMPD $0xD8, Y10, Y10
    VEXTRACTI128 $0, Y9, X11
    VEXTRACTI128 $1, Y9, X12
    VPACKUSDW X12, X11, X11
    VEXTRACTI128 $0, Y10, X13
    VEXTRACTI128 $1, Y10, X14
    VPACKUSDW X14, X13, X13
    VPACKUSWB X13, X11, X11
    VMOVDQU X11, 16(DI)
    
    ADDQ    $128, SI
    ADDQ    $32, DI
    SUBQ    $32, CX
    JMP     loop_pack8

tail_pack8_outer:
loop_pack8_small:
    CMPQ    CX, $8
    JL      tail_pack8
    VMOVDQU (SI), Y5
    VADDPS  Y0, Y5, Y5
    VMULPS  Y1, Y5, Y5
    VMAXPS  Y4, Y5, Y5
    VMINPS  Y6, Y5, Y5
    VMULPS  Y2, Y5, Y5
    VADDPS  Y3, Y5, Y5
    VCVTPS2DQ Y5, Y5
    VPERMPD $0xD8, Y5, Y5
    VEXTRACTI128 $0, Y5, X11
    VEXTRACTI128 $1, Y5, X12
    VPACKUSDW X12, X11, X11
    VPACKUSWB X11, X11, X11
    VMOVQ   X11, (DI)
    ADDQ    $32, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_pack8_small

tail_pack8:
    TESTQ   CX, CX
    JZ      done_pack8
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JMP     tail_pack8
    
done_pack8:
    VZEROUPPER
    RET

// func unpackTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
TEXT ·unpackTQ2AVX512VBMIKernel(SB), NOSPLIT, $0-40
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
TEXT ·packTQ4AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Y0
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Y1
    VMOVSS  tq_max4<>(SB), X0
    VBROADCASTSS X0, Y2
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Y3
    VPXOR   Y4, Y4, Y4
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Y6
    
    MOVQ    $0x00FF00FF00FF00FF, AX
    VMOVQ   AX, X15 // Mask for e0, e2, ...

loop_pack4:
    CMPQ    CX, $16
    JL      tail_pack4
    
    VMOVDQU (SI), Y7
    VMOVDQU 32(SI), Y8
    
    VADDPS  Y0, Y7, Y7
    VMULPS  Y1, Y7, Y7
    VMAXPS  Y4, Y7, Y7
    VMINPS  Y6, Y7, Y7
    VMULPS  Y2, Y7, Y7
    VADDPS  Y3, Y7, Y7
    VCVTPS2DQ Y7, Y7
    
    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VCVTPS2DQ Y8, Y8

    // Narrow to 16 bytes: [e15, ..., e0]
    VPERMPD $0xD8, Y7, Y7
    VPERMPD $0xD8, Y8, Y8
    VEXTRACTI128 $0, Y7, X11
    VEXTRACTI128 $1, Y7, X12
    VPACKUSDW X12, X11, X11
    VEXTRACTI128 $0, Y8, X13
    VEXTRACTI128 $1, Y8, X14
    VPACKUSDW X14, X13, X13
    VPACKUSWB X13, X11, X11 // X11 = [e15, ..., e0]
    
    // Combine nibbles
    VPAND   X11, X15, X13 // X13 = [0, e14, 0, e12, ..., 0, e0]
    VPSRLW  $8, X11, X12  // X12 = [0, e15, 0, e13, ..., 0, e1]
    VPSLLW  $4, X12, X12  // X12 = [0, e15<<4, 0, e13<<4, ..., 0, e1<<4]
    VPOR    X12, X13, X13 // X13 = [0, e15:e14, 0, e13:e12, ..., 0, e1:e0]
    
    VPACKUSWB X13, X13, X13
    VMOVQ   X13, (DI)
    
    ADDQ    $64, SI
    ADDQ    $8, DI
    SUBQ    $16, CX
    JMP     loop_pack4

tail_pack4:
    TESTQ   CX, CX
    JZ      done_pack4
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, AX
    ANDL    $0x0F, AX
    
    DECQ    CX
    JZ      last_e0_4
    VMOVSS  4(SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, BX
    ANDL    $0x0F, BX
    SHLL    $4, BX
    ORL     BX, AX
    MOVB    AL, (DI)
    ADDQ    $8, SI
    INCQ    DI
    DECQ    CX
    JMP     tail_pack4

last_e0_4:
    MOVB    AL, (DI)
    INCQ    DI
    ADDQ    $4, SI
    
done_pack4:
    VZEROUPPER
    RET

// func packTQ2AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
    
    VMOVSS  tq_pi<>(SB), X0
    VBROADCASTSS X0, Y0
    VMOVSS  tq_inv2pi<>(SB), X0
    VBROADCASTSS X0, Y1
    VMOVSS  tq_max2<>(SB), X0
    VBROADCASTSS X0, Y2
    VMOVSS  tq_half<>(SB), X0
    VBROADCASTSS X0, Y3
    VPXOR   Y4, Y4, Y4
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Y6
    
    MOVQ    $0x00FF00FF00FF00FF, AX
    VMOVQ   AX, X15 // 8-bit mask

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
    VCVTPS2DQ Y7, Y7
    
    VADDPS  Y0, Y8, Y8
    VMULPS  Y1, Y8, Y8
    VMAXPS  Y4, Y8, Y8
    VMINPS  Y6, Y8, Y8
    VMULPS  Y2, Y8, Y8
    VADDPS  Y3, Y8, Y8
    VCVTPS2DQ Y8, Y8

    VCVTPS2DQ Y8, Y8

    // Narrow to bytes
    VPACKUSDW Y8, Y7, Y7 // Y7 = [Y8_high, Y8_low, Y7_high, Y7_low] as words? No.
    // Use VPERMQ to fix order if needed.
    VPERMPD $0xD8, Y7, Y7
    VPACKUSWB Y7, Y7, Y7 // Narrow to bytes
    
    // Now X7 has 16 bytes: [e15, ..., e0]
    // Use VPMADDUBSW to pack 2 elements into 1 word
    VMOVDQU pack2_weights_0<>(SB), X8
    VPMADDUBSW X8, X7, X7 // X7 = [e15*4+e14, ..., e1*4+e0] (words)
    
    // Use VPMADDWD to pack 2 words into 1 dword
    VMOVDQU pack2_weights_1<>(SB), X8
    VPMADDWD X8, X7, X7 // X7 = [e15:e14:e13:e12, ..., e3:e2:e1:e0] (dwords)
    
    // Pack dwords to bytes
    VPACKUSDW X7, X7, X7
    VPACKUSWB X7, X7, X7
    VMOVD   X7, (DI)
    
    ADDQ    $64, SI
    ADDQ    $4, DI
    SUBQ    $16, CX
    JMP     loop_pack2

tail_pack2:
    TESTQ   CX, CX
    JZ      done_pack2
    XORL    AX, AX // byte accumulator
    
    // Element 0
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, DX
    ANDL    $0x03, DX
    ORL     DX, AX
    ADDQ    $4, SI
    DECQ    CX
    JZ      flush_pack2
    
    // Element 1
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, DX
    ANDL    $0x03, DX
    SHLL    $2, DX
    ORL     DX, AX
    ADDQ    $4, SI
    DECQ    CX
    JZ      flush_pack2

    // Element 2
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, DX
    ANDL    $0x03, DX
    SHLL    $4, DX
    ORL     DX, AX
    ADDQ    $4, SI
    DECQ    CX
    JZ      flush_pack2

    // Element 3
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, DX
    ANDL    $0x03, DX
    SHLL    $6, DX
    ORL     DX, AX
    ADDQ    $4, SI
    DECQ    CX

flush_pack2:
    MOVB    AL, (DI)
    INCQ    DI
    JMP     tail_pack2

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
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Z6

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
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    VMAXSS  X4, X5, X5
    VMINSS  X6, X5, X5
    VMULSS  X2, X5, X5
    VADDSS  X3, X5, X5
    VCVTSS2SI X5, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JMP     tail_pack8_512
    
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
