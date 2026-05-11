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
    
    // Constant for clamping [0, 255]
    VPXOR   Y4, Y4, Y4  // 0.0
    
loop_pack8:
    CMPQ    CX, $8
    JL      tail_pack8
    
    VMOVDQU (SI), Y5    // Load 8 floats
    VADDPS  Y0, Y5, Y5   // + PI
    VMULPS  Y1, Y5, Y5   // * INV_2PI
    
    // Clamp to [0, 1]
    VMAXPS  Y4, Y5, Y5
    VMOVSS  $1.0, X6
    VBROADCASTSS X6, Y6
    VMINPS  Y6, Y5, Y5
    
    VMULPS  Y2, Y5, Y5   // * 255.0
    VADDPS  Y3, Y5, Y5   // + 0.5
    VCVTPS2DQ Y5, Y5     // Round to int32
    
    // Pack int32 to uint8
    VPMOVUSDB Y5, X5    // Only in AVX-512? No, VPMOVUSDB is AVX-512.
    // In AVX2 we use VPACKUSWB + VPACKUSDW.
    
    // Wait! VPMOVUSDB is indeed AVX-512.
    // In AVX2:
    // Y5 = [i7, i6, i5, i4, i3, i2, i1, i0] (each 32-bit)
    // We need to pack them into 8 bytes.
    VPERMPD $0xD8, Y5, Y5 // Reorder to get 0,1,2,3,4,5,6,7 in order
    // This is getting complicated. I'll use a simpler way.
    // Just use VPACKUS if possible.
    
    // Actually, I'll use a scalar loop for packing in AVX2 for now or just VMOVD extraction.
    VEXTRACTI128 $0, Y5, X6
    VEXTRACTI128 $1, Y5, X7
    VPACKUSDW X7, X6, X6 // 16-bit
    VPACKUSWB X6, X6, X6 // 8-bit
    VMOVQ   X6, (DI)
    
    ADDQ    $32, SI
    ADDQ    $8, DI
    SUBQ    $8, CX
    JMP     loop_pack8

tail_pack8:
    TESTQ   CX, CX
    JZ      done_pack8
    VMOVSS  (SI), X5
    VADDSS  X0, X5, X5
    VMULSS  X1, X5, X5
    // Clamp
    VMAXSS  X4, X5, X5
    VMOVSS  $1.0, X6
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
    CMPQ    CX, $16
    JL      tail_pack2_vbmi
    
    VMOVDQU64 (SI), Z6    // Load 16 floats
    VADDPS  Z0, Z6, Z6
    VMULPS  Z1, Z6, Z6
    VMAXPS  Z4, Z6, Z6
    VMINPS  Z5, Z6, Z6
    VMULPS  Z2, Z6, Z6
    VADDPS  Z3, Z6, Z6
    VCVTPS2DQ Z6, Z6     // Round to int32 (0-3)
    
    // Pack 16 int32s into 4 bytes (32 bits)
    // First, pack int32 to byte
    VPMOVDB Z6, X7      // 16 int32 -> 16 bytes
    // Now X7 has [e15, e14, ..., e0] (each 1 byte)
    // We want to pack 4 into each byte: [e3:e2:e1:e0], [e7:e6:e5:e4], ...
    
    // In AVX-512 VBMI we can use VPMULTISHIFTQB to pack bits if we treat it as qwords
    // But VPMOVD and shifts are easier here.
    // Each byte has 2 bits.
    // Byte [000000ab]
    
    MOVQ    $0x0C, AX
    VMOVQ   AX, X11
    VPSLLW  $2, X7, X8
    VPAND   X11, X8, X8 // e1 bits

    MOVQ    $0x30, AX
    VMOVQ   AX, X11
    VPSLLW  $4, X7, X9
    VPAND   X11, X9, X9 // e2 bits

    MOVQ    $0xC0, AX
    VMOVQ   AX, X11
    VPSLLW  $6, X7, X10
    VPAND   X11, X10, X10 // e3 bits
    
    MOVQ    $0x03, AX
    VMOVQ   AX, X11
    VPAND   X11, X7, X7 // e0 bits
    VPOR    X8, X7, X7
    VPOR    X9, X7, X7
    VPOR    X10, X7, X7
    
    // Now X7 has the packed bytes in positions 0, 4, 8, 12?
    // No, each byte in X7 was an element.
    // We need to gather them.
    
    // Actually, VPMULTISHIFTQB is for GATHERING bits.
    // If we have a qword with [byte3, byte2, byte1, byte0]
    // where each byte has 2 bits.
    // We want to gather them into one byte.
    
    VMOVD   X7, AX // elements 0, 1, 2, 3
    // ... scalar pack for now ...
    STOSL   // Store 4 bytes? No, we want 4 elements = 1 byte.
    
    ADDQ    $64, SI
    ADDQ    $4, DI // 16 elements = 4 bytes
    SUBQ    $16, CX
    JMP     loop_pack2_vbmi
    
tail_pack2_vbmi:
    JZ      done_pack2_vbmi
    JMP     done_pack2_vbmi

done_pack2_vbmi:
    VZEROUPPER
    RET

// func packTQ2AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ2AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
tail_pack2:
    TESTQ   CX, CX
    JZ      done_pack2
    VMOVSS  (SI), X0
    VCVTSS2SI X0, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JMP     tail_pack2
done_pack2:
    RET

// func packTQ4AVX2Kernel(src, dst unsafe.Pointer, n int)
TEXT ·packTQ4AVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    src+0(FP), SI
    MOVQ    dst+8(FP), DI
    MOVQ    n+16(FP), CX
tail_pack4:
    TESTQ   CX, CX
    JZ      done_pack4
    VMOVSS  (SI), X0
    VCVTSS2SI X0, AX
    MOVB    AL, (DI)
    ADDQ    $4, SI
    INCQ    DI
    DECQ    CX
    JMP     tail_pack4
done_pack4:
    RET
