// +build amd64

#include "textflag.h"

// func dotInt4AVX512Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4AVX512Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Z0, Z0, Z0          // sum accumulator
    VMOVDQU64 mask_low<>(SB), Z10
    VMOVDQU64 one_word<>(SB), Z11

    CMPQ    BX, $64
    JL      tail

loop:
    // Load 64 bytes (128 int4 values)
    VMOVDQU64 (SI), Z1
    VMOVDQU64 (DI), Z2

    // Unpack low nibbles
    VPANDD   Z10, Z1, Z3
    VPANDD   Z10, Z2, Z4
    VPMADDUBSW Z3, Z4, Z7
    
    // Unpack high nibbles
    VPSRLW   $4, Z1, Z5
    VPANDD   Z10, Z5, Z5
    VPSRLW   $4, Z2, Z6
    VPANDD   Z10, Z6, Z6
    VPMADDUBSW Z5, Z6, Z8

    // Sum partials
    VPADDW   Z7, Z8, Z7
    VPMADDWD Z11, Z7, Z7       // Z7 = 16 int32
    
    // Convert to float and accumulate
    VCVTDQ2PS Z7, Z7
    VADDPS  Z7, Z0, Z0

    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $64, BX
    CMPQ    BX, $64
    JGE     loop

tail:
    // Horizontal reduction
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET

// func dotInt4AVX2Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4AVX2Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Y0, Y0, Y0          // sum accumulator
    VMOVDQU mask_low<>(SB), Y10
    VMOVDQU one_word<>(SB), Y11

    CMPQ    BX, $32
    JL      tail_avx2

loop_avx2:
    // Load 32 bytes (64 int4 values)
    VMOVDQU (SI), Y1
    VMOVDQU (DI), Y2

    // Unpack low nibbles
    VPAND    Y10, Y1, Y3
    VPAND    Y10, Y2, Y4
    VPMADDUBSW Y3, Y4, Y7
    
    // Unpack high nibbles
    VPSRLW   $4, Y1, Y5
    VPAND    Y10, Y5, Y5
    VPSRLW   $4, Y2, Y6
    VPAND    Y10, Y6, Y6
    VPMADDUBSW Y5, Y6, Y8

    // Sum partials
    VPADDW   Y7, Y8, Y7
    VPMADDWD Y11, Y7, Y7       // Y7 = 8 int32
    
    // Convert to float and accumulate
    VCVTDQ2PS Y7, Y7
    VADDPS  Y7, Y0, Y0

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $32, BX
    CMPQ    BX, $32
    JGE     loop_avx2

tail_avx2:
    // Horizontal reduction
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET

GLOBL mask_low<>(SB), (RODATA|NOPTR), $64
DATA mask_low<>+0(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+8(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+16(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+24(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+32(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+40(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+48(SB)/8, $0x0F0F0F0F0F0F0F0F
DATA mask_low<>+56(SB)/8, $0x0F0F0F0F0F0F0F0F

GLOBL one_word<>(SB), (RODATA|NOPTR), $64
DATA one_word<>+0(SB)/8, $0x0001000100010001
DATA one_word<>+8(SB)/8, $0x0001000100010001
DATA one_word<>+16(SB)/8, $0x0001000100010001
DATA one_word<>+24(SB)/8, $0x0001000100010001
DATA one_word<>+32(SB)/8, $0x0001000100010001
DATA one_word<>+40(SB)/8, $0x0001000100010001
DATA one_word<>+48(SB)/8, $0x0001000100010001
DATA one_word<>+56(SB)/8, $0x0001000100010001
