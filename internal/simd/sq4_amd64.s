// +build amd64

#include "textflag.h"

// func dotInt4AVX512(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4AVX512(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Z0, Z0, Z0          // sum accumulator
    CMPQ    BX, $32
    JL      tail

loop:
    // Load 32 bytes (64 int4 values)
    VMOVDQU32 (SI), Z1
    VMOVDQU32 (DI), Z2

    // Unpack low nibbles
    VPANDD   mask_low<>(SB), Z1, Z3
    VPANDD   mask_low<>(SB), Z2, Z4
    
    // Unpack high nibbles
    VPSRLD   $4, Z1, Z5
    VPANDD   mask_low<>(SB), Z5, Z5
    VPSRLD   $4, Z2, Z6
    VPANDD   mask_low<>(SB), Z6, Z6

    // Convert to float and multiply-accumulate
    VCVTDQ2PS Z3, Z3
    VCVTDQ2PS Z4, Z4
    VFMADD231PS Z3, Z4, Z0

    VCVTDQ2PS Z5, Z5
    VCVTDQ2PS Z6, Z6
    VFMADD231PS Z5, Z6, Z0

    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $32, BX
    CMPQ    BX, $32
    JGE     loop

tail:
    // Horizontal reduction
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
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
