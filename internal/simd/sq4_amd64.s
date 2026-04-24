// +build amd64

#include "textflag.h"

// func dotInt4AVX512Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4AVX512Kernel(SB), NOSPLIT, $0-28
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

    // Load mask into Z10
    VMOVDQU32 mask_low<>(SB), Z10

    // Unpack low nibbles
    VPANDD   Z10, Z1, Z3
    VPANDD   Z10, Z2, Z4
    
    // Unpack high nibbles
    VPSRLD   $4, Z1, Z5
    VPANDD   Z10, Z5, Z5
    VPSRLD   $4, Z2, Z6
    VPANDD   Z10, Z6, Z6

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

// func dotInt4AVX2Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4AVX2Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Y0, Y0, Y0          // sum accumulator
    CMPQ    BX, $16
    JL      tail_avx2

loop_avx2:
    // Load 16 bytes (32 int4 values)
    VMOVDQU (SI), Y1
    VMOVDQU (DI), Y2

    // Load mask into Y10
    VMOVDQU mask_low<>(SB), Y10

    // Unpack low nibbles
    VPAND    Y10, Y1, Y3
    VPAND    Y10, Y2, Y4
    
    // Unpack high nibbles
    VPSRLD   $4, Y1, Y5
    VPAND    Y10, Y5, Y5
    VPSRLD   $4, Y2, Y6
    VPAND    Y10, Y6, Y6

    // Convert to float and multiply-accumulate
    VCVTDQ2PS Y3, Y3
    VCVTDQ2PS Y4, Y4
    VFMADD231PS Y3, Y4, Y0

    VCVTDQ2PS Y5, Y5
    VCVTDQ2PS Y6, Y6
    VFMADD231PS Y5, Y6, Y0

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     loop_avx2

tail_avx2:
    // Horizontal reduction
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
