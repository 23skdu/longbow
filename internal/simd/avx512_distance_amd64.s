//go:build amd64 && avx512
#include "textflag.h"

// func l2Squared128AVX512Kernel(a, b uintptr) float32
TEXT ·l2Squared128AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    // 8 ZMM registers = 128 floats
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    VMOVUPS 256(AX), Z4
    VMOVUPS 256(CX), Z8
    VMOVUPS 320(AX), Z5
    VMOVUPS 320(CX), Z9
    VMOVUPS 384(AX), Z6
    VMOVUPS 384(CX), Z10
    VMOVUPS 448(AX), Z7
    VMOVUPS 448(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    // Reduction to scalar
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2Squared384AVX512Kernel(a, b uintptr) float32
TEXT ·l2Squared384AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    MOVQ    $6, R8 // 6 iterations * 64 floats = 384

l2_384_loop:
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    ADDQ    $256, AX
    ADDQ    $256, CX
    DECQ    R8
    JNZ     l2_384_loop

    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2Squared768AVX512Kernel(a, b uintptr) float32
TEXT ·l2Squared768AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    MOVQ    $12, R8 // 12 * 64 = 768

l2_768_loop:
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    ADDQ    $256, AX
    ADDQ    $256, CX
    DECQ    R8
    JNZ     l2_768_loop

    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2Squared1024AVX512Kernel(a, b uintptr) float32
TEXT ·l2Squared1024AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    MOVQ    $16, R8 // 16 * 64 = 1024

l2_1024_loop:
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    ADDQ    $256, AX
    ADDQ    $256, CX
    DECQ    R8
    JNZ     l2_1024_loop

    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2Squared3072AVX512Kernel(a, b uintptr) float32
TEXT ·l2Squared3072AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    MOVQ    $48, R8 // 48 * 64 = 3072

l2_3072_loop:
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    ADDQ    $256, AX
    ADDQ    $256, CX
    DECQ    R8
    JNZ     l2_3072_loop

    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2SquaredAVX512Kernel(a, b uintptr, n int) float32
TEXT ·l2SquaredAVX512Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX
    MOVQ    n+16(FP), R8

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

l2_generic_loop_64:
    CMPQ    R8, $64
    JL      l2_generic_loop_16
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z8
    VMOVUPS 64(AX), Z5
    VMOVUPS 64(CX), Z9
    VMOVUPS 128(AX), Z6
    VMOVUPS 128(CX), Z10
    VMOVUPS 192(AX), Z7
    VMOVUPS 192(CX), Z11

    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    ADDQ    $256, AX
    ADDQ    $256, CX
    SUBQ    $64, R8
    JMP     l2_generic_loop_64

l2_generic_loop_16:
    CMPQ    R8, $16
    JL      l2_generic_tail
    VMOVUPS (AX), Z4
    VMOVUPS (CX), Z5
    VSUBPS  Z5, Z4, Z4
    VFMADD231PS Z4, Z4, Z0
    ADDQ    $64, AX
    ADDQ    $64, CX
    SUBQ    $16, R8
    JMP     l2_generic_loop_16

l2_generic_tail:
    CMPQ    R8, $0
    JE      l2_generic_done
    VMOVSS  (AX), X4
    VMOVSS  (CX), X5
    VSUBSS  X5, X4, X4
    VMULSS  X4, X4, X4
    VADDSS  X4, X0, X0
    ADDQ    $4, AX
    ADDQ    $4, CX
    DECQ    R8
    JMP     l2_generic_tail

l2_generic_done:
    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0

    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    MOVSS   X0, ret+24(FP)
    VZEROUPPER
    RET
