// +build amd64

#include "textflag.h"

// AVX2-optimized distance functions

// func dotVertical4AVX2(SB), NOSPLIT, $0-56
TEXT ·dotVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0      // sum = 0

    CMPQ    BX, $8
    JL      dot_tail

dot_loop8:
    VMOVUPS (SI), Y4        // load q
    VMULPS  (DI), Y4, Y4    // q * v[0]
    VADDPS  Y4, Y0, Y0      // sum += q * v[0]

    VMULPS  (R8), Y4, Y4    // q * v[1]
    VADDPS  Y4, Y0, Y0      // sum += q * v[1]

    VMULPS  (R9), Y4, Y4    // q * v[2]
    VADDPS  Y4, Y0, Y0      // sum += q * v[2]

    VMULPS  (R10), Y4, Y4   // q * v[3]
    VADDPS  Y4, Y0, Y0      // sum += q * v[3]

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     dot_loop8

dot_tail:
    VEXTRACTF128    $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSS  X0, (R11)
    RET

// func cosineVertical4AVX2(SB), NOSPLIT, $0-56
TEXT ·cosineVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    CMPQ    BX, $8
    JL      cos_tail

cos_loop8:
    VMOVUPS (SI), Y4  // q

    VMOVUPS (DI), Y5  // v0
    VFMADD231PS Y5, Y4, Y0  // sum0 += q * v0

    VMOVUPS (R8), Y6  // v1
    VFMADD231PS Y6, Y4, Y1  // sum1 += q * v1

    VMOVUPS (R9), Y7  // v2
    VFMADD231PS Y7, Y4, Y2  // sum2 += q * v2

    VMOVUPS (R10), Y8  // v3
    VFMADD231PS Y8, Y4, Y3  // sum3 += q * v3

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     cos_loop8

cos_tail:
    VEXTRACTF128    $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSS  X0, (R11)
    RET

// func euclideanVertical4AVX2(SB), NOSPLIT, $0-56
TEXT ·euclideanVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0

    CMPQ    BX, $8
    JL      euc_tail

euc_loop8:
    VMOVUPS (SI), Y4
    VMOVUPS (DI), Y5
    VSUBPS  Y5, Y4, Y4
    VMULPS  Y4, Y4, Y4
    VADDPS  Y4, Y0, Y0

    VMOVUPS (R8), Y5
    VSUBPS  Y5, Y4, Y4
    VMULPS  Y4, Y4, Y4
    VADDPS  Y4, Y0, Y0

    VMOVUPS (R9), Y5
    VSUBPS  Y5, Y4, Y4
    VMULPS  Y4, Y4, Y4
    VADDPS  Y4, Y0, Y0

    VMOVUPS (R10), Y5
    VSUBPS  Y5, Y4, Y4
    VMULPS  Y4, Y4, Y4
    VADDPS  Y4, Y0, Y0

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     euc_loop8

euc_tail:
    VEXTRACTF128    $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSS  X0, (R11)
    RET

// func dotAVX2Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotAVX2Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX
    MOVQ    res+24(FP), R11

    VPXOR   Y0, Y0, Y0
    XORQ    CX, CX

dot2_loop:
    VMOVUPS (SI)(CX*4), Y4
    VMULPS  (DI)(CX*4), Y4, Y4
    VADDPS  Y4, Y0, Y0
    ADDQ    $8, CX
    CMPQ    CX, BX
    JL      dot2_loop

    VEXTRACTF128    $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSS  X0, (R11)
    RET

// func l2SquaredAVX2Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·l2SquaredAVX2Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX
    MOVQ    res+24(FP), R11

    VPXOR   Y0, Y0, Y0
    XORQ    CX, CX

l2_loop:
    VMOVUPS (SI)(CX*4), Y4
    VMOVUPS (DI)(CX*4), Y5
    VSUBPS  Y5, Y4, Y4
    VMULPS  Y4, Y4, Y4
    VADDPS  Y4, Y0, Y0
    ADDQ    $8, CX
    CMPQ    CX, BX
    JL      l2_loop

    VEXTRACTF128    $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVSS  X0, (R11)
    RET
