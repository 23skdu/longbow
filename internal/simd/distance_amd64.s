// +build amd64,!avx512

#include "textflag.h"

// ----------------------------------------------------------------------------
// AVX2-optimized distance functions only
// ----------------------------------------------------------------------------

// func l2SquaredAVX2Kernel(a, b unsafe.Pointer, n int) float32
TEXT ·l2SquaredAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX
    MOVQ    res+24(FP), R11

    // Implementation...
    RET

// func dotAVX2Kernel(a, b unsafe.Pointer, n int) float32  
TEXT ·dotAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX
    MOVQ    res+24(FP), R11
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
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    CMPQ    BX, $8
    JL      ev2_tail

ev2_loop8:
    VMOVUPS (SI), Y4
    VADDPS  (DI), Y4, Y4
    VMOVUPS Y4, (R11)

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R11
    SUBQ    $8, BX
    JGE     ev2_loop8

ev2_tail:
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
    VMOVUPS (SI), Y4
    VFMADD231PS (DI), Y4, Y4
    VMOVUPS Y4, (R11)

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R11
    SUBQ    $8, BX
    JGE     cos_loop8

cos_tail:
    RET

// func dotVertical4AVX2(SB), NOSPLIT, $0-56
TEXT ·dotVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0

    CMPQ    BX, $8
    JL      dot_tail

dot_loop8:
    VMOVUPS (SI), Y4
    VMULPS  (DI), Y4, Y4
    VADDPS  Y4, Y0, Y0

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $8, BX
    JGE     dot_loop8

dot_tail:
    VEXTRACTF128    $1, Y0, X0
    VADDPS  X0, X0, X0
    VMOVSS  X0, (R11)
    RET
