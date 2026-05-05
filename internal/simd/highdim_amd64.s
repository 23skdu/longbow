//go:build amd64
#include "textflag.h"

// func l2Squared384AVX2(a, b uintptr) float32
TEXT ·l2Squared384AVX2Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    MOVQ    $8, R8 // 8 * 48 floats = 384

l2_384_loop:
    VMOVUPS (AX), Y4
    VMOVUPS (CX), Y10
    VMOVUPS 32(AX), Y5
    VMOVUPS 32(CX), Y11
    VMOVUPS 64(AX), Y6
    VMOVUPS 64(CX), Y12
    VMOVUPS 96(AX), Y7
    VMOVUPS 96(CX), Y13
    VMOVUPS 128(AX), Y8
    VMOVUPS 128(CX), Y14
    VMOVUPS 160(AX), Y9
    VMOVUPS 160(CX), Y15

    VSUBPS  Y10, Y4, Y4
    VSUBPS  Y11, Y5, Y5
    VSUBPS  Y12, Y6, Y6
    VSUBPS  Y13, Y7, Y7
    VSUBPS  Y14, Y8, Y8
    VSUBPS  Y15, Y9, Y9

    VFMADD231PS Y4, Y4, Y0
    VFMADD231PS Y5, Y5, Y1
    VFMADD231PS Y6, Y6, Y2
    VFMADD231PS Y7, Y7, Y3
    VFMADD231PS Y8, Y8, Y0
    VFMADD231PS Y9, Y9, Y1

    ADDQ    $192, AX
    ADDQ    $192, CX
    DECQ    R8
    JNZ     l2_384_loop

    VADDPS  Y1, Y0, Y0
    VADDPS  Y3, Y2, Y2
    VADDPS  Y2, Y0, Y0

    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func l2Squared768AVX2(a, b uintptr) float32
TEXT ·l2Squared768AVX2Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    MOVQ    $24, R8 // 24 iterations * 32 floats = 768

l2_768_loop:
    VMOVUPS (AX), Y4
    VMOVUPS (CX), Y8
    VMOVUPS 32(AX), Y5
    VMOVUPS 32(CX), Y9
    VMOVUPS 64(AX), Y6
    VMOVUPS 64(CX), Y10
    VMOVUPS 96(AX), Y7
    VMOVUPS 96(CX), Y11

    VSUBPS  Y8, Y4, Y4
    VSUBPS  Y9, Y5, Y5
    VSUBPS  Y10, Y6, Y6
    VSUBPS  Y11, Y7, Y7

    VFMADD231PS Y4, Y4, Y0
    VFMADD231PS Y5, Y5, Y1
    VFMADD231PS Y6, Y6, Y2
    VFMADD231PS Y7, Y7, Y3

    ADDQ    $128, AX
    ADDQ    $128, CX
    DECQ    R8
    JNZ     l2_768_loop

    VADDPS  Y1, Y0, Y0
    VADDPS  Y3, Y2, Y2
    VADDPS  Y2, Y0, Y0

    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func dot384AVX2(a, b uintptr) float32
TEXT ·dot384AVX2Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    MOVQ    $8, R8

dot_384_loop:
    VMOVUPS (AX), Y4
    VMOVUPS (CX), Y5
    VFMADD231PS Y4, Y5, Y0
    
    VMOVUPS 32(AX), Y4
    VMOVUPS 32(CX), Y5
    VFMADD231PS Y4, Y5, Y1
    
    VMOVUPS 64(AX), Y4
    VMOVUPS 64(CX), Y5
    VFMADD231PS Y4, Y5, Y2
    
    VMOVUPS 96(AX), Y4
    VMOVUPS 96(CX), Y5
    VFMADD231PS Y4, Y5, Y3
    
    VMOVUPS 128(AX), Y4
    VMOVUPS 128(CX), Y5
    VFMADD231PS Y4, Y5, Y0
    
    VMOVUPS 160(AX), Y4
    VMOVUPS 160(CX), Y5
    VFMADD231PS Y4, Y5, Y1

    ADDQ    $192, AX
    ADDQ    $192, CX
    DECQ    R8
    JNZ     dot_384_loop

    VADDPS  Y1, Y0, Y0
    VADDPS  Y3, Y2, Y2
    VADDPS  Y2, Y0, Y0

    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET

// func dot768AVX2(a, b uintptr) float32
TEXT ·dot768AVX2Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    MOVQ    $24, R8 // 24 * 32 floats = 768

dot_768_loop:
    VMOVUPS (AX), Y4
    VMOVUPS (CX), Y8
    VFMADD231PS Y4, Y8, Y0
    
    VMOVUPS 32(AX), Y5
    VMOVUPS 32(CX), Y9
    VFMADD231PS Y5, Y9, Y1
    
    VMOVUPS 64(AX), Y6
    VMOVUPS 64(CX), Y10
    VFMADD231PS Y6, Y10, Y2
    
    VMOVUPS 96(AX), Y7
    VMOVUPS 96(CX), Y11
    VFMADD231PS Y7, Y11, Y3

    ADDQ    $128, AX
    ADDQ    $128, CX
    DECQ    R8
    JNZ     dot_768_loop

    VADDPS  Y1, Y0, Y0
    VADDPS  Y3, Y2, Y2
    VADDPS  Y2, Y0, Y0

    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0

    MOVSS   X0, ret+16(FP)
    VZEROUPPER
    RET
