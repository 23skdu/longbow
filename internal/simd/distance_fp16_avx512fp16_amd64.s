//go:build amd64

#include "textflag.h"

// Native AVX-512 FP16 distance kernels (Sapphire Rapids+, Emerald Rapids+, Granite Rapids+)
// Processes 16 half-precision floats per iteration in 512-bit ZMM registers
// with full FP32 accumulation and scalar tail handling.

// func l2SquaredF16AVX512FP16Kernel(a, b uintptr, n int) float32
TEXT ·l2SquaredF16AVX512FP16Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX
    MOVQ    n+16(FP), DX
    VXORPS  Z0, Z0, Z0

l2_loop:
    CMPQ        DX, $0x10
    JL          l2_tail
    VCVTPH2PS   (AX), Z1
    VCVTPH2PS   (CX), Z2
    VSUBPS      Z2, Z1, Z1
    VFMADD231PS Z1, Z1, Z0
    ADDQ        $0x20, AX
    ADDQ        $0x20, CX
    SUBQ        $0x10, DX
    JMP         l2_loop

l2_tail:
    VEXTRACTF64X4 $0x00, Z0, Y1
    VEXTRACTF64X4 $0x01, Z0, Y0
    VADDPS        Y1, Y0, Y0
    VEXTRACTF128  $0x00, Y0, X1
    VEXTRACTF128  $0x01, Y0, X0
    VADDPS        X1, X0, X0
    VMOVHLPS      X0, X3, X3
    VADDPS        X3, X0, X0
    VMOVSHDUP     X0, X1
    VADDSS        X1, X0, X0

l2_scalar_loop:
    CMPQ        DX, $0x00
    JE          l2_done
    VXORPS      X1, X1, X1
    VXORPS      X2, X2, X2
    MOVWLZX     (AX), BX
    PINSRW      $0x00, BX, X1
    MOVWLZX     (CX), BX
    PINSRW      $0x00, BX, X2
    VCVTPH2PS   X1, X1
    VCVTPH2PS   X2, X2
    VSUBSS      X2, X1, X1
    VFMADD231SS X1, X1, X0
    ADDQ        $0x02, AX
    ADDQ        $0x02, CX
    DECQ        DX
    JMP         l2_scalar_loop

l2_done:
    MOVSS   X0, ret+24(FP)
    VZEROUPPER
    RET

// func dotF16AVX512FP16Kernel(a, b uintptr, n int) float32
TEXT ·dotF16AVX512FP16Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX
    MOVQ    n+16(FP), DX
    VXORPS  Z0, Z0, Z0

dot_loop:
    CMPQ        DX, $0x10
    JL          dot_tail
    VCVTPH2PS   (AX), Z1
    VCVTPH2PS   (CX), Z2
    VFMADD231PS Z1, Z2, Z0
    ADDQ        $0x20, AX
    ADDQ        $0x20, CX
    SUBQ        $0x10, DX
    JMP         dot_loop

dot_tail:
    VEXTRACTF64X4 $0x00, Z0, Y1
    VEXTRACTF64X4 $0x01, Z0, Y0
    VADDPS        Y1, Y0, Y0
    VEXTRACTF128  $0x00, Y0, X1
    VEXTRACTF128  $0x01, Y0, X0
    VADDPS        X1, X0, X0
    VMOVHLPS      X0, X3, X3
    VADDPS        X3, X0, X0
    VMOVSHDUP     X0, X1
    VADDSS        X1, X0, X0

dot_scalar_loop:
    CMPQ        DX, $0x00
    JE          dot_done
    VXORPS      X1, X1, X1
    VXORPS      X2, X2, X2
    MOVWLZX     (AX), BX
    PINSRW      $0x00, BX, X1
    MOVWLZX     (CX), BX
    PINSRW      $0x00, BX, X2
    VCVTPH2PS   X1, X1
    VCVTPH2PS   X2, X2
    VFMADD231SS X1, X2, X0
    ADDQ        $0x02, AX
    ADDQ        $0x02, CX
    DECQ        DX
    JMP         dot_scalar_loop

dot_done:
    MOVSS   X0, ret+24(FP)
    VZEROUPPER
    RET
