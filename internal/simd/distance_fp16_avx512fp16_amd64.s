//go:build amd64

#include "textflag.h"

// Native AVX-512 FP16 distance kernels (Sapphire Rapids+, Emerald Rapids+, Granite Rapids+)
// Processes 32 half-precision floats per iteration directly in 512-bit ZMM registers
// using VFMADD231PH / VSUBPH without expanding through float32.

// func l2SquaredF16AVX512FP16Kernel(a, b uintptr, n int) float32
TEXT ·l2SquaredF16AVX512FP16Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX
    MOVQ    n+16(FP), DX

    XORPS   X0, X0
    CMPQ    DX, $32
    JL      l2_tail

l2_loop32:
    VMOVDQU64 (AX), Z1
    VMOVDQU64 (CX), Z2

    // VSUBPH Z2, Z1, Z3: EVEX.512.66.0F38.W0 5C /r
    BYTE $0x62; BYTE $0xF1; BYTE $0x74; BYTE $0x48; BYTE $0x5C; BYTE $0xDA

    // VFMADD231PH Z3, Z3, Z0: EVEX.512.66.0F38.W0 B8 /r
    BYTE $0x62; BYTE $0xF2; BYTE $0x65; BYTE $0x48; BYTE $0xB8; BYTE $0xC3

    ADDQ    $64, AX
    ADDQ    $64, CX
    SUBQ    $32, DX
    CMPQ    DX, $32
    JGE     l2_loop32

l2_tail:
    VEXTRACTF32X8 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    MOVSHDUP X0, X1
    ADDSS   X1, X0
    MOVHLPS X0, X1
    ADDSS   X1, X0

    MOVSS   X0, ret+24(FP)
    RET

// func dotF16AVX512FP16Kernel(a, b uintptr, n int) float32
TEXT ·dotF16AVX512FP16Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), AX
    MOVQ    b+8(FP), CX
    MOVQ    n+16(FP), DX

    XORPS   X0, X0
    CMPQ    DX, $32
    JL      dot_tail

dot_loop32:
    VMOVDQU64 (AX), Z1
    VMOVDQU64 (CX), Z2

    // VFMADD231PH Z2, Z1, Z0: EVEX.512.66.0F38.W0 B8 /r
    BYTE $0x62; BYTE $0xF2; BYTE $0x75; BYTE $0x48; BYTE $0xB8; BYTE $0xC2

    ADDQ    $64, AX
    ADDQ    $64, CX
    SUBQ    $32, DX
    CMPQ    DX, $32
    JGE     dot_loop32

dot_tail:
    VEXTRACTF32X8 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    MOVSHDUP X0, X1
    ADDSS   X1, X0
    MOVHLPS X0, X1
    ADDSS   X1, X0

    MOVSS   X0, ret+24(FP)
    RET
