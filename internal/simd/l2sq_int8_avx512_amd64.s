#include "textflag.h"

// l2SquaredInt8AVX512Kernel(a, b uintptr, n int) float32
// Computes sum((a[i]-b[i])^2) for n int8 elements (no final sqrt).

TEXT ·l2SquaredInt8AVX512Kernel(SB),NOSPLIT,$0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), CX

    VXORPS Y0, Y0, Y0       // float32 accumulator x8 (YMM — reuse AVX2 reduction)

loop32_eucl_i8_avx512:
    CMPQ CX, $32
    JL   tail16_eucl_i8_avx512

    VPMOVSXBW (SI), Z1      // 32 int8 → 32 int16 (512-bit)
    VPMOVSXBW (DI), Z2
    VPSUBW Z2, Z1, Z1       // diff (signed int16)
    VPMADDWD Z1, Z1, Z1     // diff², adjacent pair sum → int32 x16
    VCVTDQ2PS Z1, Z1        // → 16 float32
    VEXTRACTF64X4 $0, Z1, Y2 // low 8 floats → Y2
    VEXTRACTF64X4 $1, Z1, Y3 // high 8 floats → Y3
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y0, Y0

    ADDQ $32, SI
    ADDQ $32, DI
    SUBQ $32, CX
    JMP  loop32_eucl_i8_avx512

tail16_eucl_i8_avx512:
    CMPQ CX, $16
    JL   tail8_eucl_i8_avx512

    VPMOVSXBW (SI), Y1      // 16 int8 → 16 int16 (256-bit)
    VPMOVSXBW (DI), Y2
    VPSUBW Y2, Y1, Y1       // diff (signed int16)
    VPMADDWD Y1, Y1, Y1     // diff² → int32 x8
    VCVTDQ2PS Y1, Y1        // → 8 float32
    VADDPS Y1, Y0, Y0

    ADDQ $16, SI
    ADDQ $16, DI
    SUBQ $16, CX

tail8_eucl_i8_avx512:
    CMPQ CX, $8
    JL   tail_scalar_eucl_i8_avx512

    VPMOVSXBW 0(SI), X1     // 8 int8 → 8 int16 (XMM)
    VPMOVSXBW 0(DI), X2
    VPSUBW X2, X1, X1        // diff
    VPMADDWD X1, X1, X1      // diff² → int32 x4
    VCVTDQ2PS X1, X1         // → 4 float32
    VEXTRACTF128 $0, Y0, X2
    VADDPS X1, X2, X2
    VINSERTF128 $0, X2, Y0, Y0

    ADDQ $8, SI
    ADDQ $8, DI
    SUBQ $8, CX

tail_scalar_eucl_i8_avx512:
        VEXTRACTF128 $1, Y0, X1
    VEXTRACTF128 $0, Y0, X0
    VADDPS X1, X0, X0
    VMOVHLPS X0, X0, X1
    VADDPS X1, X0, X0
    VMOVSHDUP X0, X2
    VADDSS X2, X0, X0
    VZEROUPPER

    TESTQ CX, CX
    JZ    done_l2sq_int8_avx512

scalar_loop_eucl_i8_avx512:
    MOVBLSX 0(SI), AX
    MOVBLSX 0(DI), BX
    SUBL BX, AX
    IMULL AX, AX
    CVTSL2SS AX, X1
    VADDSS X1, X0, X0

    INCQ SI
    INCQ DI
    DECQ CX
    JNZ  scalar_loop_eucl_i8_avx512

done_l2sq_int8_avx512:
    MOVSS X0, ret+24(FP)
    RET
