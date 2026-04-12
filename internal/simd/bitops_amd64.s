//go:build amd64
#include "textflag.h"

// func hammingAVX2Kernel(a, b unsafe.Pointer, n int) int
TEXT ·hammingAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), CX

    XORQ    AX, AX        // Accumulator

    // Check header
    CMPQ    CX, $4
    JB      tail_check

loop_4:
    MOVQ    (SI), BX
    MOVQ    (DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    8(SI), BX
    MOVQ    8(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    16(SI), BX
    MOVQ    16(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    MOVQ    24(SI), BX
    MOVQ    24(DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $4, CX
    CMPQ    CX, $4
    JAE     loop_4

tail_check:
    CMPQ    CX, $0
    JE      done

tail_loop:
    MOVQ    (SI), BX
    MOVQ    (DI), DX
    XORQ    DX, BX
    POPCNTQ BX, BX
    ADDQ    BX, AX

    ADDQ    $8, SI
    ADDQ    $8, DI
    DECQ    CX
    JNZ     tail_loop

done:
    MOVQ    AX, ret+24(FP)
    RET

// func andBytesAVX2Kernel(dst, src unsafe.Pointer, n int)
TEXT ·andBytesAVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    dst+0(FP), DI
    MOVQ    src+8(FP), SI
    MOVQ    n+16(FP), CX

loop_and_avx2:
    CMPQ    CX, $32
    JL      tail_and_avx2
    VMOVDQU (DI), Y0
    VMOVDQU (SI), Y1
    VPAND   Y1, Y0, Y0
    VMOVDQU Y0, (DI)
    ADDQ    $32, DI
    ADDQ    $32, SI
    SUBQ    $32, CX
    JMP     loop_and_avx2

tail_and_avx2:
    TESTQ   CX, CX
    JZ      and_done
    MOVB    (DI), AL
    ANDB    (SI), AL
    MOVB    AL, (DI)
    INCQ    DI
    INCQ    SI
    DECQ    CX
    JMP     tail_and_avx2

and_done:
    VZEROUPPER
    RET

// func orBytesAVX2Kernel(dst, src unsafe.Pointer, n int)
TEXT ·orBytesAVX2Kernel(SB), NOSPLIT, $0-24
    MOVQ    dst+0(FP), DI
    MOVQ    src+8(FP), SI
    MOVQ    n+16(FP), CX

loop_or_avx2:
    CMPQ    CX, $32
    JL      tail_or_avx2
    VMOVDQU (DI), Y0
    VMOVDQU (SI), Y1
    VPOR    Y1, Y0, Y0
    VMOVDQU Y0, (DI)
    ADDQ    $32, DI
    ADDQ    $32, SI
    SUBQ    $32, CX
    JMP     loop_or_avx2

tail_or_avx2:
    TESTQ   CX, CX
    JZ      or_done
    MOVB    (DI), AL
    ORB     (SI), AL
    MOVB    AL, (DI)
    INCQ    DI
    INCQ    SI
    DECQ    CX
    JMP     tail_or_avx2

or_done:
    VZEROUPPER
    RET

// func isAllZerosAVX2Kernel(src unsafe.Pointer, n int) bool
TEXT ·isAllZerosAVX2Kernel(SB), NOSPLIT, $0-25
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    XORQ    AX, AX // Default false (0)

    VPXOR   Y0, Y0, Y0 // All zeros

loop_zeros_avx2:
    CMPQ    CX, $32
    JL      tail_zeros_avx2
    VMOVDQU (SI), Y1
    VPCMPEQB Y0, Y1, Y2 // Y2 = (Y1 == 0) ? 0xFF : 0
    VPMOVMSKB Y2, DX
    CMPL    DX, $0xFFFFFFFF
    JNE     not_all_zeros
    
    ADDQ    $32, SI
    SUBQ    $32, CX
    JMP     loop_zeros_avx2

tail_zeros_avx2:
    TESTQ   CX, CX
    JZ      all_zeros
    MOVB    (SI), DL
    TESTB   DL, DL
    JNZ     not_all_zeros
    INCQ    SI
    DECQ    CX
    JMP     tail_zeros_avx2

all_zeros:
    MOVB    $1, ret+16(FP)
    VZEROUPPER
    RET

not_all_zeros:
    MOVB    $0, ret+16(FP)
    VZEROUPPER
    RET
