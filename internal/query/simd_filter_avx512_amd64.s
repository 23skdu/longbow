//go:build amd64 && !nosimd
// +build amd64,!nosimd

#include "textflag.h"

// func fastPathInt32EqualAVX512Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)
TEXT ·fastPathInt32EqualAVX512Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    MOVL    val+16(FP), DX
    MOVQ    result+24(FP), DI

    VPBROADCASTD DX, Z0

loop_i32_512:
    CMPQ    CX, $16
    JL      tail_i32_512

    VMOVDQU32 (SI), Z1
    VPCMPD  $0, Z0, Z1, K1   // $0 = _CMP_EQ_OQ
    
    VPMOVM2D K1, Z2
    VMOVDQU32 Z2, (DI)
    
    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $16, CX
    JMP     loop_i32_512

tail_i32_512:
    CMPQ    CX, $0
    JE      done_i32_512
tail_loop_i32_512:
    MOVL    (SI), R8
    CMPL    R8, DX
    JNE     no_match_i32_512
    MOVL    $-1, (DI)
    JMP     next_i32_512
no_match_i32_512:
    MOVL    $0, (DI)
next_i32_512:
    ADDQ    $4, SI
    ADDQ    $4, DI
    DECQ    CX
    JNZ     tail_loop_i32_512

done_i32_512:
    VZEROUPPER
    RET

// func fastPathFloat32EqualAVX512Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)
TEXT ·fastPathFloat32EqualAVX512Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    VMOVSS  val+16(FP), X0
    MOVQ    result+24(FP), DI

    VBROADCASTSS X0, Z0

loop_f32_512:
    CMPQ    CX, $16
    JL      tail_f32_512

    VMOVUPS (SI), Z1
    VCMPPS  $0, Z0, Z1, K1
    
    VPMOVM2D K1, Z2
    VMOVUPS Z2, (DI)
    
    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $16, CX
    JMP     loop_f32_512

tail_f32_512:
    CMPQ    CX, $0
    JE      done_f32_512
tail_loop_f32_512:
    VMOVSS  (SI), X1
    UCOMISS X1, X0
    JP      no_match_f32_512
    JNE     no_match_f32_512
    MOVL    $-1, (DI)
    JMP     next_f32_512
no_match_f32_512:
    MOVL    $0, (DI)
next_f32_512:
    ADDQ    $4, SI
    ADDQ    $4, DI
    DECQ    CX
    JNZ     tail_loop_f32_512

done_f32_512:
    VZEROUPPER
    RET

// func fastPathInt64EqualAVX512Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)
TEXT ·fastPathInt64EqualAVX512Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    MOVQ    val+16(FP), DX
    MOVQ    result+24(FP), DI

    VPBROADCASTQ DX, Z0

loop_i64_512:
    CMPQ    CX, $8
    JL      tail_i64_512

    VMOVDQU64 (SI), Z1
    VPCMPQ  $0, Z0, Z1, K1
    
    VPMOVM2Q K1, Z2
    VMOVDQU64 Z2, (DI)
    
    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $8, CX
    JMP     loop_i64_512

tail_i64_512:
    CMPQ    CX, $0
    JE      done_i64_512
tail_loop_i64_512:
    MOVQ    (SI), R8
    CMPQ    R8, DX
    JNE     no_match_i64_512
    MOVQ    $-1, (DI)
    JMP     next_i64_512
no_match_i64_512:
    MOVQ    $0, (DI)
next_i64_512:
    ADDQ    $8, SI
    ADDQ    $8, DI
    DECQ    CX
    JNZ     tail_loop_i64_512

done_i64_512:
    VZEROUPPER
    RET

// func fastPathFloat64EqualAVX512Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)
TEXT ·fastPathFloat64EqualAVX512Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    VMOVSD  val+16(FP), X0
    MOVQ    result+24(FP), DI

    VBROADCASTSD X0, Z0

loop_f64_512:
    CMPQ    CX, $8
    JL      tail_f64_512

    VMOVUPS (SI), Z1
    VCMPPD  $0, Z0, Z1, K1
    
    VPMOVM2Q K1, Z2
    VMOVUPS Z2, (DI)
    
    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $8, CX
    JMP     loop_f64_512

tail_f64_512:
    CMPQ    CX, $0
    JE      done_f64_512
tail_loop_f64_512:
    VMOVSD  (SI), X1
    UCOMISD X1, X0
    JP      no_match_f64_512
    JNE     no_match_f64_512
    MOVQ    $-1, (DI)
    JMP     next_f64_512
no_match_f64_512:
    MOVQ    $0, (DI)
next_f64_512:
    ADDQ    $8, SI
    ADDQ    $8, DI
    DECQ    CX
    JNZ     tail_loop_f64_512

done_f64_512:
    VZEROUPPER
    RET
