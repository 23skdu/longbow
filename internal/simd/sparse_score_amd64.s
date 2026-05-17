//go:build amd64

#include "textflag.h"

// bm25ScoreBatchAVX512(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
TEXT ·bm25ScoreBatchAVX512(SB), NOSPLIT, $0-48
    MOVQ    tfs+0(FP), R8
    MOVQ    docLens+8(FP), R9
    MOVQ    n+16(FP), R10
    
    // Broadcast scalars to ZMM registers
    VBROADCASTSS invAvgDL+24(FP), Z0
    VBROADCASTSS idf+28(FP), Z1
    VBROADCASTSS k1+32(FP), Z2
    VBROADCASTSS b+36(FP), Z3
    MOVQ    results+40(FP), R11

    TESTQ   R10, R10
    JZ      done_avx512

    // pre-compute constants
    // Y4 = 1.0
    MOVL    $0x3f800000, R12
    MOVQ    R12, X4
    VBROADCASTSS X4, Y4
    
    // Y5 = 1.0 - b
    VSUBPS  Y3, Y4, Y5
    // Y6 = k1 + 1.0
    VADDPS  Y2, Y4, Y6
    // Y7 = idf * (k1 + 1.0)
    VMULPS  Y1, Y6, Y7

loop_avx512_16x:
    CMPQ    R10, $16
    JL      tail_avx512

    // Load 16 TFs (int64)
    VMOVDQU64 (R8), Z8
    VMOVDQU64 64(R8), Z9
    
    // Pack 16x 64-bit int to 16x 32-bit int
    VPMOVQD Z8, Y14
    VPMOVQD Z9, Y15
    VINSERTI64X4 $1, Y15, Z14, Z14 // Z14 now has 16 Dwords
    VCVTDQ2PS Z14, Z8 // Z8 = TFs (float32 x 16)

    // Load 16 DocLens
    VMOVDQU64 (R9), Z9
    VMOVDQU64 64(R9), Z10
    VPMOVQD Z9, Y14
    VPMOVQD Z10, Y15
    VINSERTI64X4 $1, Y15, Z14, Z14
    VCVTDQ2PS Z14, Z9 // Z9 = DocLens (float32 x 16)

    // math on ZMM:
    // lengthNorm = (1.0 - b) + b * (docLen * invAvgDL)
    VMULPS  Z9, Z0, Z10 // docLen * invAvgDL
    VFMADD213PS Z5, Z3, Z10 // Z10 = Z10 * Z3 + Z5
    
    // denominator = tf + k1 * lengthNorm
    VMULPS  Z10, Z2, Z11 // k1 * lengthNorm
    VADDPS  Z8, Z11, Z11 // tf + k1 * lengthNorm

    // numerator = tf * Z7
    VMULPS  Z8, Z7, Z12

    // score = numerator / denominator
    VDIVPS  Z11, Z12, Z13

    VMOVUPS Z13, (R11)

    ADDQ    $128, R8
    ADDQ    $128, R9
    ADDQ    $64, R11
    SUBQ    $16, R10
    JMP     loop_avx512_16x

tail_avx512:
    TESTQ   R10, R10
    JZ      done_avx512

    MOVQ    (R8), R12 // TF
    MOVQ    (R9), R13 // DocLen
    VCVTSI2SSQ R12, X14, X14
    VCVTSI2SSQ R13, X15, X15

    VMULSS  X15, X0, X10
    VFMADD213SS X5, X3, X10
    
    VMULSS  X10, X2, X11
    VADDSS  X14, X11, X11

    VMULSS  X14, X7, X12

    VDIVSS  X11, X12, X13
    VMOVSS  X13, (R11)

    ADDQ    $8, R8
    ADDQ    $8, R9
    ADDQ    $4, R11
    DECQ    R10
    JMP     tail_avx512

done_avx512:
    VZEROUPPER
    RET

// bm25ScoreBatchAVX2(tfs, docLens unsafe.Pointer, n int, invAvgDL, idf, k1, b float32, results unsafe.Pointer)
TEXT ·bm25ScoreBatchAVX2(SB), NOSPLIT, $0-48
    MOVQ    tfs+0(FP), R8
    MOVQ    docLens+8(FP), R9
    MOVQ    n+16(FP), R10
    
    // Broadcast scalars to YMM registers
    VBROADCASTSS invAvgDL+24(FP), Y0
    VBROADCASTSS idf+28(FP), Y1
    VBROADCASTSS k1+32(FP), Y2
    VBROADCASTSS b+36(FP), Y3
    MOVQ    results+40(FP), R11

    TESTQ   R10, R10
    JZ      done_avx2

    // pre-compute constants
    MOVL    $0x3f800000, R12
    MOVQ    R12, X4
    VBROADCASTSS X4, Y4
    
    VSUBPS  Y3, Y4, Y5
    VADDPS  Y2, Y4, Y6
    VMULPS  Y1, Y6, Y7

loop_avx2_16x:
    CMPQ    R10, $16
    JL      tail_avx2

    // Block 1 (8 elements)
    VMOVDQU (R8), Y8; VMOVDQU 32(R8), Y9
    VPSHUFD $0x08, Y8, Y8; VPERMQ $0x58, Y8, Y8; VPSHUFD $0x08, Y9, Y9; VPERMQ $0x58, Y9, Y9; VINSERTI128 $1, X9, Y8, Y8; VCVTDQ2PS Y8, Y8
    VMOVDQU (R9), Y9; VMOVDQU 32(R9), Y10
    VPSHUFD $0x08, Y9, Y9; VPERMQ $0x58, Y9, Y9; VPSHUFD $0x08, Y10, Y10; VPERMQ $0x58, Y10, Y10; VINSERTI128 $1, X10, Y9, Y9; VCVTDQ2PS Y9, Y9

    VMULPS  Y9, Y0, Y10; VFMADD213PS Y5, Y3, Y10
    VMULPS  Y10, Y2, Y11; VADDPS Y8, Y11, Y11
    VMULPS  Y8, Y7, Y12; VDIVPS Y11, Y12, Y13
    VMOVUPS Y13, (R11)

    // Block 2 (8 elements)
    VMOVDQU 64(R8), Y8; VMOVDQU 96(R8), Y9
    VPSHUFD $0x08, Y8, Y8; VPERMQ $0x58, Y8, Y8; VPSHUFD $0x08, Y9, Y9; VPERMQ $0x58, Y9, Y9; VINSERTI128 $1, X9, Y8, Y8; VCVTDQ2PS Y8, Y8
    VMOVDQU 64(R9), Y9; VMOVDQU 96(R9), Y10
    VPSHUFD $0x08, Y9, Y9; VPERMQ $0x58, Y9, Y9; VPSHUFD $0x08, Y10, Y10; VPERMQ $0x58, Y10, Y10; VINSERTI128 $1, X10, Y9, Y9; VCVTDQ2PS Y9, Y9

    VMULPS  Y9, Y0, Y10; VFMADD213PS Y5, Y3, Y10
    VMULPS  Y10, Y2, Y11; VADDPS Y8, Y11, Y11
    VMULPS  Y8, Y7, Y12; VDIVPS Y11, Y12, Y13
    VMOVUPS Y13, 32(R11)

    ADDQ    $128, R8
    ADDQ    $128, R9
    ADDQ    $64, R11
    SUBQ    $16, R10
    JMP     loop_avx2_16x


tail_avx2:
    TESTQ   R10, R10
    JZ      done_avx2

    MOVQ    (R8), R12 // TF
    MOVQ    (R9), R13 // DocLen
    VCVTSI2SSQ R12, X14, X14
    VCVTSI2SSQ R13, X15, X15

    VMULSS  X15, X0, X10
    VFMADD213SS X5, X3, X10
    
    VMULSS  X10, X2, X11
    VADDSS  X14, X11, X11

    VMULSS  X14, X7, X12

    VDIVSS  X11, X12, X13
    VMOVSS  X13, (R11)

    ADDQ    $8, R8
    ADDQ    $8, R9
    ADDQ    $4, R11
    DECQ    R10
    JMP     tail_avx2

done_avx2:
    VZEROUPPER
    RET
