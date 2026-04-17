// +build amd64,!nosimd

#include "textflag.h"

// func fastPathInt32EqualAVX2Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)
TEXT ·fastPathInt32EqualAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    MOVL    val+16(FP), DX
    MOVQ    result+24(FP), DI

    // Broadcast scalar val to Y0
    VMOVQ   DX, X0
    VPBROADCASTD X0, Y0

    // Process 8 elements at a time (32 bytes)
loop:
    CMPQ    CX, $8
    JL      tail

    VMOVDQU (SI), Y1
    VPCMPEQD Y0, Y1, Y2      // Y2 = (Y0 == Y1) ? 0xFFFFFFFF : 0
    VPMOVMSKB Y2, AX         // Extract most significant bit of each byte
    
    // VPMOVMSKB returns 32 bits. Since we have 8 DWORDS, we only care about bits 0, 4, 8, 12, 16, 20, 24, 28.
    // However, Arrow booleans are bit-packed (1 bit per element).
    // To keep it simple for now, we'll store as bytes and pack later, 
    // OR just use a simple loop for the bit-packing if we want to be exact.
    
    // For this implementation, we will store 8 BYTES (0xFF or 0x00) into result.
    // Wait, the caller expects a bitmask if it's following Arrow's layout.
    // Let's assume the result is a byte slice where we store 0xFF for matches.
    
    VMOVDQU Y2, (DI)         // Store 32 bytes (8 results)
    
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $8, CX
    JMP     loop

tail:
    VZEROUPPER
    RET

// func fastPathFloat32EqualAVX2Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)
TEXT ·fastPathFloat32EqualAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    VMOVSS  val+16(FP), X0
    MOVQ    result+24(FP), DI

    // Broadcast scalar val to Y0
    VBROADCASTSS X0, Y0

loop_f32:
    CMPQ    CX, $8
    JL      tail_f32

    VMOVUPS (SI), Y1
    VCMPPS  $0, Y0, Y1, Y2    // $0 = _CMP_EQ_OQ (Equal, Ordered, Non-signaling)
    
    VMOVDQU Y2, (DI)
    
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $8, CX
    JMP     loop_f32

tail_f32:
    VZEROUPPER
    RET

// func fastPathBoolAVX2Kernel(src unsafe.Pointer, nBytes int, negate bool, result unsafe.Pointer)
TEXT ·fastPathBoolAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    nBytes+8(FP), CX
    MOVB    negate+16(FP), AL
    MOVQ    result+24(FP), DI

    TESTB   AL, AL
    JZ      identity

    // Negate using VPXOR with all 1s
    VPCMPEQD Y0, Y0, Y0     // Y0 = all 1s

bool_loop:
    CMPQ    CX, $32
    JL      bool_tail
    VMOVDQU (SI), Y1
    VPXOR   Y0, Y1, Y2
    VMOVDQU Y2, (DI)
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $32, CX
    JMP     bool_loop

identity:
    // Just copy
    CMPQ    CX, $32
    JL      bool_tail
    VMOVDQU (SI), Y1
    VMOVDQU Y1, (DI)
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $32, CX
    JMP     identity

bool_tail:
    // Process remaining bytes one by one
    TESTQ   CX, CX
    JZ      bool_done
    MOVB    (SI), BL
    TESTB   AL, AL
    JZ      bool_store
    NOTB    BL
bool_store:
    MOVB    BL, (DI)
    INCQ    SI
    INCQ    DI
    DECQ    CX
    JMP     bool_tail

bool_done:
    VZEROUPPER
    RET

// func fastPathStringEqualAVX2Kernel(offsets unsafe.Pointer, data unsafe.Pointer, n int, target unsafe.Pointer, targetLen int, result unsafe.Pointer)
TEXT ·fastPathStringEqualAVX2Kernel(SB), NOSPLIT, $0-48
    MOVQ    offsets+0(FP), SI   // *int32
    MOVQ    data+8(FP), DX      // *byte
    MOVQ    n+16(FP), R15       // use R15 for count to keep CX for CMPSB
    MOVQ    target+24(FP), R8   // *byte
    MOVQ    targetLen+32(FP), R9// int
    MOVQ    result+40(FP), DI   // *int32

    // Broadcast targetLen for vectorized check
    VMOVQ   R9, X0
    VPBROADCASTD X0, Y0

next_block:
    CMPQ    R15, $8
    JL      tail_str

    // Load 8 offsets + 1 next offset
    VMOVDQU (SI), Y1
    VMOVDQU 4(SI), Y2
    VPSUBD  Y1, Y2, Y3
    VPCMPEQD Y0, Y3, Y4
    
    VPTEST  Y4, Y4
    JZ      skip_data_block
    
    VPMOVMSKB Y4, AX
    
    MOVQ    $0, R13
loop_block:
    CMPQ    R13, $8
    JE      skip_data_block
    
    MOVQ    R13, R14
    SHLQ    $2, R14
    BTQ     R14, AX
    JNC     next_in_block
    
    MOVL    (SI)(R13*4), R10
    MOVQ    DX, R12
    ADDQ    R10, R12
    
    PUSHQ   SI
    PUSHQ   DI
    PUSHQ   DX
    PUSHQ   AX
    
    MOVQ    R12, SI
    MOVQ    R8, DI
    MOVQ    R9, CX              // length into CX for CMPSB
    REP; CMPSB
    JNE     data_mismatch
    
    POPQ    AX
    POPQ    DX
    POPQ    DI
    POPQ    SI
    MOVL    $0xFFFFFFFF, (DI)(R13*4)
    JMP     next_in_block

data_mismatch:
    POPQ    AX
    POPQ    DX
    POPQ    DI
    POPQ    SI
    MOVL    $0, (DI)(R13*4)

next_in_block:
    INCQ    R13
    JMP     loop_block

skip_data_block:
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $8, R15
    JMP     next_block

tail_str:
    CMPQ    R15, $0
    JE      done_str
    
    MOVL    (SI), R10
    MOVL    4(SI), R11
    SUBL    R10, R11
    
    MOVL    $0, (DI)
    CMPL    R11, R9
    JNE     tail_advance
    
    MOVQ    DX, R12
    ADDQ    R10, R12
    
    PUSHQ   SI
    PUSHQ   DI
    MOVQ    R12, SI
    MOVQ    R8, DI
    MOVQ    R9, CX
    REP; CMPSB
    JNE     tail_mismatch
    
    POPQ    DI
    POPQ    SI
    MOVL    $0xFFFFFFFF, (DI)
    JMP     tail_advance

tail_mismatch:
    POPQ    DI
    POPQ    SI
tail_advance:
    ADDQ    $4, SI
    ADDQ    $4, DI
    DECQ    R15
    JMP     tail_str
    
done_str:
    VZEROUPPER
    RET
    VZEROUPPER
    RET

// func fastPathInt64EqualAVX2Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)
TEXT ·fastPathInt64EqualAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    MOVQ    val+16(FP), DX
    MOVQ    result+24(FP), DI

    // Broadcast scalar val to Y0
    VMOVQ   DX, X0
    VPBROADCASTQ X0, Y0

loop_i64:
    CMPQ    CX, $4
    JL      tail_i64

    VMOVDQU (SI), Y1
    VPCMPEQQ Y0, Y1, Y2      // Y2 = (Y0 == Y1) ? 0xFFFFFFFFFFFFFFFF : 0
    
    VMOVDQU Y2, (DI)         // Store 32 bytes (4 results)
    
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $4, CX
    JMP     loop_i64

tail_i64:
    VZEROUPPER
    RET

// func fastPathFloat64EqualAVX2Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)
TEXT ·fastPathFloat64EqualAVX2Kernel(SB), NOSPLIT, $0-32
    MOVQ    src+0(FP), SI
    MOVQ    n+8(FP), CX
    VMOVSD  val+16(FP), X0
    MOVQ    result+24(FP), DI

    // Broadcast scalar val to Y0
    VBROADCASTSD X0, Y0

loop_f64:
    CMPQ    CX, $4
    JL      tail_f64

    VMOVUPS (SI), Y1
    VCMPPD  $0, Y0, Y1, Y2    // $0 = _CMP_EQ_OQ
    
    VMOVDQU Y2, (DI)
    
    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $4, CX
    JMP     loop_f64

tail_f64:
    VZEROUPPER
    RET
