//go:build arm64 && !noasm

#include "textflag.h"

// func fastPathInt64EqualNEONKernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer)
TEXT ·fastPathInt64EqualNEONKernel(SB), NOSPLIT, $0-32
    MOVD src+0(FP), R0
    MOVD n+8(FP), R1
    MOVD val+16(FP), R2
    MOVD result+24(FP), R3

    // Broadcast val to v0.D[0], v0.D[1]
    VMOV R2, V0.D[0]
    VMOV R2, V0.D[1]

loop_2x:
    CMP $2, R1
    BLT tail
    VLD1.P 16(R0), [V1.D2]
    VCMEQ V0.D2, V1.D2, V2.D2
    VST1.P [V2.D2], 16(R3)
    SUB $2, R1
    B loop_2x

tail:
    CMP $0, R1
    BLE done
    MOVD (R0), R4
    CMP R2, R4
    BNE not_equal
    MOVD $-1, R5
    JMP store
not_equal:
    MOVD $0, R5
store:
    MOVD R5, (R3)
done:
    RET

// func fastPathInt32EqualNEONKernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer)
TEXT ·fastPathInt32EqualNEONKernel(SB), NOSPLIT, $0-32
    MOVD src+0(FP), R0
    MOVD n+8(FP), R1
    MOVW val+16(FP), R2
    MOVD result+24(FP), R3

    // Broadcast val to v0.S4
    VMOV R2, V0.S[0]
    VMOV R2, V0.S[1]
    VMOV R2, V0.S[2]
    VMOV R2, V0.S[3]

loop_4x:
    CMP $4, R1
    BLT tail_int32
    VLD1.P 16(R0), [V1.S4]
    VCMEQ V0.S4, V1.S4, V2.S4
    VST1.P [V2.S4], 16(R3)
    SUB $4, R1
    B loop_4x

tail_int32:
    CMP $0, R1
    BLE done_int32
    MOVW (R0), R4
    CMP R2, R4
    BNE not_equal_int32
    MOVW $-1, R5
    JMP store_int32
not_equal_int32:
    MOVW $0, R5
store_int32:
    MOVW R5, (R3)
    ADD $4, R0
    ADD $4, R3
    SUB $1, R1
    B tail_int32
done_int32:
    RET

// func fastPathFloat64EqualNEONKernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer)
TEXT ·fastPathFloat64EqualNEONKernel(SB), NOSPLIT, $0-32
    MOVD src+0(FP), R0
    MOVD n+8(FP), R1
    MOVD val+16(FP), R2
    MOVD result+24(FP), R3
    VMOV R2, V0.D[0]
    VMOV R2, V0.D[1]

loop_2x_f64:
    CMP $2, R1
    BLT tail_f64
    VLD1.P 16(R0), [V1.D2]
    VCMEQ V0.D2, V1.D2, V2.D2
    VST1.P [V2.D2], 16(R3)
    SUB $2, R1
    B loop_2x_f64

tail_f64:
    CMP $0, R1
    BLE done_f64
    FMOVD (R0), F1
    FCMPD F0, F1
    BNE not_equal_f64
    MOVD $-1, R5
    JMP store_f64
not_equal_f64:
    MOVD $0, R5
store_f64:
    MOVD R5, (R3)
done_f64:
    RET

// func fastPathFloat32EqualNEONKernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer)
TEXT ·fastPathFloat32EqualNEONKernel(SB), NOSPLIT, $0-32
    MOVD src+0(FP), R0
    MOVD n+8(FP), R1
    MOVW val+16(FP), R2
    MOVD result+24(FP), R3
    VMOV R2, V0.S[0]
    VMOV R2, V0.S[1]
    VMOV R2, V0.S[2]
    VMOV R2, V0.S[3]

loop_4x_f32:
    CMP $4, R1
    BLT tail_f32
    VLD1.P 16(R0), [V1.S4]
    VCMEQ V0.S4, V1.S4, V2.S4
    VST1.P [V2.S4], 16(R3)
    SUB $4, R1
    B loop_4x_f32

tail_f32:
    CMP $0, R1
    BLE done_f32
    FMOVS (R0), F1
    FCMPS F0, F1
    BNE not_equal_f32
    MOVW $-1, R5
    JMP store_f32
not_equal_f32:
    MOVW $0, R5
store_f32:
    MOVW R5, (R3)
    ADD $4, R0
    ADD $4, R3
    SUB $1, R1
    B tail_f32
done_f32:
    RET
