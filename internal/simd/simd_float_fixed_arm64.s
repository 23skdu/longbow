//go:build arm64

#include "textflag.h"
#include "simd_arm64_macros.h"

// ============================================================================
// FIXED-DIMENSION DOT KERNELS
// ============================================================================

// func dot128NEONKernel(a, b []float32) float32
TEXT ·dot128NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $8, R1   // 128/16 = 8 iterations

dot128_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot128_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot384NEONKernel(a, b []float32) float32
TEXT ·dot384NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $24, R1  // 384/16 = 24 iterations

dot384_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot384_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot768NEONKernel(a, b []float32) float32
TEXT ·dot768NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $48, R1  // 768/16 = 48 iterations

dot768_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot768_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot1024NEONKernel(a, b []float32) float32
TEXT ·dot1024NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $64, R1  // 1024/16 = 64 iterations

dot1024_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot1024_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot1536NEONKernel(a, b []float32) float32
TEXT ·dot1536NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $96, R1  // 1536/16 = 96 iterations

dot1536_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot1536_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func dot3072NEONKernel(a, b []float32) float32
TEXT ·dot3072NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $192, R1  // 3072/16 = 192 iterations

dot3072_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V9.S4, V5.S4, V1.S4
    VFMLA   V10.S4, V6.S4, V2.S4
    VFMLA   V11.S4, V7.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     dot3072_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// ============================================================================
// FIXED-DIMENSION L2SQUARED KERNELS
// ============================================================================

// func l2Squared128NEONKernel(a, b []float32) float32
TEXT ·l2Squared128NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $8, R1

l2sq128_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFSUB_V(8, 4, 12)
    
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VFSUB_V(9, 5, 13)
    VFMLA   V12.S4, V12.S4, V0.S4

    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VFSUB_V(10, 6, 14)
    VFMLA   V13.S4, V13.S4, V1.S4

    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]
    VFSUB_V(11, 7, 15)
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CBNZ    R1, l2sq128_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared384NEONKernel(a, b []float32) float32
TEXT ·l2Squared384NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $24, R1

l2sq384_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFSUB_V(8, 4, 12)
    VFSUB_V(9, 5, 13)
    VFSUB_V(10, 6, 14)
    VFSUB_V(11, 7, 15)

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq384_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared768NEONKernel(a, b []float32) float32
TEXT ·l2Squared768NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $48, R1

l2sq768_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFSUB_V(8, 4, 12)
    VFSUB_V(9, 5, 13)
    VFSUB_V(10, 6, 14)
    VFSUB_V(11, 7, 15)

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq768_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared1024NEONKernel(a, b []float32) float32
TEXT ·l2Squared1024NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $64, R1

l2sq1024_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFSUB_V(8, 4, 12)
    VFSUB_V(9, 5, 13)
    VFSUB_V(10, 6, 14)
    VFSUB_V(11, 7, 15)

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq1024_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared1536NEONKernel(a, b []float32) float32
TEXT ·l2Squared1536NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $96, R1

l2sq1536_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFSUB_V(8, 4, 12)
    VFSUB_V(9, 5, 13)
    VFSUB_V(10, 6, 14)
    VFSUB_V(11, 7, 15)

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq1536_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func l2Squared3072NEONKernel(a, b []float32) float32
TEXT ·l2Squared3072NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16

    MOVW    $192, R1

l2sq3072_loop:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V9.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V10.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V11.S4]

    VFSUB_V(8, 4, 12)
    VFSUB_V(9, 5, 13)
    VFSUB_V(10, 6, 14)
    VFSUB_V(11, 7, 15)

    VFMLA   V12.S4, V12.S4, V0.S4
    VFMLA   V13.S4, V13.S4, V1.S4
    VFMLA   V14.S4, V14.S4, V2.S4
    VFMLA   V15.S4, V15.S4, V3.S4

    SUB     $1, R1
    CMP     $0, R1
    BGT     l2sq3072_loop

    WORD    $0x4e21d400
    WORD    $0x4e22d400
    WORD    $0x4e23d400

    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

    FMOVS   F1, ret+48(FP)
    RET

// func expNEONKernel(src, dst unsafe.Pointer, n int)
