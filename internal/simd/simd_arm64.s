//go:build arm64

#include "textflag.h"

#define VFADD_V(m, n, d) WORD $(0x4e20d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFSUB_V(m, n, d) WORD $(0x4ea0d400 | ((m) << 16) | ((n) << 5) | (d))
#define VFMUL_V(m, n, d) WORD $(0x6e20dc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFMLA_V(m, n, d) WORD $(0x4e20cc00 | ((m) << 16) | ((n) << 5) | (d))
#define VFNEG_V(n, d)    WORD $(0x6ea1f800 | ((n) << 5) | (d))
#define VFCVTZS_V(n, d)  WORD $(0x4e21b800 | ((n) << 5) | (d))
#define VSCVTF_V(n, d)   WORD $(0x4e21d800 | ((n) << 5) | (d))
#define VFRINTM_V(n, d)  WORD $(0x4e214000 | ((n) << 5) | (d))
#define VFRECPE_V(n, d)  WORD $(0x4e21d000 | ((n) << 5) | (d))
#define VFRECPS_V(m, n, d) WORD $(0x4e20fc00 | ((m) << 16) | ((n) << 5) | (d))

// func euclideanNEONKernel(a, b []float32) float32
TEXT ·euclideanNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    FMOVS   $0.0, F1
    MOVD    $0, R3

    CMP     $4, R1
    BLT     tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x4ea2d423

    // Accumulate diff^2: V0 += V3 * V3
    VFMLA   V3.S4, V3.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

tail_loop:
    CBZ     R1, done
    
    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FSUBS   F2, F1, F3
    FMULS   F3, F3, F3
    FADDS   F3, F0, F0
    
    SUB     $1, R1
    B       tail_loop

done:
    FSQRTS  F0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func euclideanHighDimNEONKernel(a, b []float32) float32
TEXT ·euclideanHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V16.B16, V16.B16, V16.B16
    VEOR    V17.B16, V17.B16, V17.B16
    VEOR    V18.B16, V18.B16, V18.B16
    VEOR    V19.B16, V19.B16, V19.B16

    CMP     $32, R1
    BLT     hd_tail_loop

hd_loop_32x:
    // Interleave 8 streams of (load, sub, fmla)
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V13.S4]
    VFSUB_V(12, 4, 4)
    VFSUB_V(13, 5, 5)

    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V15.S4]
    VFSUB_V(14, 6, 6)
    VFSUB_V(15, 7, 7)

    VFMLA   V4.S4, V4.S4, V0.S4
    VFMLA   V5.S4, V5.S4, V1.S4
    VFMLA   V6.S4, V6.S4, V2.S4
    VFMLA   V7.S4, V7.S4, V3.S4

    VLD1.P  16(R0), [V8.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V9.S4]
    VLD1.P  16(R2), [V13.S4]
    VFSUB_V(12, 8, 8)
    VFSUB_V(13, 9, 9)

    VLD1.P  16(R0), [V10.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V11.S4]
    VLD1.P  16(R2), [V15.S4]
    VFSUB_V(14, 10, 10)
    VFSUB_V(15, 11, 11)

    VFMLA   V8.S4, V8.S4, V16.S4
    VFMLA   V9.S4, V9.S4, V17.S4
    VFMLA   V10.S4, V10.S4, V18.S4
    VFMLA   V11.S4, V11.S4, V19.S4

    SUB     $32, R1
    CMP     $32, R1
    BGE     hd_loop_32x

    // Sum accumulators
    VFADD_V(16, 0, 0)
    VFADD_V(17, 1, 1)
    VFADD_V(18, 2, 2)
    VFADD_V(19, 3, 3)
    
    VFADD_V(1, 0, 0)
    VFADD_V(3, 2, 2)
    VFADD_V(2, 0, 0)

hd_tail_loop:
    CMP     $4, R1
    BLT     hd_scalar_reduction

hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFSUB_V(8, 4, 12)
    VFMLA   V12.S4, V12.S4, V0.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     hd_tail_4x

hd_scalar_reduction:
    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1     // F1 = F1 + F0 (accumulate V0[0])
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

hd_scalar_tail:
    CBZ     R1, hd_done
    
    FMOVS.P 4(R0), F5
    FMOVS.P 4(R2), F6
    
    FSUBS   F6, F5, F7
    FMULS   F7, F7, F7
    FADDS   F7, F1, F1
    
    SUB     $1, R1
    B       hd_scalar_tail

hd_done:
    FSQRTS  F1, F1
    FMOVS   F1, ret+48(FP)
    RET

// func dotNEONKernel(a, b []float32) float32
TEXT ·dotNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $4, R1
    BLT     dot_tail

dot_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]
    
    VFMLA   V2.S4, V1.S4, V0.S4
    
    SUB     $4, R1
    CMP     $4, R1
    BGE     dot_loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

dot_tail:
    CBZ     R1, dot_done

    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FMULS   F2, F1, F3
    FADDS   F3, F0, F0

    SUB     $1, R1
    B       dot_tail

dot_done:
    FMOVS   F0, ret+48(FP)
    RET

// func dotHighDimNEONKernel(a, b []float32) float32
TEXT ·dotHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V16.B16, V16.B16, V16.B16
    VEOR    V17.B16, V17.B16, V17.B16
    VEOR    V18.B16, V18.B16, V18.B16
    VEOR    V19.B16, V19.B16, V19.B16

    CMP     $32, R1
    BLT     dot_hd_tail_loop

dot_hd_loop_32x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R2), [V13.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V7.S4]
    VLD1.P  16(R2), [V15.S4]

    VFMLA   V12.S4, V4.S4, V0.S4
    VFMLA   V13.S4, V5.S4, V1.S4
    VFMLA   V14.S4, V6.S4, V2.S4
    VFMLA   V15.S4, V7.S4, V3.S4

    VLD1.P  16(R0), [V8.S4]
    VLD1.P  16(R2), [V12.S4]
    VLD1.P  16(R0), [V9.S4]
    VLD1.P  16(R2), [V13.S4]
    VLD1.P  16(R0), [V10.S4]
    VLD1.P  16(R2), [V14.S4]
    VLD1.P  16(R0), [V11.S4]
    VLD1.P  16(R2), [V15.S4]

    VFMLA   V12.S4, V8.S4, V16.S4
    VFMLA   V13.S4, V9.S4, V17.S4
    VFMLA   V14.S4, V10.S4, V18.S4
    VFMLA   V15.S4, V11.S4, V19.S4

    SUB     $32, R1
    CMP     $32, R1
    BGE     dot_hd_loop_32x

    // Final sum of 8 accumulators
    VFADD_V(16, 0, 0)
    VFADD_V(17, 1, 1)
    VFADD_V(18, 2, 2)
    VFADD_V(19, 3, 3)
    
    VFADD_V(1, 0, 0)
    VFADD_V(3, 2, 2)
    VFADD_V(2, 0, 0)

dot_hd_tail_loop:
    CMP     $4, R1
    BLT     dot_hd_scalar_reduction

dot_hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFMLA   V8.S4, V4.S4, V0.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     dot_hd_tail_4x

dot_hd_scalar_reduction:
    FMOVS   $0.0, F1
    VMOV    V0.S[1], V2.S[0]
    VMOV    V0.S[2], V3.S[0]
    VMOV    V0.S[3], V4.S[0]
    FADDS   F0, F1, F1
    FADDS   F2, F1, F1
    FADDS   F3, F1, F1
    FADDS   F4, F1, F1

dot_hd_scalar_tail:
    CBZ     R1, dot_hd_done
    
    FMOVS.P 4(R0), F2
    FMOVS.P 4(R2), F3
    
    FMULS   F3, F2, F4
    FADDS   F4, F1, F1
    
    SUB     $1, R1
    B       dot_hd_scalar_tail

dot_hd_done:
    FMOVS   F1, ret+48(FP)
    RET


// func l2SquaredNEONKernel(a, b []float32) float32
TEXT ·l2SquaredNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    FMOVS   $0.0, F1
    MOVD    $0, R3

    CMP     $4, R1
    BLT     l2_tail_loop

    VEOR    V0.B16, V0.B16, V0.B16

l2_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    // FSUB V1.4S, V2.4S, V3.4S (V3 = V1 - V2)
    WORD    $0x4ea2d423

    // Accumulate diff^2: V0 += V3 * V3
    VFMLA   V3.S4, V3.S4, V0.S4

    SUB     $4, R1
    CMP     $4, R1
    BGE     l2_loop_4x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

l2_tail_loop:
    CBZ     R1, l2_done
    
    FMOVS.P 4(R0), F1
    FMOVS.P 4(R2), F2
    
    FSUBS   F2, F1, F3
    FMULS   F3, F3, F3
    FADDS   F3, F0, F0
    
    SUB     $1, R1
    B       l2_tail_loop

l2_done:
    // No FSQRTS for L2 Squared
    FMOVS   F0, ret+48(FP)
    RET

// ============================================================================
// FP16 OPTIMIZED IMPLEMENTATIONS
// ============================================================================

// func euclideanF16NEONKernel(a, b []float16.Num) float32
TEXT ·euclideanF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator

    CMP     $8, R1
    BLT     euc_f16_tail

euc_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8] // Load 8x FP16 (128 bits)
    VLD1.P  16(R2), [V2.H8] // Load 8x FP16 (128 bits)

    // Convert lower 4
    WORD    $0x0e217823
    WORD    $0x0e217844

    VFSUB_V(4, 3, 5)
    VFMLA   V5.S4, V5.S4, V0.S4

    // Convert upper 4
    WORD    $0x4e217823
    WORD    $0x4e217844

    VFSUB_V(4, 3, 5)
    VFMLA   V5.S4, V5.S4, V0.S4

    SUB     $8, R1
    CMP     $8, R1
    BGE     euc_f16_loop_8x

    // Final reduction of V0.S4 into F0
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

euc_f16_tail:
    CBZ     R1, euc_f16_done
    
    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    
    // Convert R3, R4 to float32
    WORD    $0x0e217823 
    WORD    $0x0e217844
    
    FSUBS   F4, F3, F5
    FMULS   F5, F5, F5
    FADDS   F5, F0, F0
    
    SUB     $1, R1
    B       euc_f16_tail

euc_f16_done:
    FSQRTS  F0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func dotF16NEONKernel(a, b []float16.Num) float32
TEXT ·dotF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16

    CMP     $8, R1
    BLT     dot_f16_tail

dot_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8]
    VLD1.P  16(R2), [V2.H8]

    // Lower 4
    WORD    $0x0e217823
    WORD    $0x0e217844
    VFMLA   V4.S4, V3.S4, V0.S4

    // Upper 4
    WORD    $0x4e217823
    WORD    $0x4e217844
    VFMLA   V4.S4, V3.S4, V0.S4

    SUB     $8, R1
    CMP     $8, R1
    BGE     dot_f16_loop_8x

    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

dot_f16_tail:
    CBZ     R1, dot_f16_done

    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VMOV    R3, V1.H[0]
    VMOV    R4, V2.H[0]
    
    WORD    $0x0e217823
    WORD    $0x0e217844
    
    FMULS   F4, F3, F5
    FADDS   F5, F0, F0

    SUB     $1, R1
    B       dot_f16_tail

dot_f16_done:
    FMOVS   F0, ret+48(FP)
    RET

// func cosineF16NEONKernel(a, b []float16.Num) float32
TEXT ·cosineF16NEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    // Accumulators
    VEOR    V0.B16, V0.B16, V0.B16 // Dot
    VEOR    V10.B16, V10.B16, V10.B16 // NormA
    VEOR    V11.B16, V11.B16, V11.B16 // NormB
    
    // Initialize scalar accumulators for tail case (Dim < 8)
    FMOVS   $0.0, F1
    FMOVS   $0.0, F2

    CMP     $8, R1
    BLT     cos_f16_tail

cos_f16_loop_8x:
    VLD1.P  16(R0), [V1.H8]
    VLD1.P  16(R2), [V2.H8]

    // --- Lower 4 ---
    WORD    $0x0e217823
    WORD    $0x0e217844
    
    VFMLA   V4.S4, V3.S4, V0.S4   // Dot += A * B
    VFMLA   V3.S4, V3.S4, V10.S4  // NormA += A * A
    VFMLA   V4.S4, V4.S4, V11.S4  // NormB += B * B

    // --- Upper 4 ---
    WORD    $0x4e217823
    WORD    $0x4e217844

    VFMLA   V4.S4, V3.S4, V0.S4   // Dot += A * B
    VFMLA   V3.S4, V3.S4, V10.S4  // NormA += A * A
    VFMLA   V4.S4, V4.S4, V11.S4  // NormB += B * B

    SUB     $8, R1
    CMP     $8, R1
    BGE     cos_f16_loop_8x

    // Reduction for Dot (V0)
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

    // Reduction for NormA (V10)
    VMOV    V10.S[0], V20.S[0] // Copy to V20/F20
    VMOV    V10.S[1], V21.S[0]
    VMOV    V10.S[2], V22.S[0]
    VMOV    V10.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F1 // Store in F1 for tail calc

    // Reduction for NormB (V11)
    VMOV    V11.S[0], V24.S[0]
    VMOV    V11.S[1], V25.S[0]
    VMOV    V11.S[2], V26.S[0]
    VMOV    V11.S[3], V27.S[0]
    FADDS   F25, F24, F24
    FADDS   F26, F24, F24
    FADDS   F27, F24, F24
    FMOVS   F24, F2 // Store in F2 for tail calc

cos_f16_tail:
    CBZ     R1, cos_f16_calc

    MOVHU.P 2(R0), R3
    MOVHU.P 2(R2), R4
    
    VEOR    V5.B16, V5.B16, V5.B16 // Use V5 as temp
    VEOR    V6.B16, V6.B16, V6.B16 // Use V6 as temp
    VMOV    R3, V5.H[0]
    VMOV    R4, V6.H[0]
    
    // Convert V5, V6 to float32 (V3, V4)
    WORD    $0x0e2178a3 // FCVT S3, H5
    WORD    $0x0e2178c4 // FCVT S4, H6
    
    // Dot
    FMULS   F4, F3, F5
    FADDS   F5, F0, F0
    
    // NormA
    FMULS   F3, F3, F3
    FADDS   F3, F1, F1
    
    // NormB
    FMULS   F4, F4, F4
    FADDS   F4, F2, F2

    SUB     $1, R1
    B       cos_f16_tail

cos_f16_calc:
    FMOVS   $0.0, F3
    FCMPS   F1, F3
    BEQ     ret_one
    FCMPS   F2, F3
    BEQ     ret_one

    FSQRTS  F1, F1
    FSQRTS  F2, F2
    FMULS   F2, F1, F3
    FDIVS   F3, F0, F0
    
    FMOVS   $1.0, F4
    FSUBS   F0, F4, F0
    FMOVS   F0, ret+48(FP)
    RET

ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func fastWalshHadamardTransform32NEONKernel(a []float32)
TEXT ·fastWalshHadamardTransform32NEONKernel(SB), NOSPLIT, $0-24
    MOVD    a_base+0(FP), R0
    
    // Load 32 floats into V0-V7 (128 bytes total)
    VLD1.P  16(R0), [V0.S4]
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R0), [V2.S4]
    VLD1.P  16(R0), [V3.S4]
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R0), [V5.S4]
    VLD1.P  16(R0), [V6.S4]
    VLD1.P  16(R0), [V7.S4]
    
    // Stage 1 (h=16): Butterflies (V0,V4), (V1,V5), (V2,V6), (V3,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0) // V0 = V0 + V4
    VFSUB_V(4, 16, 4) // V4 = V16 - V4
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1) 
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3) 
    VFSUB_V(7, 16, 7)

    // Stage 2 (h=8): (V0,V2), (V1,V3), (V4,V6), (V5,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(2, 0, 0)
    VFSUB_V(2, 16, 2)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(3, 1, 1)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(6, 4, 4)
    VFSUB_V(6, 16, 6)
    
    VORR    V5.B16, V5.B16, V16.B16
    VFADD_V(7, 5, 5)
    VFSUB_V(7, 16, 7)

    // Stage 3 (h=4): (V0,V1), (V2,V3), (V4,V5), (V6,V7)
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 16, 1)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(3, 2, 2)
    VFSUB_V(3, 16, 3)
    
    VORR    V4.B16, V4.B16, V16.B16
    VFADD_V(5, 4, 4)
    VFSUB_V(5, 16, 5)
    
    VORR    V6.B16, V6.B16, V16.B16
    VFADD_V(7, 6, 6)
    VFSUB_V(7, 16, 7)

    // Stage 4 (h=2): Use VEXT and intra-register butterfly
    VEXT    $8, V0.B16, V0.B16, V8.B16
    VFADD_V(8, 0, 9)
    VFSUB_V(8, 0, 10)
    VMOV    V9.D[0], V0.D[0]
    VMOV    V10.D[0], V0.D[1]
    
    VEXT    $8, V1.B16, V1.B16, V8.B16
    VFADD_V(8, 1, 9)
    VFSUB_V(8, 1, 10)
    VMOV    V9.D[0], V1.D[0]
    VMOV    V10.D[0], V1.D[1]
    
    VEXT    $8, V2.B16, V2.B16, V8.B16
    VFADD_V(8, 2, 9)
    VFSUB_V(8, 2, 10)
    VMOV    V9.D[0], V2.D[0]
    VMOV    V10.D[0], V2.D[1]
    
    VEXT    $8, V3.B16, V3.B16, V8.B16
    VFADD_V(8, 3, 9)
    VFSUB_V(8, 3, 10)
    VMOV    V9.D[0], V3.D[0]
    VMOV    V10.D[0], V3.D[1]
    
    VEXT    $8, V4.B16, V4.B16, V8.B16
    VFADD_V(8, 4, 9)
    VFSUB_V(8, 4, 10)
    VMOV    V9.D[0], V4.D[0]
    VMOV    V10.D[0], V4.D[1]
    
    VEXT    $8, V5.B16, V5.B16, V8.B16
    VFADD_V(8, 5, 9)
    VFSUB_V(8, 5, 10)
    VMOV    V9.D[0], V5.D[0]
    VMOV    V10.D[0], V5.D[1]
    
    VEXT    $8, V6.B16, V6.B16, V8.B16
    VFADD_V(8, 6, 9)
    VFSUB_V(8, 6, 10)
    VMOV    V9.D[0], V6.D[0]
    VMOV    V10.D[0], V6.D[1]
    
    VEXT    $8, V7.B16, V7.B16, V8.B16
    VFADD_V(8, 7, 9)
    VFSUB_V(8, 7, 10)
    VMOV    V9.D[0], V7.D[0]
    VMOV    V10.D[0], V7.D[1]

    // Stage 5 (h=1): Use VREV64 and VMOV
    VREV64  V0.S4, V11.S4
    VFADD_V(11, 0, 12)
    VFSUB_V(11, 0, 8) 
    VMOV    V12.S[0], V0.S[0]
    VMOV    V8.S[0], V0.S[1]
    VMOV    V12.S[2], V0.S[2]
    VMOV    V8.S[2], V0.S[3]
    
    VREV64  V1.S4, V11.S4
    VFADD_V(11, 1, 12)
    VFSUB_V(11, 1, 8) 
    VMOV    V12.S[0], V1.S[0]
    VMOV    V8.S[0], V1.S[1]
    VMOV    V12.S[2], V1.S[2]
    VMOV    V8.S[2], V1.S[3]

    VREV64  V2.S4, V11.S4
    VFADD_V(11, 2, 12)
    VFSUB_V(11, 2, 13)
    VMOV    V12.S[0], V2.S[0]
    VMOV    V13.S[0], V2.S[1]
    VMOV    V12.S[2], V2.S[2]
    VMOV    V13.S[2], V2.S[3]
    
    VREV64  V3.S4, V11.S4
    VFADD_V(11, 3, 12)
    VFSUB_V(11, 3, 13)
    VMOV    V12.S[0], V3.S[0]
    VMOV    V13.S[0], V3.S[1]
    VMOV    V12.S[2], V3.S[2]
    VMOV    V13.S[2], V3.S[3]

    VREV64  V4.S4, V11.S4
    VFADD_V(11, 4, 12)
    VFSUB_V(11, 4, 13)
    VMOV    V12.S[0], V4.S[0]
    VMOV    V13.S[0], V4.S[1]
    VMOV    V12.S[2], V4.S[2]
    VMOV    V13.S[2], V4.S[3]

    VREV64  V5.S4, V11.S4
    VFADD_V(11, 5, 12)
    VFSUB_V(11, 5, 13)
    VMOV    V12.S[0], V5.S[0]
    VMOV    V13.S[0], V5.S[1]
    VMOV    V12.S[2], V5.S[2]
    VMOV    V13.S[2], V5.S[3]

    VREV64  V6.S4, V11.S4
    VFADD_V(11, 6, 12)
    VFSUB_V(11, 6, 13)
    VMOV    V12.S[0], V6.S[0]
    VMOV    V13.S[0], V6.S[1]
    VMOV    V12.S[2], V6.S[2]
    VMOV    V13.S[2], V6.S[3]

    VREV64  V7.S4, V11.S4
    VFADD_V(11, 7, 12)
    VFSUB_V(11, 7, 13)
    VMOV    V12.S[0], V7.S[0]
    VMOV    V13.S[0], V7.S[1]
    VMOV    V12.S[2], V7.S[2]
    VMOV    V13.S[2], V7.S[3]

    // Store 32 floats back to memory
    MOVD    a_base+0(FP), R0
    VST1.P  [V0.S4], 16(R0)
    VST1.P  [V1.S4], 16(R0)
    VST1.P  [V2.S4], 16(R0)
    VST1.P  [V3.S4], 16(R0)
    VST1.P  [V4.S4], 16(R0)
    VST1.P  [V5.S4], 16(R0)
    VST1.P  [V6.S4], 16(R0)
    VST1.P  [V7.S4], 16(R0)
    RET

// func vectorButterflyNEONKernel(a, b []float32)
TEXT ·vectorButterflyNEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Individual loads
    VLD1    (R0), [V0.S4]
    VLD1    (R1), [V1.S4]
    
    VORR    V0.B16, V0.B16, V2.B16
    VFADD_V(1, 0, 0)
    VFSUB_V(1, 2, 1)
    
    VST1    [V0.S4], (R0)
    VST1    [V1.S4], (R1)
    RET

// func vectorButterfly16NEONKernel(a, b []float32)
TEXT ·vectorButterfly16NEONKernel(SB), NOSPLIT, $0-48
    MOVD    a_base+0(FP), R0
    MOVD    b_base+24(FP), R1
    
    // Load 16 floats (4x 128-bit registers) from a and b
    VLD1    (R0), [V0.S4, V1.S4, V2.S4, V3.S4]
    VLD1    (R1), [V4.S4, V5.S4, V6.S4, V7.S4]
    
    // Butterfly operations: a' = a + b, b' = a - b
    VORR    V0.B16, V0.B16, V16.B16
    VFADD_V(4, 0, 0)
    VFSUB_V(4, 16, 4)
    
    VORR    V1.B16, V1.B16, V16.B16
    VFADD_V(5, 1, 1)
    VFSUB_V(5, 16, 5)
    
    VORR    V2.B16, V2.B16, V16.B16
    VFADD_V(6, 2, 2)
    VFSUB_V(6, 16, 6)
    
    VORR    V3.B16, V3.B16, V16.B16
    VFADD_V(7, 3, 3)
    VFSUB_V(7, 16, 7)
    
    // Store results back
    VST1    [V0.S4, V1.S4, V2.S4, V3.S4], (R0)
    VST1    [V4.S4, V5.S4, V6.S4, V7.S4], (R1)
    RET

// func randomSignFlipNEONKernel(a []float32, seed int64)
TEXT ·randomSignFlipNEONKernel(SB), NOSPLIT, $0-32
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    seed+24(FP), R2

    MOVD    $0x5851f42d4c957f2d, R3 // Constant

    CMP     $4, R1
    BLT     sign_flip_tail

sign_flip_loop:
    // Element 0
    MUL     R3, R2, R8      // h = seed * c
    LSR     $63, R8, R8     // h >> 63
    LSL     $31, R8, R8     // mask = (h >> 63) << 31
    ADD     $1, R2, R5      // next_seed = seed + 1

    // Element 1
    MUL     R3, R5, R9
    LSR     $63, R9, R9
    LSL     $31, R9, R9
    ADD     $2, R2, R6

    // Element 2
    MUL     R3, R6, R10
    LSR     $63, R10, R10
    LSL     $31, R10, R10
    ADD     $3, R2, R7

    // Element 3
    MUL     R3, R7, R11
    LSR     $63, R11, R11
    LSL     $31, R11, R11

    // Pack into V1.S4
    VMOV    R8, V1.S[0]
    VMOV    R9, V1.S[1]
    VMOV    R10, V1.S[2]
    VMOV    R11, V1.S[3]

    // Load 4 floats
    VLD1    (R0), [V0.S4]

    // XOR with sign mask
    VEOR    V1.B16, V0.B16, V0.B16

    // Store back and increment pointer
    VST1.P  [V0.S4], 16(R0)

    ADD     $4, R2          // seed += 4
    SUB     $4, R1          // len -= 4
    CMP     $4, R1
    BGE     sign_flip_loop

sign_flip_tail:
    CBZ     R1, sign_flip_done
    
    // Tail case for 1-3 elements
    MUL     R3, R2, R8
    LSR     $63, R8, R8
    LSL     $31, R8, R8
    
    FMOVS.P 4(R0), F0
    VMOV    R8, V1.S[0]
    VEOR    V1.B16, V0.B16, V0.B16
    FMOVS   F0, -4(R0)
    
    ADD     $1, R2
    SUB     $1, R1
    B       sign_flip_tail

sign_flip_done:
    RET

// ============================================================================
// FLOAT32 COSINE KERNELS
// ============================================================================

// func cosineNEONKernel(a, b []float32) float32
TEXT ·cosineNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16  // Dot accumulator
    VEOR    V10.B16, V10.B16, V10.B16 // NormA accumulator
    VEOR    V11.B16, V11.B16, V11.B16 // NormB accumulator

    FMOVS   $0.0, F0             // Init accumulators
    FMOVS   $0.0, F4
    FMOVS   $0.0, F5

    CMP     $4, R1
    BLT     cos_tail

cos_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    VLD1.P  16(R2), [V2.S4]

    VFMLA   V2.S4, V1.S4, V0.S4     // Dot += A * B
    VFMLA   V1.S4, V1.S4, V10.S4   // NormA += A * A
    VFMLA   V2.S4, V2.S4, V11.S4   // NormB += B * B

    SUB     $4, R1
    CMP     $4, R1
    BGE     cos_loop_4x

    // Reduction for Dot (V0) - accumulate 4 lanes to F0
    VMOV    V0.S[1], V4.S[0]
    VMOV    V0.S[2], V5.S[0]
    VMOV    V0.S[3], V6.S[0]
    FADDS   F4, F0, F0     // F0 += other lanes
    FADDS   F5, F0, F0
    FADDS   F6, F0, F0
    // F0 now has total dot

    // Reduction for NormA to F4
    VMOV    V10.S[1], V5.S[0]
    VMOV    V10.S[2], V6.S[0]
    VMOV    V10.S[3], V7.S[0]
    FMOVS   F10, F4
    FADDS   F5, F4, F4
    FADDS   F6, F4, F4
    FADDS   F7, F4, F4
    // F4 now has normA (move V0 result first)

    // Reduction for NormB to F5
    VMOV    V11.S[1], V6.S[0]
    VMOV    V11.S[2], V7.S[0]
    VMOV    V11.S[3], V8.S[0]
    FMOVS   F11, F5
    FADDS   F6, F5, F5
    FADDS   F7, F5, F5
    FADDS   F8, F5, F5
    // F5 now has normB

cos_tail:
    CBZ     R1, cos_calc

    FMOVS.P 4(R0), F10
    FMOVS.P 4(R2), F11

    FMULS   F11, F10, F12        // a*b
    FADDS   F12, F0, F0         // Add to dot (F0)

    FMULS   F10, F10, F10       // a*a
    FADDS   F10, F4, F4        // Add to normA (F4)

    FMULS   F11, F11, F11      // b*b
    FADDS   F11, F5, F5       // Add to normB (F5)

    SUB     $1, R1
    B       cos_tail

cos_calc:
    FMOVS   $0.0, F6
    FCMPS   F4, F6           // Check normA != 0
    BEQ     cos_ret_one
    FCMPS   F5, F6           // Check normB != 0
    BEQ     cos_ret_one

    FSQRTS  F4, F4           // sqrt(normA)
    FSQRTS  F5, F5           // sqrt(normB)
    FMULS   F5, F4, F4       // denominator = normA * normB

    FDIVS   F4, F0, F0       // cosine_dist = dot / denominator

    FMOVS   $1.0, F6
    FSUBS   F0, F6, F0       // 1 - cosine_dist

    FMOVS   F0, ret+48(FP)
    RET

cos_ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET

// func cosineHighDimNEONKernel(a, b []float32) float32
TEXT ·cosineHighDimNEONKernel(SB), NOSPLIT, $0-52
    MOVD    a_base+0(FP), R0
    MOVD    a_len+8(FP), R1
    MOVD    b_base+24(FP), R2

    VEOR    V0.B16, V0.B16, V0.B16
    VEOR    V1.B16, V1.B16, V1.B16
    VEOR    V2.B16, V2.B16, V2.B16
    VEOR    V3.B16, V3.B16, V3.B16
    VEOR    V10.B16, V10.B16, V10.B16
    VEOR    V11.B16, V11.B16, V11.B16
    VEOR    V12.B16, V12.B16, V12.B16
    VEOR    V13.B16, V13.B16, V13.B16

    CMP     $16, R1
    BLT     cos_hd_tail_loop

cos_hd_loop_16x:
    WORD    $0xf8804000  // PLDL1KEEP [R0, #128]
    WORD    $0xf8804040  // PLDL1KEEP [R2, #128]

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

    VFMLA   V4.S4, V4.S4, V10.S4
    VFMLA   V5.S4, V5.S4, V11.S4
    VFMLA   V6.S4, V6.S4, V12.S4
    VFMLA   V7.S4, V7.S4, V13.S4

    VFMLA   V8.S4, V8.S4, V10.S4
    VFMLA   V9.S4, V9.S4, V11.S4
    VFMLA   V10.S4, V10.S4, V12.S4
    VFMLA   V11.S4, V11.S4, V13.S4

    SUB     $16, R1
    CMP     $16, R1
    BGE     cos_hd_loop_16x

    // Reduction
    WORD    $0x4e21d400  // FADD V1.4S, V0.4S, V0.4S
    WORD    $0x4e22d400  // FADD V2.4S, V0.4S, V0.4S
    WORD    $0x4e23d400  // FADD V3.4S, V0.4S, V0.4S

    FMOVS   $0.0, F20
    VMOV    V10.S[1], V21.S[0]
    VMOV    V10.S[2], V22.S[0]
    VMOV    V10.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F2

    FMOVS   $0.0, F20
    VMOV    V11.S[1], V21.S[0]
    VMOV    V11.S[2], V22.S[0]
    VMOV    V11.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F3

    FMOVS   $0.0, F20
    VMOV    V12.S[1], V21.S[0]
    VMOV    V12.S[2], V22.S[0]
    VMOV    V12.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F4

    FMOVS   $0.0, F20
    VMOV    V13.S[1], V21.S[0]
    VMOV    V13.S[2], V22.S[0]
    VMOV    V13.S[3], V23.S[0]
    FADDS   F21, F20, F20
    FADDS   F22, F20, F20
    FADDS   F23, F20, F20
    FMOVS   F20, F5

    FADDS   F4, F2, F2
    FADDS   F5, F3, F3
    FMOVS   F2, F1
    FMOVS   F3, F2

cos_hd_tail_loop:
    CMP     $4, R1
    BLT     cos_hd_scalar_reduction

cos_hd_tail_4x:
    VLD1.P  16(R0), [V4.S4]
    VLD1.P  16(R2), [V8.S4]
    VFMLA   V8.S4, V4.S4, V0.S4
    VFMLA   V4.S4, V4.S4, V10.S4
    VFMLA   V8.S4, V8.S4, V10.S4
    SUB     $4, R1
    CMP     $4, R1
    BGE     cos_hd_tail_4x

cos_hd_scalar_reduction:
    FMOVS   $0.0, F1
    FMOVS   $0.0, F2
    FMOVS   $0.0, F3
    VMOV    V0.S[1], V4.S[0]
    VMOV    V0.S[2], V5.S[0]
    VMOV    V0.S[3], V6.S[0]
    FADDS   F0, F1, F1
    FADDS   F4, F1, F1
    FADDS   F5, F1, F1
    FADDS   F6, F1, F1
    FMOVS   F1, F0
    VMOV    V10.S[1], V4.S[0]
    VMOV    V10.S[2], V5.S[0]
    VMOV    V10.S[3], V6.S[0]
    FADDS   F4, F2, F2
    FADDS   F5, F2, F2
    FADDS   F6, F2, F2
    VMOV    V11.S[1], V4.S[0]
    VMOV    V11.S[2], V5.S[0]
    VMOV    V11.S[3], V6.S[0]
    FADDS   F4, F3, F3
    FADDS   F5, F3, F3
    FADDS   F6, F3, F3

cos_hd_scalar_tail:
    CBZ     R1, cos_hd_calc

    FMOVS.P 4(R0), F4
    FMOVS.P 4(R2), F5

    FMULS   F5, F4, F6
    FADDS   F6, F0, F0
    FMULS   F4, F4, F4
    FADDS   F4, F2, F2
    FMULS   F5, F5, F5
    FADDS   F5, F3, F3

    SUB     $1, R1
    B       cos_hd_scalar_tail

cos_hd_calc:
    FMOVS   $0.0, F4
    FCMPS   F2, F4
    BEQ     cos_hd_ret_one
    FCMPS   F3, F4
    BEQ     cos_hd_ret_one

    FSQRTS  F2, F2
    FSQRTS  F3, F3
    FMULS   F3, F2, F2
    FDIVS   F2, F0, F0

    FMOVS   $1.0, F4
    FSUBS   F0, F4, F0

    FMOVS   F0, ret+48(FP)
    RET

cos_hd_ret_one:
    FMOVS   $1.0, F0
    FMOVS   F0, ret+48(FP)
    RET

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
TEXT ·expNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    CBZ     R2, exp_done

    MOVW    $127, R3            // bias

    MOVW    $0x3fb8aa3b, R4     // log2(e)
    VMOV    R4, V31.S[0]
    VDUP    V31.S[0], V31.S4
    
    MOVW    $0x3f000000, R4     // 0.5
    VMOV    R4, V30.S[0]
    VDUP    V30.S[0], V30.S4
    
    MOVW    $0x3f800000, R4     // 1.0 (c0)
    VMOV    R4, V28.S[0]
    VDUP    V28.S[0], V28.S4
    
    MOVW    $0x3f317218, R4     // 0.69314718 (c1)
    VMOV    R4, V27.S[0]
    VDUP    V27.S[0], V27.S4
    
    MOVW    $127, R3            // Exponent bias
    
    MOVW    $0x3e762d61, R4     // 0.240226507 (c2)
    VMOV    R4, V26.S[0]
    VDUP    V26.S[0], V26.S4
    
    MOVW    $0x3d6359a4, R4     // 0.0555041086 (c3)
    VMOV    R4, V25.S[0]
    VDUP    V25.S[0], V25.S4
    
    MOVW    $0x3c1d9551, R4     // 0.009618129 (c4)
    VMOV    R4, V24.S[0]
    VDUP    V24.S[0], V24.S4
    
    MOVW    $0x3aafd05d, R4     // 0.00134204 (c5)
    VMOV    R4, V23.S[0]
    VDUP    V23.S[0], V23.S4

exp_loop:
    CBZ     R2, exp_done
    FMOVS.P 4(R0), F0
    
    // z = x * log2(e)
    FMULS   F31, F0, F1
    FADDS   F30, F1, F2
    FRINTMS F2, F2               // n = floor(z + 0.5)
    FSUBS   F2, F1, F3           // f = z - n
    
    // Poly for 2^f
    FMOVS   $0.00134204, F4
    FMULS   F3, F4, F4
    FADDS   F24, F4, F4
    FMULS   F3, F4, F4
    FADDS   F25, F4, F4
    FMULS   F3, F4, F4
    FADDS   F26, F4, F4
    FMULS   F3, F4, F4
    FADDS   F27, F4, F4
    FMULS   F3, F4, F4
    FADDS   F28, F4, F4          // 2^f
    
    FCVTZSS F2, R4               // n as int
    ADD     $127, R4, R4         // n + 127
    LSL     $23, R4, R4          // shift to exponent
    VMOV    R4, V5.S[0]          // bits of 2^n
    FMULS   F4, F5, F0           // exp(x) = 2^f * 2^n
    
    FMOVS.P F0, 4(R1)
    SUB     $1, R2
    B       exp_loop

exp_tail:
    CBZ     R2, exp_done
    FMOVS.P 4(R0), F0
    
    FMULS   F31, F0, F1          // z = x * log2(e)
    FADDS   F30, F1, F2
    FRINTMS F2, F2               // n = floor(z + 0.5)
    FSUBS   F2, F1, F3           // f = z - n
    
    // Polynomial for 2^f
    FMOVS   $0.00134204, F4
    FMULS   F3, F4, F4
    FADDS   F24, F4, F4
    FMULS   F3, F4, F4
    FADDS   F25, F4, F4
    FMULS   F3, F4, F4
    FADDS   F26, F4, F4
    FMULS   F3, F4, F4
    FADDS   F27, F4, F4
    FMULS   F3, F4, F4
    FADDS   F28, F4, F4          // 2^f
    
    FCVTZSS F2, R4               // n as int
    ADD     $127, R4, R4         // n + 127
    LSL     $23, R4, R4          // shift to exponent
    VMOV    R4, V5.S[0]          // 2^n as float bit pattern
    FMULS   F4, F5, F0           // exp(x) = 2^f * 2^n
    
    FMOVS.P F0, 4(R1)
    SUB     $1, R2
    B       exp_tail

exp_done:
    RET

// func sigmoidNEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·sigmoidNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    CBZ     R2, sigmoid_done
    
    FMOVS   $1.44269504, F31    // log2(e)
    FMOVS   $0.5, F30           // half
    MOVW    $127, R3            // bias
    FMOVS   $1.0, F29           // 1.0
    
    // Poly constants
    MOVW    $0x3f317218, R4     // 0.69314718 (c1)
    VMOV    R4, V27.S[0]
    MOVW    $0x3e762d61, R4     // 0.240226507 (c2)
    VMOV    R4, V26.S[0]
    MOVW    $0x3d6359a4, R4     // 0.0555041086 (c3)
    VMOV    R4, V25.S[0]
    MOVW    $0x3c1d9551, R4     // 0.009618129 (c4)
    VMOV    R4, V24.S[0]
    MOVW    $0x3f800000, R4     // 1.0 (c0)
    VMOV    R4, V28.S[0]

    VDUP    V31.S[0], V31.S4
    VDUP    V30.S[0], V30.S4
    VDUP    V29.S[0], V29.S4

sigmoid_loop:
    CBZ     R2, sigmoid_done
    FMOVS.P 4(R0), F0
    FNEGS   F0, F0               // x_neg = -x
    
    // exp(x_neg)
    FMULS   F31, F0, F1          // z = x_neg * log2(e)
    FADDS   F30, F1, F2
    FRINTMS F2, F2               // n = floor(z + 0.5)
    FSUBS   F2, F1, F3           // f = z - n
    
    // Poly for 2^f
    FMOVS   $0.00134204, F4
    FMULS   F3, F4, F4
    FADDS   F24, F4, F4
    FMULS   F3, F4, F4
    FADDS   F25, F4, F4
    FMULS   F3, F4, F4
    FADDS   F26, F4, F4
    FMULS   F3, F4, F4
    FADDS   F27, F4, F4
    FMULS   F3, F4, F4
    FADDS   F28, F4, F4          // 2^f
    
    FCVTZSS F2, R4               // n as int
    ADD     $127, R4, R4         // n + 127
    LSL     $23, R4, R4          // shift to exponent
    VMOV    R4, V5.S[0]          // bits of 2^n
    FMULS   F4, F5, F0           // exp(-x) = 2^f * 2^n
    
    FMOVS   $1.0, F1
    FADDS   F1, F0, F0           // 1 + exp(-x)
    FDIVS   F0, F1, F0           // 1 / (1 + exp(-x))
    
    FMOVS.P F0, 4(R1)
    SUB     $1, R2
    B       sigmoid_loop

sigmoid_tail:
    CBZ     R2, sigmoid_done
    FMOVS.P 4(R0), F0
    FNEGS   F0, F0               // -x
    
    // exp(-x) approximation (scalar)
    FMOVS   $1.0, F1
    FMOVS   $0.5, F2
    FMOVS   $0.166666, F3
    FMULS   F0, F3, F4
    FADDS   F2, F4, F4
    FMULS   F0, F4, F4
    FADDS   F1, F4, F4
    FMULS   F0, F4, F4
    FADDS   F1, F4, F0           // exp(-x)
    
    FADDS   F1, F0, F0           // 1 + exp(-x)
    FDIVS   F0, F1, F0           // 1 / (1 + exp(-x))
    
    FMOVS.P F0, 4(R1)
    SUB     $1, R2
    B       sigmoid_tail

sigmoid_done:
    RET

// func logNEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·logNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2

    CBZ     R2, log_done
    
    // Constants
    FMOVS   $0.69314718, F31     // ln(2)
    MOVW    $0x007fffff, R4      // mantissa mask
    MOVW    $127, R5             // bias
    
    VDUP    V31.S[0], V31.S4

log_loop:
    CMP     $4, R2
    BLT     log_tail
    
    VLD1.P  16(R0), [V0.S4]      // V0 = x
    
    // Extract exponent n
    VMOV    V0.B16, V1.B16
    VUSHR   $23, V1.S4, V1.S4    // V1 = x >> 23
    MOVW    $0xFF, R6
    VMOV    R6, V10.S[0]
    VDUP    V10.S[0], V10.S4
    VAND    V10.B16, V1.B16, V1.B16  // V1 = (x >> 23) & 0xFF (raw exponent)
    
    VMOV    R5, V11.S[0]         // 127
    VDUP    V11.S[0], V11.S4
    VSUB    V11.S4, V1.S4, V2.S4  // V2 = n = raw_exp - 127
    VSCVTF_V(2, 2)               // V2 = n as float
    
    // Extract mantissa f in [1, 2)
    VMOV    R4, V12.S[0]         // 0x007fffff
    VDUP    V12.S[0], V12.S4
    VAND    V12.B16, V0.B16, V3.B16  // V3 = mantissa bits
    
    MOVW    $0x3f800000, R6      // bits of 1.0
    VMOV    R6, V13.S[0]
    VDUP    V13.S[0], V13.S4
    VORR    V13.B16, V3.B16, V3.B16  // V3 = f in [1, 2)
    
    // log(f) approx (f-1) * (a0 + (f-1)*(a1 + ...))
    FMOVS   $1.0, F20
    VDUP    V20.S[0], V20.S4
    VFSUB_V(20, 3, 4)            // V4 = m = f - 1
    
    // Polynomial for log(1+m) on [0, 1]
    FMOVS   $0.99999642, F21     // a1
    FMOVS   $-0.49987412, F22    // a2
    FMOVS   $0.33179904, F23     // a3
    FMOVS   $-0.2407338, F24     // a4
    FMOVS   $0.16765407, F25     // a5
    FMOVS   $-0.09532939, F26    // a6

    VDUP    V21.S[0], V21.S4
    VDUP    V22.S[0], V22.S4
    VDUP    V23.S[0], V23.S4
    VDUP    V24.S[0], V24.S4
    VDUP    V25.S[0], V25.S4
    VDUP    V26.S[0], V26.S4
    
    VFMUL_V(26, 4, 5)            // V5 = a6 * m
    VFADD_V(25, 5, 5)            // V5 = a6 * m + a5
    VFMUL_V(5, 4, 5)             // V5 = (a6 * m + a5) * m
    VFADD_V(24, 5, 5)            // V5 = (a6 * m + a5) * m + a4
    VFMUL_V(5, 4, 5)             // V5 = ((a6 * m + a5) * m + a4) * m
    VFADD_V(23, 5, 5)            // V5 = ... + a3
    VFMUL_V(5, 4, 5)             // V5 = ... * m
    VFADD_V(22, 5, 5)            // V5 = ... + a2
    VFMUL_V(5, 4, 5)             // V5 = ... * m
    VFADD_V(21, 5, 5)            // V5 = ... + a1
    VFMUL_V(4, 5, 5)             // V5 = log(f)
    // Wait, log(1+m) = m * (a1 + m*(a2 + ...))
    
    // log(x) = n*ln(2) + log(f)
    VFMLA_V(31, 2, 5)
    
    VST1.P  [V5.S4], 16(R1)
    
    SUB     $4, R2
    B       log_loop

log_tail:
    CBZ     R2, log_done
    FMOVS.P 4(R0), F0
    
    FMOVS   F0, R6
    LSR     $23, R6, R7
    AND     $0xFF, R7, R7        // R7 = raw_exp
    SUB     $127, R7, R7         // R7 = n
    SCVTFS  R7, F2               // F2 = n as float
    
    AND     R4, R6, R6           // R6 = mantissa bits
    MOVW    $0x3f800000, R8
    ORR     R8, R6, R6
    VMOV    R6, V10.S[0]
    FMOVS   F10, F3              // F3 = f in [1, 2)
    
    FMOVS   $1.0, F4
    FSUBS   F4, F3, F4          // F4 = m = f - 1
    
    FMOVS   $-0.09532939, F5     // a6
    FMULS   F4, F5, F5
    FADDS   F25, F5, F5
    FMULS   F4, F5, F5
    FADDS   F24, F5, F5
    FMULS   F4, F5, F5
    FADDS   F23, F5, F5
    FMULS   F4, F5, F5
    FADDS   F22, F5, F5
    FMULS   F4, F5, F5
    FADDS   F21, F5, F5
    FMULS   F4, F5, F5           // F5 = log(f)
    
    FMOVS   $0.69314718, F6
    FMULS   F2, F6, F6          // n*ln(2)
    FADDS   F6, F5, F0           // log(x)
    
    FMOVS.P F0, 4(R1)
    SUB     $1, R2
    B       log_tail

log_done:
    RET

// func softmaxNEONKernel(src, dst unsafe.Pointer, n int)
TEXT ·softmaxNEONKernel(SB), NOSPLIT, $0-24
    MOVD    src+0(FP), R0
    MOVD    dst+8(FP), R1
    MOVD    n+16(FP), R2
    
    CBZ     R2, softmax_done
    
    // 1. Find Max
    MOVD    R0, R3
    MOVD    R2, R4
    MOVW    $0xff7fffff, R5
    VMOV    R5, V0.S[0]
softmax_max_loop:
    CBZ     R4, softmax_max_done
    FMOVS.P 4(R3), F1
    FMAXS   F1, F0, F0
    SUB     $1, R4
    B       softmax_max_loop
softmax_max_done:
    
    // 2. Compute Exp(x - max) and Sum
    MOVD    R0, R3
    MOVD    R1, R5
    MOVD    R2, R4
    FMOVS   $0.0, F1             // Sum
softmax_exp_loop:
    CBZ     R4, softmax_exp_done
    FMOVS.P 4(R3), F2
    FSUBS   F0, F2, F2           // x - max
    
    // Inline Exp (simple polynomial for testing)
    // e^x approx 1 + x + 0.5x^2 + 0.166x^3
    FMOVS   $1.0, F3
    FMOVS   $0.5, F4
    FMOVS   $0.166666, F5
    FMULS   F2, F5, F6           // 0.166x
    FADDS   F4, F6, F6           // 0.5 + 0.166x
    FMULS   F2, F6, F6           // 0.5x + 0.166x^2
    FADDS   F3, F6, F6           // 1 + 0.5x + 0.166x^2
    FMULS   F2, F6, F6           // x + 0.5x^2 + 0.166x^3
    FADDS   F3, F6, F2           // 1 + x + 0.5x^2 + 0.166x^3
    
    // For large negative values, result should be 0
    FMOVS   $0.0, F3
    FMAXS   F3, F2, F2
    
    FMOVS.P F2, 4(R5)
    FADDS   F2, F1, F1
    SUB     $1, R4
    B       softmax_exp_loop
softmax_exp_done:

    // 3. Divide by Sum
    MOVD    R1, R5
    MOVD    R2, R4
softmax_div_loop:
    CBZ     R4, softmax_done
    FMOVS   (R5), F2
    FDIVS   F1, F2, F2
    FMOVS.P F2, 4(R5)
    SUB     $1, R4
    B       softmax_div_loop
softmax_done:
    RET

// func sumNEONKernel(src unsafe.Pointer, n int) float32
TEXT ·sumNEONKernel(SB), NOSPLIT, $0-20
    MOVD    src+0(FP), R0
    MOVD    n+8(FP), R1
    
    FMOVS   $0.0, F0
    VEOR    V0.B16, V0.B16, V0.B16
    
    CMP     $4, R1
    BLT     sum_tail
    
sum_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    // FADD V1.4S, V0.4S, V0.4S
    WORD    $0x4e21d400
    SUB     $4, R1
    CMP     $4, R1
    BGE     sum_loop_4x
    
    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FADDS   F1, F0, F0
    FADDS   F2, F0, F0
    FADDS   F3, F0, F0

sum_tail:
    CBZ     R1, sum_done
    FMOVS.P 4(R0), F1
    FADDS   F1, F0, F0
    SUB     $1, R1
    B       sum_tail

sum_done:
    FMOVS   F0, ret+16(FP)
    RET

// func maxNEONKernel(src unsafe.Pointer, n int) float32
TEXT ·maxNEONKernel(SB), NOSPLIT, $0-20
    MOVD    src+0(FP), R0
    MOVD    n+8(FP), R1
    
    MOVW    $0xff7fffff, R2
    VMOV    R2, V0.S[0]
    VDUP    V0.S[0], V0.S4
    
    CMP     $4, R1
    BLT     max_tail
    
max_loop_4x:
    VLD1.P  16(R0), [V1.S4]
    // FMAX V1.4S, V0.4S, V0.4S
    WORD    $0x4e21f400
    SUB     $4, R1
    CMP     $4, R1
    BGE     max_loop_4x
    
    // Reduction
    VMOV    V0.S[1], V1.S[0]
    VMOV    V0.S[2], V2.S[0]
    VMOV    V0.S[3], V3.S[0]
    FMAXS   F1, F0, F0
    FMAXS   F2, F0, F0
    FMAXS   F3, F0, F0

max_tail:
    CBZ     R1, max_done
    FMOVS.P 4(R0), F1
    FMAXS   F1, F0, F0
    SUB     $1, R1
    B       max_tail

max_done:
    FMOVS   F0, ret+16(FP)
    RET

// func minNEONKernel(src unsafe.Pointer, n int) float32
TEXT ·minNEONKernel(SB), NOSPLIT, $0-20
    MOVD    src+0(FP), R0
    MOVD    n+8(FP), R1
    
    MOVW    $0x7f7fffff, R2
    VMOV    R2, V0.S[0]
    
min_loop:
    CBZ     R1, min_done
    FMOVS.P 4(R0), F1
    FMINS   F1, F0, F0
    SUB     $1, R1
    B       min_loop

min_done:
    FMOVS   F0, ret+16(FP)
    RET
    RET

#define VCMEQ_S4(m, n, d) WORD $(0x4ea08c00 | ((m) << 16) | ((n) << 5) | (d))
#define VCMGT_S4(m, n, d) WORD $(0x4ea03400 | ((m) << 16) | ((n) << 5) | (d))
#define VCMGE_S4(m, n, d) WORD $(0x6ea03c00 | ((m) << 16) | ((n) << 5) | (d))
#define VCMEQ_D2(m, n, d) WORD $(0x4ee08c00 | ((m) << 16) | ((n) << 5) | (d))
#define VCMGT_D2(m, n, d) WORD $(0x4ee03400 | ((m) << 16) | ((n) << 5) | (d))
#define VCMGE_D2(m, n, d) WORD $(0x6ee03c00 | ((m) << 16) | ((n) << 5) | (d))

#define VFCMEQ_S4(m, n, d) WORD $(0x4e20e400 | ((m) << 16) | ((n) << 5) | (d))
#define VFCMGT_S4(m, n, d) WORD $(0x6e20e400 | ((m) << 16) | ((n) << 5) | (d))
#define VFCMGE_S4(m, n, d) WORD $(0x6e20ec00 | ((m) << 16) | ((n) << 5) | (d))
#define VFCMEQ_D2(m, n, d) WORD $(0x4e60e400 | ((m) << 16) | ((n) << 5) | (d))
#define VFCMGT_D2(m, n, d) WORD $(0x6e60e400 | ((m) << 16) | ((n) << 5) | (d))
#define VFCMGE_D2(m, n, d) WORD $(0x6e60ec00 | ((m) << 16) | ((n) << 5) | (d))

// func matchInt32NeonKernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int)
TEXT ·matchInt32NeonKernel(SB), NOSPLIT, $0-40
    MOVD    src+0(FP), R0
    MOVW    val+8(FP), R1
    MOVD    op+16(FP), R2
    MOVD    dst+24(FP), R3
    MOVD    n+32(FP), R4
    CBZ     R4, match32_done
    VDUP    R1, V0.S4
    MOVD    $1, R5
    VDUP    R5, V1.B16
    MOVD    $-1, R6
    VDUP    R6, V7.B16
loop32:
    CMP     $4, R4
    BLT     tail32
    VLD1.P  16(R0), [V2.S4]
    CMP     $0, R2; BEQ eq32; CMP $1, R2; BEQ neq32; CMP $2, R2; BEQ gt32; CMP $3, R2; BEQ ge32; CMP $4, R2; BEQ lt32; CMP $5, R2; BEQ le32; B eq32
eq32:  VCMEQ_S4(0, 2, 3); B store32
neq32: VCMEQ_S4(0, 2, 3); VEOR V7.B16, V3.B16, V3.B16; B store32
gt32:  VCMGT_S4(0, 2, 3); B store32
ge32:  VCMGE_S4(0, 2, 3); B store32
lt32:  VCMGT_S4(2, 0, 3); B store32
le32:  VCMGE_S4(2, 0, 3); B store32
store32:
    VAND    V1.B16, V3.B16, V3.B16
    VMOV    V3.S[0], R6; MOVB R6, (R3)
    VMOV    V3.S[1], R6; MOVB R6, 1(R3)
    VMOV    V3.S[2], R6; MOVB R6, 2(R3)
    VMOV    V3.S[3], R6; MOVB R6, 3(R3)
    ADD     $4, R3; SUB     $4, R4; B       loop32
tail32:
    CBZ     R4, match32_done
    MOVW.P  4(R0), R5
    CMP     $0, R2; BEQ t_eq32; CMP $1, R2; BEQ t_neq32; CMP $2, R2; BEQ t_gt32; CMP $3, R2; BEQ t_ge32; CMP $4, R2; BEQ t_lt32; CMP $5, R2; BEQ t_le32; B t_eq32
t_eq32:  CMP R1, R5; CSET EQ, R6; B t_store32
t_neq32: CMP R1, R5; CSET NE, R6; B t_store32
t_gt32:  CMP R5, R1; CSET GT, R6; B t_store32
t_ge32:  CMP R5, R1; CSET GE, R6; B t_store32
t_lt32:  CMP R1, R5; CSET GT, R6; B t_store32
t_le32:  CMP R1, R5; CSET GE, R6; B t_store32
t_store32:
    MOVB    R6, (R3); ADD $1, R3; SUB $1, R4; B tail32
match32_done: RET

// func matchInt64NeonKernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int)
TEXT ·matchInt64NeonKernel(SB), NOSPLIT, $0-40
    MOVD    src+0(FP), R0; MOVD val+8(FP), R1; MOVD op+16(FP), R2; MOVD dst+24(FP), R3; MOVD n+32(FP), R4
    CBZ     R4, match64_done
    VDUP    R1, V0.D2; MOVD $1, R5; VDUP R5, V1.B16; MOVD $-1, R6; VDUP R6, V7.B16
loop64:
    CMP     $2, R4; BLT tail64; VLD1.P 16(R0), [V2.D2]
    CMP $0, R2; BEQ eq64; CMP $1, R2; BEQ neq64; CMP $2, R2; BEQ gt64; CMP $3, R2; BEQ ge64; CMP $4, R2; BEQ lt64; CMP $5, R2; BEQ le64; B eq64
eq64:  VCMEQ_D2(0, 2, 3); B store64
neq64: VCMEQ_D2(0, 2, 3); VEOR V7.B16, V3.B16, V3.B16; B store64
gt64:  VCMGT_D2(2, 0, 3); B store64
ge64:  VCMGE_D2(2, 0, 3); B store64
lt64:  VCMGT_D2(0, 2, 3); B store64
le64:  VCMGE_D2(0, 2, 3); B store64
store64:
    VAND    V1.B16, V3.B16, V3.B16
    VMOV    V3.D[0], R6; MOVB R6, (R3); VMOV V3.D[1], R6; MOVB R6, 1(R3); ADD $2, R3; SUB $2, R4; B loop64
tail64:
    CBZ R4, match64_done; MOVD.P 8(R0), R5
    CMP $0, R2; BEQ t_eq64; CMP $1, R2; BEQ t_neq64; CMP $2, R2; BEQ t_gt64; CMP $3, R2; BEQ t_ge64; CMP $4, R2; BEQ t_lt64; CMP $5, R2; BEQ t_le64; B t_eq64
t_eq64: CMP R1, R5; CSET EQ, R6; B t_store64
t_neq64: CMP R1, R5; CSET NE, R6; B t_store64
t_gt64: CMP R5, R1; CSET GT, R6; B t_store64
t_ge64: CMP R5, R1; CSET GE, R6; B t_store64
t_lt64: CMP R1, R5; CSET GT, R6; B t_store64
t_le64: CMP R1, R5; CSET GE, R6; B t_store64
t_store64: MOVB R6, (R3); ADD $1, R3; SUB $1, R4; B tail64
match64_done: RET

// func matchFloat32NeonKernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int)
TEXT ·matchFloat32NeonKernel(SB), NOSPLIT, $0-40
    MOVD src+0(FP), R0; FMOVS val+8(FP), F0; MOVD op+16(FP), R2; MOVD dst+24(FP), R3; MOVD n+32(FP), R4
    CBZ R4, matchf32_done; VDUP V0.S[0], V0.S4; MOVD $1, R5; VDUP R5, V1.B16; MOVD $-1, R6; VDUP R6, V7.B16
loopf32:
    CMP $4, R4; BLT tailf32; VLD1.P 16(R0), [V2.S4]
    CMP $0, R2; BEQ eqf32; CMP $1, R2; BEQ neqf32; CMP $2, R2; BEQ gtf32; CMP $3, R2; BEQ gef32; CMP $4, R2; BEQ ltf32; CMP $5, R2; BEQ lef32; B eqf32
eqf32: VFCMEQ_S4(0, 2, 3); B storef32
neqf32: VFCMEQ_S4(0, 2, 3); VEOR V7.B16, V3.B16, V3.B16; B storef32
gtf32: VFCMGT_S4(2, 0, 3); B storef32
gef32: VFCMGE_S4(2, 0, 3); B storef32
ltf32: VFCMGT_S4(0, 2, 3); B storef32
lef32: VFCMGE_S4(0, 2, 3); B storef32
storef32:
    VAND V1.B16, V3.B16, V3.B16
    VMOV V3.S[0], R6; MOVB R6, (R3); VMOV V3.S[1], R6; MOVB R6, 1(R3); VMOV V3.S[2], R6; MOVB R6, 2(R3); VMOV V3.S[3], R6; MOVB R6, 3(R3); ADD $4, R3; SUB $4, R4; B loopf32
tailf32:
    CBZ R4, matchf32_done; FMOVS.P 4(R0), F1
    CMP $0, R2; BEQ t_eqf32; CMP $1, R2; BEQ t_neqf32; CMP $2, R2; BEQ t_gtf32; CMP $3, R2; BEQ t_gef32; CMP $4, R2; BEQ t_ltf32; CMP $5, R2; BEQ t_lef32; B t_eqf32
t_eqf32: FCMPS F0, F1; CSET EQ, R6; B t_storef32
t_neqf32: FCMPS F0, F1; CSET NE, R6; B t_storef32
t_gtf32: FCMPS F1, F0; CSET LT, R6; B t_storef32
t_gef32: FCMPS F1, F0; CSET LE, R6; B t_storef32
t_ltf32: FCMPS F1, F0; CSET GT, R6; B t_storef32
t_lef32: FCMPS F1, F0; CSET GE, R6; B t_storef32
t_storef32: MOVB R6, (R3); ADD $1, R3; SUB $1, R4; B tailf32
matchf32_done: RET

// func matchFloat64NeonKernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int)
TEXT ·matchFloat64NeonKernel(SB), NOSPLIT, $0-40
    MOVD src+0(FP), R0; FMOVD val+8(FP), F0; MOVD op+16(FP), R2; MOVD dst+24(FP), R3; MOVD n+32(FP), R4
    CBZ R4, matchf64_done; VDUP V0.D[0], V0.D2; MOVD $1, R5; VDUP R5, V1.B16; MOVD $-1, R6; VDUP R6, V7.B16
loopf64:
    CMP $2, R4; BLT tailf64; VLD1.P 16(R0), [V2.D2]
    CMP $0, R2; BEQ eqf64; CMP $1, R2; BEQ neqf64; CMP $2, R2; BEQ gtf64; CMP $3, R2; BEQ gef64; CMP $4, R2; BEQ ltf64; CMP $5, R2; BEQ lef64; B eqf64
eqf64: VFCMEQ_D2(0, 2, 3); B storef64
neqf64: VFCMEQ_D2(0, 2, 3); VEOR V7.B16, V3.B16, V3.B16; B storef64
gtf64: VFCMGT_D2(2, 0, 3); B storef64
gef64: VFCMGE_D2(2, 0, 3); B storef64
ltf64: VFCMGT_D2(0, 2, 3); B storef64
lef64: VFCMGE_D2(0, 2, 3); B storef64
storef64:
    VAND V1.B16, V3.B16, V3.B16
    VMOV V3.D[0], R6; MOVB R6, (R3); VMOV V3.D[1], R6; MOVB R6, 1(R3); ADD $2, R3; SUB $2, R4; B loopf64
tailf64:
    CBZ R4, matchf64_done; FMOVD.P 8(R0), F1
    CMP $0, R2; BEQ t_eqf64; CMP $1, R2; BEQ t_neqf64; CMP $2, R2; BEQ t_gtf64; CMP $3, R2; BEQ t_gef64; CMP $4, R2; BEQ t_ltf64; CMP $5, R2; BEQ t_lef64; B t_eqf64
t_eqf64: FCMPD F0, F1; CSET EQ, R6; B t_storef64
t_neqf64: FCMPD F0, F1; CSET NE, R6; B t_storef64
t_gtf64: FCMPD F1, F0; CSET LT, R6; B t_storef64
t_gef64: FCMPD F1, F0; CSET LE, R6; B t_storef64
t_ltf64: FCMPD F1, F0; CSET GT, R6; B t_storef64
t_lef64: FCMPD F1, F0; CSET GE, R6; B t_storef64
t_storef64: MOVB R6, (R3); ADD $1, R3; SUB $1, R4; B tailf64
matchf64_done: RET

// func dotInt4NeonKernel(a, b unsafe.Pointer, n int) int32
TEXT ·dotInt4NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    
    MOVD    $0, R3                 // Total accumulator
    MOVD    $0x0F0F0F0F0F0F0F0F, R4 // Low nibble mask
    
    VEOR    V0.B16, V0.B16, V0.B16 // Clear vector accumulator (16-bit lanes)

    CMP     $16, R2
    BLT     dot4_tail

dot4_loop_16x:
    VLD1.P  16(R0), [V1.B16]       // Load 16 bytes (32x 4-bit)
    VLD1.P  16(R1), [V2.B16]
    
    VMOV    R4, V10.D[0]
    VMOV    R4, V10.D[1]
    
    // Extract low 4 bits (low nibble)
    VAND    V1.B16, V10.B16, V3.B16 
    VAND    V2.B16, V10.B16, V4.B16 
    
    // Extract high 4 bits (high nibble)
    VUSHR   $4, V1.B16, V5.B16      
    VUSHR   $4, V2.B16, V6.B16      
    
    // Multiply and accumulate: (a_low * b_low) + (a_high * b_high)
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    // VMLAL V5.8B, V6.8B, V0.8H
    WORD    $0x2e2680a0
    // VMLAL2 V5.16B, V6.16B, V0.8H
    WORD    $0x6e2680a0

    SUB     $16, R2
    CMP     $16, R2
    BGE     dot4_loop_16x

    // Manual reduction of V0.8H into R3
    VMOV    V0.H[0], R3
    VMOV    V0.H[1], R10
    ADD     R10, R3
    VMOV    V0.H[2], R10
    ADD     R10, R3
    VMOV    V0.H[3], R10
    ADD     R10, R3
    VMOV    V0.H[4], R10
    ADD     R10, R3
    VMOV    V0.H[5], R10
    ADD     R10, R3
    VMOV    V0.H[6], R10
    ADD     R10, R3
    VMOV    V0.H[7], R10
    ADD     R10, R3

dot4_tail:
    CBZ     R2, dot4_done
    MOVBU.P 1(R0), R10
    MOVBU.P 1(R1), R11
    
    // Low nibble
    AND     $0x0F, R10, R12
    AND     $0x0F, R11, R13
    MUL     R12, R13, R12
    ADD     R12, R3
    
    // High nibble
    LSR     $4, R10, R10
    LSR     $4, R11, R11
    MUL     R10, R11, R10
    ADD     R10, R3
    
    SUB     $1, R2
    B       dot4_tail

dot4_done:
    MOVW    R3, ret+24(FP)
    RET

// func dotInt2NeonKernel(a, b unsafe.Pointer, n int) int32
TEXT ·dotInt2NeonKernel(SB), NOSPLIT, $0-28
    MOVD    a+0(FP), R0
    MOVD    b+8(FP), R1
    MOVD    n+16(FP), R2
    
    MOVD    $0, R3                 // Total accumulator
    MOVD    $0x0303030303030303, R4 // 2-bit mask
    
    VEOR    V0.B16, V0.B16, V0.B16 // Accumulator

    CMP     $16, R2
    BLT     dot2_tail

dot2_loop_16x:
    VLD1.P  16(R0), [V1.B16]
    VLD1.P  16(R1), [V2.B16]
    
    VMOV    R4, V10.D[0]
    VMOV    R4, V10.D[1]
    
    // 2-bit extraction (4 elements per byte)
    // Element 0: bits 0-1
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 1: bits 2-3
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 2: bits 4-5
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060
    
    // Element 3: bits 6-7
    VUSHR   $2, V1.B16, V1.B16
    VUSHR   $2, V2.B16, V2.B16
    VAND    V1.B16, V10.B16, V3.B16
    VAND    V2.B16, V10.B16, V4.B16
    // VMLAL V3.8B, V4.8B, V0.8H
    WORD    $0x2e248060
    // VMLAL2 V3.16B, V4.16B, V0.8H
    WORD    $0x6e248060

    SUB     $16, R2
    CMP     $16, R2
    BGE     dot2_loop_16x

    // Manual reduction of V0.8H into R3
    VMOV    V0.H[0], R3
    VMOV    V0.H[1], R10
    ADD     R10, R3
    VMOV    V0.H[2], R10
    ADD     R10, R3
    VMOV    V0.H[3], R10
    ADD     R10, R3
    VMOV    V0.H[4], R10
    ADD     R10, R3
    VMOV    V0.H[5], R10
    ADD     R10, R3
    VMOV    V0.H[6], R10
    ADD     R10, R3
    VMOV    V0.H[7], R10
    ADD     R10, R3

dot2_tail:
    CBZ     R2, dot2_done
    MOVBU.P 1(R0), R10
    MOVBU.P 1(R1), R11
    
    // 4 elements per byte
    MOVD    $4, R14
dot2_scalar_inner:
    AND     $0x03, R10, R12
    AND     $0x03, R11, R13
    MUL     R12, R13, R12
    ADD     R12, R3
    LSR     $2, R10, R10
    LSR     $2, R11, R11
    SUB     $1, R14
    CBNZ    R14, dot2_scalar_inner
    
    SUB     $1, R2
    B       dot2_tail

dot2_done:
    MOVW    R3, ret+24(FP)
    RET
