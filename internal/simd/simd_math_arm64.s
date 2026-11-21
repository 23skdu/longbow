//go:build arm64

#include "textflag.h"
#include "simd_arm64_macros.h"

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
