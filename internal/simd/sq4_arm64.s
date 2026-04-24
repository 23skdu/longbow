//go:build arm64
// +build arm64

#include "textflag.h"

// func dotInt4NeonKernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4NeonKernel(SB), NOSPLIT, $0-28
    MOVW $0, R0
    FMOVS R0, F0
    FMOVS F0, ret+24(FP)
    RET

// func dotInt2NeonKernel(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt2NeonKernel(SB), NOSPLIT, $0-28
    MOVW $0, R0
    FMOVS R0, F0
    FMOVS F0, ret+24(FP)
    RET
