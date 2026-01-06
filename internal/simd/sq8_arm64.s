//go:build arm64
#include "textflag.h"

// Stub file to prevent build error
// func euclideanSQ8NEONKernel(a, b unsafe.Pointer, n int) int32
// TEXT ·euclideanSQ8NEONKernel(SB), NOSPLIT, $0-28
//     MOVW $0, R0
//     MOVW R0, ret+24(FP)
//     RET
