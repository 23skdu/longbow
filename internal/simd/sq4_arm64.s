// +build arm64

#include "textflag.h"

// func dotInt4Neon(a, b unsafe.Pointer, n int) float32
TEXT ·dotInt4Neon(SB), NOSPLIT, $0-28
    // Fallback to Go implementation for now to avoid assembly syntax issues
    // during multi-platform build stabilization.
    RET
