//go:build amd64
#include "textflag.h"

// func euclideanSQ8AVX2Kernel(a, b unsafe.Pointer, n int) int32
TEXT ·euclideanSQ8AVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    a+0(FP), SI       // SI = a pointer
    MOVQ    b+8(FP), DI       // DI = b pointer
    MOVQ    n+16(FP), CX      // CX = n (count)

    XORL    AX, AX            // AX = accumulator (lower 32)
    VXORPS  Y0, Y0, Y0        // Y0 = accumulator (8 x int32)

    // Check if we can do 32-byte blocks
    CMPQ    CX, $32
    JB      tail_check // Jump if < 32 bytes

loop_32:
    // Load 32 bytes from a and b
    VMOVDQU (SI), Y1          // Y1 = a[0..31]
    VMOVDQU (DI), Y2          // Y2 = b[0..31]

    // We process lower 16 and upper 16 bytes separately to widen to 16-bit
    // Lower 16 bytes -> 16 uint16
    VPMOVZXBW X1, Y3          // Y3 = a[0..15] (u16)
    VPMOVZXBW X2, Y4          // Y4 = b[0..15] (u16)
    VPSUBW    Y4, Y3, Y3      // Y3 = a - b (int16)
    VPMADDWD  Y3, Y3, Y3      // Y3 = (a-b)^2 pairs summed to int32 (horizontal add pairs)
                              // VPMADDWD does: dst[i] = src[i]*dest[i] + src[i+1]*dest[i+1]
                              // Wait, VPMADDWD multiplies signed words and adds adjacent pairs.
                              // dest[i] = src1[i]*src2[i] + src1[i+1]*src2[i+1] (32-bit result)
                              // We want (a-b)^2. Y3 holds a-b.
                              // So VPMADDWD(Y3, Y3) = (a-b)[0]^2 + (a-b)[1]^2, etc.
                              // This reduces 16 int16s to 8 int32s. Perfect.
    VPADDD    Y3, Y0, Y0      // Accumulate into Y0

    // Extract upper 16 bytes
    // VPERM2I128 $1, Y1, Y1  // Swap lanes to put upper 128 into lower?
    // or VEXTRACTI128 $1, Y1, X1_upper (needs destination X register)
    VEXTRACTI128 $1, Y1, X5   // X5 = a[16..31]
    VEXTRACTI128 $1, Y2, X6   // X6 = b[16..31]
    
    VPMOVZXBW X5, Y5          // Y5 = a[16..31] (u16)
    VPMOVZXBW X6, Y6          // Y6 = b[16..31] (u16)
    VPSUBW    Y6, Y5, Y5      // Y5 = a - b
    VPMADDWD  Y5, Y5, Y5      // Squares summed pairs
    VPADDD    Y5, Y0, Y0      // Accumulate

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $32, CX
    CMPQ    CX, $32
    JAE     loop_32

tail_check:
    CMPQ    CX, $0
    JE      reduce
    // Scalar tail handling? Or use Generic fallback logic?
    // Doing scalar here avoids JMP overhead back to generic or complexity of 
    // trying to export generic to ASM.
    // Let's implement simple scalar tail.

tail_loop:
    MOVBLZX (SI), BX      // a[i] (zero extended to 32)
    MOVBLZX (DI), DX      // b[i]
    SUBL    DX, BX        // a - b
    IMULL   BX, BX        // (a-b)^2
    ADDL    BX, AX        // accum += (a-b)^2
    
    INCQ    SI
    INCQ    DI
    DECQ    CX
    JNZ     tail_loop

reduce:
    // Sum Y0 (8 x int32)
    // Horizontal adds not trivial in AVX2?
    // Y0 : [A B C D | E F G H]
    VEXTRACTI128 $1, Y0, X1 // X1 = [E F G H]
    VPADDD       X1, X0, X0 // X0 = [A+E B+F C+G D+H]
    // Now horizontal add 4 ints
    VPHADDD      X0, X0, X0 // X0 = [AE+BF CG+DH ...] (horizontal add adjacent)
    VPHADDD      X0, X0, X0 // X0 = [AEBFCGDH ...] (total sum in low element)
    
    MOVQ         X0, BX     // Move low 64 bits to BX
    // Result is bottom 32-bit of BX
    MOVL         BX, DX     // Just to be safe, though MOVQ covers it.
    
    ADDL         DX, AX     // Add SIMD sum to scalar tail sum
    MOVL         AX, ret+24(FP)
    VZEROUPPER
    RET
