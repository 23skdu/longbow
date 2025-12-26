// +build amd64

#include "textflag.h"

// ----------------------------------------------------------------------------
// func l2SquaredAVX512(a, b unsafe.Pointer, n int) float32
//
// Input:
//   a:  SI (pointer to float32 array)
//   b:  DI (pointer to float32 array)
//   n:  BX (number of elements)
//
// Output:
//   X0 (return value, sum of squared differences)
//
// Registers used:
//   Z0..Z3:  Accumulators
//   Z4..Z11: Scratch for loading data and computations
//   K1:      Mask for tail processing
// ----------------------------------------------------------------------------
TEXT ·l2SquaredAVX512(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    // Initialize accumulators to 0
    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    // Check if n >= 64 (4x unroll loop)
    CMPQ    BX, $64
    JL      tail_check

loop_64:
    // Load 64 elements from A (cache line friendly)
    VMOVUPS (SI), Z4
    VMOVUPS 64(SI), Z5
    VMOVUPS 128(SI), Z6
    VMOVUPS 192(SI), Z7

    // Load 64 elements from B
    VMOVUPS (DI), Z8
    VMOVUPS 64(DI), Z9
    VMOVUPS 128(DI), Z10
    VMOVUPS 192(DI), Z11

    // Compute differences: diff = a - b
    VSUBPS  Z8, Z4, Z4
    VSUBPS  Z9, Z5, Z5
    VSUBPS  Z10, Z6, Z6
    VSUBPS  Z11, Z7, Z7

    // Accumulate squares: sum += diff * diff (FMA)
    // Z += diff * diff + 0 (Since accumulators started at 0 or hold prev sum)
    // Use VFMADD231PS Dest, Src1, Src2 -> Dest = Src1*Src2 + Dest
    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3

    // Advance pointers and counter
    ADDQ    $256, SI
    ADDQ    $256, DI
    SUBQ    $64, BX
    CMPQ    BX, $64
    JGE     loop_64

tail_check:
    // Reduce 4 accumulators to 1 (Z0)
    VADDPS  Z1, Z0, Z0
    VADDPS  Z2, Z0, Z0
    VADDPS  Z3, Z0, Z0

    // Check for tail processing
    CMPQ    BX, $0
    JE      reduce_final

    // Process remaining elements in chunks of 16 could be done, 
    // but masking is cleaner for the *very* end. 
    // However, if we have say 48 left, masking 16 at a time is fine.
    // Let's loop 16 until done.

loop_16:
    CMPQ    BX, $16
    JL      tail_masked

    VMOVUPS (SI), Z4
    VMOVUPS (DI), Z5
    VSUBPS  Z5, Z4, Z4
    VFMADD231PS Z4, Z4, Z0

    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $16, BX
    JMP     loop_16

tail_masked:
    CMPQ    BX, $0
    JE      reduce_final

    // Create mask for remaining elements: (1 << BX) - 1
    MOVQ    $1, R8
    MOVQ    BX, CX
    SHLQ    CX, R8
    SUBQ    $1, R8
    KMOVQ   R8, K1

    // Masked load
    // Use VMOVDQU32 for masked load. 
    // Go syntax: VMOVDQU32 (SI), K1, Z4  (Merge into Z4)
    // We must zero Z4/Z5 first to effectively get zero-masking
    VPXORD  Z4, Z4, Z4
    VPXORD  Z5, Z5, Z5
    
    VMOVDQU32 (SI), K1, Z4
    VMOVDQU32 (DI), K1, Z5
    
    VSUBPS  Z5, Z4, Z4
    VFMADD231PS Z4, Z4, Z0

reduce_final:
    // Horizontal reduction of Z0 (512-bit) -> float32
    // 1. Extract high 256
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    
    // 2. Extract high 128
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    
    // 3. Extract high 64
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    
    // 4. Extract high 32 (odd index)
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET


// ----------------------------------------------------------------------------
// func cosineDotAVX512(a, b unsafe.Pointer, n int) (dot, normA, normB float32)
// ----------------------------------------------------------------------------
TEXT ·cosineDotAVX512(SB), NOSPLIT, $0-36
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    // Accumulators for Dot, NormA, NormB
    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3
    
    VXORPS  Z4, Z4, Z4
    VXORPS  Z5, Z5, Z5
    VXORPS  Z6, Z6, Z6
    VXORPS  Z7, Z7, Z7

    VXORPS  Z8, Z8, Z8
    VXORPS  Z9, Z9, Z9
    VXORPS  Z10, Z10, Z10
    VXORPS  Z11, Z11, Z11

    CMPQ    BX, $64
    JL      cos_tail_check

cos_loop_64:
    // Load A
    VMOVUPS (SI), Z12
    VMOVUPS 64(SI), Z13
    VMOVUPS 128(SI), Z14
    VMOVUPS 192(SI), Z15
    
    // NormA: += A*A
    VFMADD231PS Z12, Z12, Z4
    VFMADD231PS Z13, Z13, Z5
    VFMADD231PS Z14, Z14, Z6
    VFMADD231PS Z15, Z15, Z7

    // Load B
    VMOVUPS (DI), Z16
    VMOVUPS 64(DI), Z17
    VMOVUPS 128(DI), Z18
    VMOVUPS 192(DI), Z19

    // NormB: += B*B
    VFMADD231PS Z16, Z16, Z8
    VFMADD231PS Z17, Z17, Z9
    VFMADD231PS Z18, Z18, Z10
    VFMADD231PS Z19, Z19, Z11

    // Dot: += A*B
    VFMADD231PS Z12, Z16, Z0
    VFMADD231PS Z13, Z17, Z1
    VFMADD231PS Z14, Z18, Z2
    VFMADD231PS Z15, Z19, Z3

    ADDQ    $256, SI
    ADDQ    $256, DI
    SUBQ    $64, BX
    CMPQ    BX, $64
    JGE     cos_loop_64

cos_tail_check:
    // Reduce unrolled accumulators
    VADDPS  Z1, Z0, Z0
    VADDPS  Z2, Z0, Z0
    VADDPS  Z3, Z0, Z0 // Z0 is partial Dot

    VADDPS  Z5, Z4, Z4
    VADDPS  Z6, Z4, Z4
    VADDPS  Z7, Z4, Z4 // Z4 is partial NormA

    VADDPS  Z9, Z8, Z8
    VADDPS  Z10, Z8, Z8
    VADDPS  Z11, Z8, Z8 // Z8 is partial NormB

cos_loop_16:
    CMPQ    BX, $16
    JL      cos_tail_masked

    VMOVUPS (SI), Z12
    VMOVUPS (DI), Z16
    
    VFMADD231PS Z12, Z12, Z4
    VFMADD231PS Z16, Z16, Z8
    VFMADD231PS Z12, Z16, Z0

    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $16, BX
    JMP     cos_loop_16

cos_tail_masked:
    CMPQ    BX, $0
    JE      cos_reduce_final

    MOVQ    $1, R8
    MOVQ    BX, CX
    SHLQ    CX, R8
    SUBQ    $1, R8
    KMOVQ   R8, K1

    VPXORD  Z12, Z12, Z12
    VPXORD  Z16, Z16, Z16

    VMOVDQU32 (SI), K1, Z12
    VMOVDQU32 (DI), K1, Z16

    VFMADD231PS Z12, Z12, Z4
    VFMADD231PS Z16, Z16, Z8
    VFMADD231PS Z12, Z16, Z0

cos_reduce_final:
    // Reduce Z0 (Dot) -> X0
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    VMOVSS  X0, ret+24(FP)

    // Reduce Z4 (NormA) -> X4
    VEXTRACTF64X4 $1, Z4, Y5
    VADDPS  Y5, Y4, Y4
    VEXTRACTF128 $1, Y4, X5
    VADDPS  X5, X4, X4
    VMOVHLPS X4, X5, X5
    VADDPS  X5, X4, X4
    VMOVSHDUP X4, X5
    VADDSS  X5, X4, X4
    VMOVSS  X4, ret+28(FP)

    // Reduce Z8 (NormB) -> X8
    VEXTRACTF64X4 $1, Z8, Y5
    VADDPS  Y5, Y8, Y8
    VEXTRACTF128 $1, Y8, X5
    VADDPS  X5, X8, X8
    VMOVHLPS X8, X5, X5
    VADDPS  X5, X8, X8
    VMOVSHDUP X8, X5
    VADDSS  X5, X8, X8
    VMOVSS  X8, ret+32(FP)

    VZEROUPPER
    RET

// ----------------------------------------------------------------------------
// func euclideanVertical4AVX2(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// ----------------------------------------------------------------------------
TEXT ·euclideanVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    // Accumulators
    VXORPS  Y0, Y0, Y0 // Sum0
    VXORPS  Y1, Y1, Y1 // Sum1
    VXORPS  Y2, Y2, Y2 // Sum2
    VXORPS  Y3, Y3, Y3 // Sum3

    CMPQ    BX, $8
    JL      ev2_tail

ev2_loop8:
    VMOVUPS (SI), Y4 // Query
    
    VMOVUPS (DI), Y5
    VMOVUPS (R8), Y6
    VMOVUPS (R9), Y7
    VMOVUPS (R10), Y8

    VSUBPS  Y4, Y5, Y5
    VSUBPS  Y4, Y6, Y6
    VSUBPS  Y4, Y7, Y7
    VSUBPS  Y4, Y8, Y8

    VFMADD231PS Y5, Y5, Y0
    VFMADD231PS Y6, Y6, Y1
    VFMADD231PS Y7, Y7, Y2
    VFMADD231PS Y8, Y8, Y3

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     ev2_loop8

ev2_tail:
    CMPQ    BX, $0
    JE      ev2_reduce

ev2_tail_loop:
    VMOVSS  (SI), X4
    VMOVSS  (DI), X5
    VMOVSS  (R8), X6
    VMOVSS  (R9), X7
    VMOVSS  (R10), X8

    VSUBSS  X4, X5, X5
    VSUBSS  X4, X6, X6
    VSUBSS  X4, X7, X7
    VSUBSS  X4, X8, X8

    VFMADD231SS X5, X5, X0
    VFMADD231SS X6, X6, X1
    VFMADD231SS X7, X7, X2
    VFMADD231SS X8, X8, X3

    ADDQ    $4, SI
    ADDQ    $4, DI
    ADDQ    $4, R8
    ADDQ    $4, R9
    ADDQ    $4, R10
    DECQ    BX
    JNZ     ev2_tail_loop

ev2_reduce:
    // Full reduction of Y0..Y3
    // Res0 (Y0)
    VEXTRACTF128 $1, Y0, X4
    VADDPS  X4, X0, X0
    VMOVHLPS X0, X4, X4
    VADDPS  X4, X0, X0
    VMOVSHDUP X0, X4
    VADDSS  X4, X0, X0
    VSQRTSS X0, X0, X0
    VMOVSS  X0, (R11)

    // Res1 (Y1)
    VEXTRACTF128 $1, Y1, X4
    VADDPS  X4, X1, X1
    VMOVHLPS X1, X4, X4
    VADDPS  X4, X1, X1
    VMOVSHDUP X1, X4
    VADDSS  X4, X1, X1
    VSQRTSS X1, X1, X1
    VMOVSS  X1, 4(R11)

    // Res2 (Y2)
    VEXTRACTF128 $1, Y2, X4
    VADDPS  X4, X2, X2
    VMOVHLPS X2, X4, X4
    VADDPS  X4, X2, X2
    VMOVSHDUP X2, X4
    VADDSS  X4, X2, X2
    VSQRTSS X2, X2, X2
    VMOVSS  X2, 8(R11)

    // Res3 (Y3)
    VEXTRACTF128 $1, Y3, X4
    VADDPS  X4, X3, X3
    VMOVHLPS X3, X4, X4
    VADDPS  X4, X3, X3
    VMOVSHDUP X3, X4
    VADDSS  X4, X3, X3
    VSQRTSS X3, X3, X3
    VMOVSS  X3, 12(R11)

    VZEROUPPER
    RET


// ----------------------------------------------------------------------------
// func euclideanVertical4AVX512(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// ----------------------------------------------------------------------------
TEXT ·euclideanVertical4AVX512(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    // Accumulators
    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    CMPQ    BX, $16
    JL      ev5_tail

ev5_loop16:
    VMOVUPS (SI), Z4 // Query
    
    VMOVUPS (DI), Z5
    VMOVUPS (R8), Z6
    VMOVUPS (R9), Z7
    VMOVUPS (R10), Z8

    VSUBPS  Z4, Z5, Z5
    VSUBPS  Z4, Z6, Z6
    VSUBPS  Z4, Z7, Z7
    VSUBPS  Z4, Z8, Z8

    VFMADD231PS Z5, Z5, Z0
    VFMADD231PS Z6, Z6, Z1
    VFMADD231PS Z7, Z7, Z2
    VFMADD231PS Z8, Z8, Z3

    ADDQ    $64, SI
    ADDQ    $64, DI
    ADDQ    $64, R8
    ADDQ    $64, R9
    ADDQ    $64, R10
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     ev5_loop16

ev5_tail:
    CMPQ    BX, $0
    JE      ev5_reduce

    // Tail mask
    MOVQ    $1, R12
    MOVQ    BX, CX
    SHLQ    CX, R12
    SUBQ    $1, R12
    KMOVQ   R12, K1

    VPXORD  Z4, Z4, Z4
    VPXORD  Z5, Z5, Z5
    VPXORD  Z6, Z6, Z6
    VPXORD  Z7, Z7, Z7
    VPXORD  Z8, Z8, Z8

    VMOVDQU32 (SI), K1, Z4
    VMOVDQU32 (DI), K1, Z5
    VMOVDQU32 (R8), K1, Z6
    VMOVDQU32 (R9), K1, Z7
    VMOVDQU32 (R10), K1, Z8

    VSUBPS  Z4, Z5, Z5
    VSUBPS  Z4, Z6, Z6
    VSUBPS  Z4, Z7, Z7
    VSUBPS  Z4, Z8, Z8

    VFMADD231PS Z5, Z5, Z0
    VFMADD231PS Z6, Z6, Z1
    VFMADD231PS Z7, Z7, Z2
    VFMADD231PS Z8, Z8, Z3

ev5_reduce:
    // Reduction for Z0
    VEXTRACTF64X4 $1, Z0, Y4
    VADDPS  Y4, Y0, Y0
    VEXTRACTF128 $1, Y0, X4
    VADDPS  X4, X0, X0
    VMOVHLPS X0, X4, X4
    VADDPS  X4, X0, X0
    VMOVSHDUP X0, X4
    VADDSS  X4, X0, X0
    VSQRTSS X0, X0, X0
    VMOVSS  X0, (R11)

    // Reduction for Z1
    VEXTRACTF64X4 $1, Z1, Y4
    VADDPS  Y4, Y1, Y1
    VEXTRACTF128 $1, Y1, X4
    VADDPS  X4, X1, X1
    VMOVHLPS X1, X4, X4
    VADDPS  X4, X1, X1
    VMOVSHDUP X1, X4
    VADDSS  X4, X1, X1
    VSQRTSS X1, X1, X1
    VMOVSS  X1, 4(R11)

    // Reduction for Z2
    VEXTRACTF64X4 $1, Z2, Y4
    VADDPS  Y4, Y2, Y2
    VEXTRACTF128 $1, Y2, X4
    VADDPS  X4, X2, X2
    VMOVHLPS X2, X4, X4
    VADDPS  X4, X2, X2
    VMOVSHDUP X2, X4
    VADDSS  X4, X2, X2
    VSQRTSS X2, X2, X2
    VMOVSS  X2, 8(R11)

    // Reduction for Z3
    VEXTRACTF64X4 $1, Z3, Y4
    VADDPS  Y4, Y3, Y3
    VEXTRACTF128 $1, Y3, X4
    VADDPS  X4, X3, X3
    VMOVHLPS X3, X4, X4
    VADDPS  X4, X3, X3
    VMOVSHDUP X3, X4
    VADDSS  X4, X3, X3
    VSQRTSS X3, X3, X3
    VMOVSS  X3, 12(R11)

    VZEROUPPER
    RET
