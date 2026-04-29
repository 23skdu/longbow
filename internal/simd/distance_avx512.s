// +build amd64,avx512

#include "textflag.h"

// AVX512-optimized distance functions

// ----------------------------------------------------------------------------
// func l2SquaredAVX512Kernel(a, b unsafe.Pointer, n int) float32
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
TEXT ·l2SquaredAVX512Kernel(SB), NOSPLIT, $0-28
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
// func dotAVX512Kernel(a, b unsafe.Pointer, n int) float32
// ----------------------------------------------------------------------------
TEXT ·dotAVX512Kernel(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    CMPQ    BX, $64
    JL      dot_tail_check

loop_dot_64:
    VMOVUPS (SI), Z4
    VMOVUPS 64(SI), Z5
    VMOVUPS 128(SI), Z6
    VMOVUPS 192(SI), Z7

    VFMADD231PS (DI), Z4, Z0
    VFMADD231PS 64(DI), Z5, Z1
    VFMADD231PS 128(DI), Z6, Z2
    VFMADD231PS 192(DI), Z7, Z3

    ADDQ    $256, SI
    ADDQ    $256, DI
    SUBQ    $64, BX
    CMPQ    BX, $64
    JGE     loop_dot_64

dot_tail_check:
    VADDPS  Z1, Z0, Z0
    VADDPS  Z2, Z0, Z0
    VADDPS  Z3, Z0, Z0

    CMPQ    BX, $0
    JE      dot_reduce_final

loop_dot_16:
    CMPQ    BX, $16
    JL      dot_tail_masked

    VMOVUPS (SI), Z4
    VFMADD231PS (DI), Z4, Z0

    ADDQ    $64, SI
    ADDQ    $64, DI
    SUBQ    $16, BX
    JMP     loop_dot_16

dot_tail_masked:
    CMPQ    BX, $0
    JE      dot_reduce_final

    MOVQ    $1, R8
    MOVQ    BX, CX
    SHLQ    CX, R8
    SUBQ    $1, R8
    KMOVQ   R8, K1

    VPXORD  Z4, Z4, Z4
    VPXORD  Z5, Z5, Z5
    
    VMOVDQU32 (SI), K1, Z4
    VMOVDQU32 (DI), K1, Z5
    VFMADD231PS Z4, Z5, Z0

dot_reduce_final:
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
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
    VMOVSS  X0, dot+24(FP)

    // Reduce Z4 (NormA) -> X4
    VEXTRACTF64X4 $1, Z4, Y5
    VADDPS  Y5, Y4, Y4
    VEXTRACTF128 $1, Y4, X5
    VADDPS  X5, X4, X4
    VMOVHLPS X4, X5, X5
    VADDPS  X5, X4, X4
    VMOVSHDUP X4, X5
    VADDSS  X5, X4, X4
    VMOVSS  X4, normA+28(FP)

    // Reduce Z8 (NormB) -> X8
    VEXTRACTF64X4 $1, Z8, Y5
    VADDPS  Y5, Y8, Y8
    VEXTRACTF128 $1, Y8, X5
    VADDPS  X5, X8, X8
    VMOVHLPS X8, X5, X5
    VADDPS  X5, X8, X8
    VMOVSHDUP X8, X5
    VADDSS  X5, X8, X8
    VMOVSS  X8, normB+32(FP)
    
    // ABI: Return values in X0, X1, X2
    // X0 already has Dot from line 270 (VMOVSS X0, dot+24(FP))
    MOVSS   X4, X1             // normA
    MOVSS   X8, X2             // normB

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


// ----------------------------------------------------------------------------
// func cosineVertical4AVX2(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// Returns cosine similarity for 4 vectors in parallel
// res[0..3] = dot(q,vi) / (norm(q) * norm(vi))
// ----------------------------------------------------------------------------
TEXT ·cosineVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0 // dot0
    VXORPS  Y1, Y1, Y1 // dot1
    VXORPS  Y2, Y2, Y2 // dot2
    VXORPS  Y3, Y3, Y3 // dot3
    VXORPS  Y4, Y4, Y4 // norm_q^2
    VXORPS  Y5, Y5, Y5 // norm_v0^2
    VXORPS  Y6, Y6, Y6 // norm_v1^2
    VXORPS  Y7, Y7, Y7 // norm_v2^2
    VXORPS  Y8, Y8, Y8 // norm_v3^2

    CMPQ    BX, $8
    JL      cv2_tail

cv2_loop8:
    VMOVUPS (SI), Y9
    VMOVUPS (DI), Y10
    VMOVUPS (R8), Y11
    VMOVUPS (R9), Y12
    VMOVUPS (R10), Y13

    VFMADD231PS Y9, Y10, Y0
    VFMADD231PS Y9, Y11, Y1
    VFMADD231PS Y9, Y12, Y2
    VFMADD231PS Y9, Y13, Y3

    VFMADD231PS Y9, Y9, Y4
    VFMADD231PS Y10, Y10, Y5
    VFMADD231PS Y11, Y11, Y6
    VFMADD231PS Y12, Y12, Y7
    VFMADD231PS Y13, Y13, Y8

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     cv2_loop8

cv2_tail:
    CMPQ    BX, $0
    JE      cv2_reduce

cv2_tail_loop:
    VMOVSS  (SI), X9
    VMOVSS  (DI), X10
    VMOVSS  (R8), X11
    VMOVSS  (R9), X12
    VMOVSS  (R10), X13

    VFMADD231SS X9, X10, X0
    VFMADD231SS X9, X11, X1
    VFMADD231SS X9, X12, X2
    VFMADD231SS X9, X13, X3

    VFMADD231SS X9, X9, X4
    VFMADD231SS X10, X10, X5
    VFMADD231SS X11, X11, X6
    VFMADD231SS X12, X12, X7
    VFMADD231SS X13, X13, X8

    ADDQ    $4, SI
    ADDQ    $4, DI
    ADDQ    $4, R8
    ADDQ    $4, R9
    ADDQ    $4, R10
    DECQ    BX
    JNZ     cv2_tail_loop

cv2_reduce:
    VEXTRACTF128 $1, Y0, X0
    VEXTRACTF128 $1, Y1, X1
    VEXTRACTF128 $1, Y2, X2
    VEXTRACTF128 $1, Y3, X3
    VEXTRACTF128 $1, Y4, X4
    VEXTRACTF128 $1, Y5, X5
    VEXTRACTF128 $1, Y6, X6
    VEXTRACTF128 $1, Y7, X7
    VEXTRACTF128 $1, Y8, X8

    VMOVHLPS X0, X0, X0; VADDSS X0, X0, X0; VMOVSHDUP X0, X0; VADDSS X0, X0, X0
    VMOVHLPS X1, X1, X1; VADDSS X1, X1, X1; VMOVSHDUP X1, X1; VADDSS X1, X1, X1
    VMOVHLPS X2, X2, X2; VADDSS X2, X2, X2; VMOVSHDUP X2, X2; VADDSS X2, X2, X2
    VMOVHLPS X3, X3, X3; VADDSS X3, X3, X3; VMOVSHDUP X3, X3; VADDSS X3, X3, X3
    VMOVHLPS X4, X4, X4; VADDSS X4, X4, X4; VMOVSHDUP X4, X4; VADDSS X4, X4, X4
    VMOVHLPS X5, X5, X5; VADDSS X5, X5, X5; VMOVSHDUP X5, X5; VADDSS X5, X5, X5
    VMOVHLPS X6, X6, X6; VADDSS X6, X6, X6; VMOVSHDUP X6, X6; VADDSS X6, X6, X6
    VMOVHLPS X7, X7, X7; VADDSS X7, X7, X7; VMOVSHDUP X7, X7; VADDSS X7, X7, X7
    VMOVHLPS X8, X8, X8; VADDSS X8, X8, X8; VMOVSHDUP X8, X8; VADDSS X8, X8, X8

    SQRTSS X4, X4
    SQRTSS X5, X5
    SQRTSS X6, X6
    SQRTSS X7, X7
    SQRTSS X8, X8

    MULSS X4, X5
    MULSS X4, X6
    MULSS X4, X7
    MULSS X4, X8

    DIVSS X5, X0
    DIVSS X6, X1
    DIVSS X7, X2
    DIVSS X8, X3

    VMOVSS X0, (R11)
    VMOVSS X1, 4(R11)
    VMOVSS X2, 8(R11)
    VMOVSS X3, 12(R11)

    VZEROUPPER
    RET


// ----------------------------------------------------------------------------
// func dotVertical4AVX2(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// Returns dot products: dot(q, v0), dot(q, v1), dot(q, v2), dot(q, v3)
// ----------------------------------------------------------------------------
TEXT ·dotVertical4AVX2(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Y0, Y0, Y0
    VXORPS  Y1, Y1, Y1
    VXORPS  Y2, Y2, Y2
    VXORPS  Y3, Y3, Y3

    CMPQ    BX, $8
    JL      dotv2_tail

dotv2_loop8:
    VMOVUPS (SI), Y4 // q
    VMOVUPS (DI), Y5 // v0
    VMOVUPS (R8), Y6 // v1
    VMOVUPS (R9), Y7 // v2
    VMOVUPS (R10), Y8 // v3

    VFMADD231PS Y4, Y5, Y0 // q*v0
    VFMADD231PS Y4, Y6, Y1 // q*v1
    VFMADD231PS Y4, Y7, Y2 // q*v2
    VFMADD231PS Y4, Y8, Y3 // q*v3

    ADDQ    $32, SI
    ADDQ    $32, DI
    ADDQ    $32, R8
    ADDQ    $32, R9
    ADDQ    $32, R10
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     dotv2_loop8

dotv2_tail:
    CMPQ    BX, $0
    JE      dotv2_reduce

dotv2_tail_loop:
    VMOVSS  (SI), X4
    VMOVSS  (DI), X5
    VMOVSS  (R8), X6
    VMOVSS  (R9), X7
    VMOVSS  (R10), X8

    VFMADD231SS X4, X5, X0
    VFMADD231SS X4, X6, X1
    VFMADD231SS X4, X7, X2
    VFMADD231SS X4, X8, X3

    ADDQ    $4, SI
    ADDQ    $4, DI
    ADDQ    $4, R8
    ADDQ    $4, R9
    ADDQ    $4, R10
    DECQ    BX
    JNZ     dotv2_tail_loop

dotv2_reduce:
    VEXTRACTF128 $1, Y0, X4
    VADDPS  X4, X0, X0
    VMOVHLPS X0, X4, X4
    VADDPS  X4, X0, X0
    VMOVSHDUP X0, X4
    VADDSS  X4, X0, X0
    VMOVSS  X0, (R11)

    VEXTRACTF128 $1, Y1, X4
    VADDPS  X4, X1, X1
    VMOVHLPS X1, X4, X4
    VADDPS  X4, X1, X1
    VMOVSHDUP X1, X4
    VADDSS  X4, X1, X1
    VMOVSS  X1, 4(R11)

    VEXTRACTF128 $1, Y2, X4
    VADDPS  X4, X2, X2
    VMOVHLPS X2, X4, X4
    VADDPS  X4, X2, X2
    VMOVSHDUP X2, X4
    VADDSS  X4, X2, X2
    VMOVSS  X2, 8(R11)

    VEXTRACTF128 $1, Y3, X4
    VADDPS  X4, X3, X3
    VMOVHLPS X3, X4, X4
    VADDPS  X4, X3, X3
    VMOVSHDUP X3, X4
    VADDSS  X4, X3, X3
    VMOVSS  X3, 12(R11)

    VZEROUPPER
    RET


// ----------------------------------------------------------------------------
// func cosineVertical4AVX512(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// Returns cosine similarity for 4 vectors in parallel (AVX-512)
// res[0..3] = dot(q,vi) / (norm(q) * norm(vi))
// ----------------------------------------------------------------------------
TEXT ·cosineVertical4AVX512(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Z0, Z0, Z0 // dot0
    VXORPS  Z1, Z1, Z1 // dot1
    VXORPS  Z2, Z2, Z2 // dot2
    VXORPS  Z3, Z3, Z3 // dot3
    VXORPS  Z4, Z4, Z4 // norm_q^2
    VXORPS  Z5, Z5, Z5 // norm_v0^2
    VXORPS  Z6, Z6, Z6 // norm_v1^2
    VXORPS  Z7, Z7, Z7 // norm_v2^2
    VXORPS  Z8, Z8, Z8 // norm_v3^2

    CMPQ    BX, $16
    JL      cv5_tail

cv5_loop16:
    VMOVUPS (SI), Z9
    VMOVUPS (DI), Z10
    VMOVUPS (R8), Z11
    VMOVUPS (R9), Z12
    VMOVUPS (R10), Z13

    VFMADD231PS Z9, Z10, Z0
    VFMADD231PS Z9, Z11, Z1
    VFMADD231PS Z9, Z12, Z2
    VFMADD231PS Z9, Z13, Z3

    VFMADD231PS Z9, Z9, Z4
    VFMADD231PS Z10, Z10, Z5
    VFMADD231PS Z11, Z11, Z6
    VFMADD231PS Z12, Z12, Z7
    VFMADD231PS Z13, Z13, Z8

    ADDQ    $64, SI
    ADDQ    $64, DI
    ADDQ    $64, R8
    ADDQ    $64, R9
    ADDQ    $64, R10
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     cv5_loop16

cv5_tail:
    CMPQ    BX, $0
    JE      cv5_reduce

    MOVQ    $1, R12
    MOVQ    BX, CX
    SHLQ    CX, R12
    SUBQ    $1, R12
    KMOVQ   R12, K1

    VPXORD  Z9, Z9, Z9
    VPXORD  Z10, Z10, Z10
    VPXORD  Z11, Z11, Z11
    VPXORD  Z12, Z12, Z12
    VPXORD  Z13, Z13, Z13

    VMOVDQU32 (SI), K1, Z9
    VMOVDQU32 (DI), K1, Z10
    VMOVDQU32 (R8), K1, Z11
    VMOVDQU32 (R9), K1, Z12
    VMOVDQU32 (R10), K1, Z13

    VFMADD231PS Z9, Z10, Z0
    VFMADD231PS Z9, Z11, Z1
    VFMADD231PS Z9, Z12, Z2
    VFMADD231PS Z9, Z13, Z3

    VFMADD231PS Z9, Z9, Z4
    VFMADD231PS Z10, Z10, Z5
    VFMADD231PS Z11, Z11, Z6
    VFMADD231PS Z12, Z12, Z7
    VFMADD231PS Z13, Z13, Z8

cv5_reduce:
    VEXTRACTF64X4 $1, Z0, Y0; VADDPS Y0, Z0, Z0
    VEXTRACTF64X4 $1, Z1, Y1; VADDPS Y1, Z1, Z1
    VEXTRACTF64X4 $1, Z2, Y2; VADDPS Y2, Z2, Z2
    VEXTRACTF64X4 $1, Z3, Y3; VADDPS Y3, Z3, Z3
    VEXTRACTF64X4 $1, Z4, Y4; VADDPS Y4, Z4, Z4
    VEXTRACTF64X4 $1, Z5, Y5; VADDPS Y5, Z5, Z5
    VEXTRACTF64X4 $1, Z6, Y6; VADDPS Y6, Z6, Z6
    VEXTRACTF64X4 $1, Z7, Y7; VADDPS Y7, Z7, Z7
    VEXTRACTF64X4 $1, Z8, Y8; VADDPS Y8, Z8, Z8

    VEXTRACTF128 $1, Z0, X0; VADDPS X0, Z0, X0
    VEXTRACTF128 $1, Z1, X1; VADDPS X1, Z1, X1
    VEXTRACTF128 $1, Z2, X2; VADDPS X2, Z2, X2
    VEXTRACTF128 $1, Z3, X3; VADDPS X3, Z3, X3
    VEXTRACTF128 $1, Z4, X4; VADDPS X4, Z4, X4
    VEXTRACTF128 $1, Z5, X5; VADDPS X5, Z5, X5
    VEXTRACTF128 $1, Z6, X6; VADDPS X6, Z6, X6
    VEXTRACTF128 $1, Z7, X7; VADDPS X7, Z7, X7
    VEXTRACTF128 $1, Z8, X8; VADDPS X8, Z8, X8

    VMOVHLPS X0, X0, X0; VADDSS X0, X0, X0; VMOVSHDUP X0, X0; VADDSS X0, X0, X0
    VMOVHLPS X1, X1, X1; VADDSS X1, X1, X1; VMOVSHDUP X1, X1; VADDSS X1, X1, X1
    VMOVHLPS X2, X2, X2; VADDSS X2, X2, X2; VMOVSHDUP X2, X2; VADDSS X2, X2, X2
    VMOVHLPS X3, X3, X3; VADDSS X3, X3, X3; VMOVSHDUP X3, X3; VADDSS X3, X3, X3
    VMOVHLPS X4, X4, X4; VADDSS X4, X4, X4; VMOVSHDUP X4, X4; VADDSS X4, X4, X4
    VMOVHLPS X5, X5, X5; VADDSS X5, X5, X5; VMOVSHDUP X5, X5; VADDSS X5, X5, X5
    VMOVHLPS X6, X6, X6; VADDSS X6, X6, X6; VMOVSHDUP X6, X6; VADDSS X6, X6, X6
    VMOVHLPS X7, X7, X7; VADDSS X7, X7, X7; VMOVSHDUP X7, X7; VADDSS X7, X7, X7
    VMOVHLPS X8, X8, X8; VADDSS X8, X8, X8; VMOVSHDUP X8, X8; VADDSS X8, X8, X8

    SQRTSS X4, X4
    SQRTSS X5, X5
    SQRTSS X6, X6
    SQRTSS X7, X7
    SQRTSS X8, X8

    MULSS X4, X5
    MULSS X4, X6
    MULSS X4, X7
    MULSS X4, X8

    DIVSS X5, X0
    DIVSS X6, X1
    DIVSS X7, X2
    DIVSS X8, X3

    VMOVSS X0, (R11)
    VMOVSS X1, 4(R11)
    VMOVSS X2, 8(R11)
    VMOVSS X3, 12(R11)

    VZEROUPPER
    RET


// ----------------------------------------------------------------------------
// func dotVertical4AVX512(q, v0, v1, v2, v3 unsafe.Pointer, n int, res unsafe.Pointer)
// Returns dot products: dot(q, v0), dot(q, v1), dot(q, v2), dot(q, v3)
// ----------------------------------------------------------------------------
TEXT ·dotVertical4AVX512(SB), NOSPLIT, $0-56
    MOVQ    q+0(FP), SI
    MOVQ    v0+8(FP), DI
    MOVQ    v1+16(FP), R8
    MOVQ    v2+24(FP), R9
    MOVQ    v3+32(FP), R10
    MOVQ    n+40(FP), BX
    MOVQ    res+48(FP), R11

    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3

    CMPQ    BX, $16
    JL      dotv5_tail

dotv5_loop16:
    VMOVUPS (SI), Z4
    VMOVUPS (DI), Z5
    VMOVUPS (R8), Z6
    VMOVUPS (R9), Z7
    VMOVUPS (R10), Z8

    VFMADD231PS Z4, Z5, Z0
    VFMADD231PS Z4, Z6, Z1
    VFMADD231PS Z4, Z7, Z2
    VFMADD231PS Z4, Z8, Z3

    ADDQ    $64, SI
    ADDQ    $64, DI
    ADDQ    $64, R8
    ADDQ    $64, R9
    ADDQ    $64, R10
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     dotv5_loop16

dotv5_tail:
    CMPQ    BX, $0
    JE      dotv5_reduce

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

    VFMADD231PS Z4, Z5, Z0
    VFMADD231PS Z4, Z6, Z1
    VFMADD231PS Z4, Z7, Z2
    VFMADD231PS Z4, Z8, Z3

dotv5_reduce:
    VEXTRACTF64X4 $1, Z0, Y4; VADDPS Y4, Z0, Z0
    VEXTRACTF64X4 $1, Z1, Y4; VADDPS Y4, Z1, Z1
    VEXTRACTF64X4 $1, Z2, Y4; VADDPS Y4, Z2, Z2
    VEXTRACTF64X4 $1, Z3, Y4; VADDPS Y4, Z3, Z3

    VEXTRACTF128 $1, Z0, X4; VADDPS X4, Z0, X0
    VEXTRACTF128 $1, Z1, X4; VADDPS X4, Z1, X1
    VEXTRACTF128 $1, Z2, X4; VADDPS X4, Z2, X2
    VEXTRACTF128 $1, Z3, X4; VADDPS X4, Z3, X3

    VMOVHLPS X0, X4, X4; VADDPS X4, X0, X0; VMOVSHDUP X0, X4; VADDSS X4, X0, X0
    VMOVHLPS X1, X4, X4; VADDPS X4, X1, X1; VMOVSHDUP X1, X4; VADDSS X4, X1, X1
    VMOVHLPS X2, X4, X4; VADDPS X4, X2, X2; VMOVSHDUP X2, X4; VADDSS X4, X2, X2
    VMOVHLPS X3, X4, X4; VADDPS X4, X3, X3; VMOVSHDUP X3, X4; VADDSS X4, X3, X3

    VMOVSS X0, (R11)
    VMOVSS X1, 4(R11)
    VMOVSS X2, 8(R11)
    VMOVSS X3, 12(R11)

    VZEROUPPER
    RET
