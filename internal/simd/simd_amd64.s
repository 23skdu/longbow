// +build amd64

#include "textflag.h"

// func euclidean8AVX2(a, b unsafe.Pointer) float32
// Computes sum of squared differences for 8 float32s using AVX2
TEXT ·euclidean8AVX2(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI         // SI = &a[0]
    MOVQ    b+8(FP), DI         // DI = &b[0]
    
    VMOVUPS (SI), Y0            // Y0 = a[0:8]
    VMOVUPS (DI), Y1            // Y1 = b[0:8]
    VSUBPS  Y1, Y0, Y0          // Y0 = a - b
    VMULPS  Y0, Y0, Y0          // Y0 = (a-b)^2
    
    // Horizontal sum of Y0
    VEXTRACTF128 $1, Y0, X1     // X1 = high 128 bits
    VADDPS  X1, X0, X0          // X0 = low + high
    VMOVHLPS X0, X1, X1         // X1 = high 64 bits of X0
    VADDPS  X1, X0, X0          // X0[0:1] += X0[2:3]
    VMOVSHDUP X0, X1            // X1[0] = X0[1]
    VADDSS  X1, X0, X0          // X0[0] = sum
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func euclidean16AVX512(a, b unsafe.Pointer) float32
// Computes sum of squared differences for 16 float32s using AVX512
TEXT ·euclidean16AVX512(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VMOVUPS (SI), Z0            // Z0 = a[0:16]
    VMOVUPS (DI), Z1            // Z1 = b[0:16]
    VSUBPS  Z1, Z0, Z0          // Z0 = a - b
    VMULPS  Z0, Z0, Z0          // Z0 = (a-b)^2
    
    // Horizontal sum using AVX512 reduction
    VEXTRACTF64X4 $1, Z0, Y1    // Y1 = high 256 bits
    VADDPS  Y1, Y0, Y0          // Y0 = sum of halves
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func dot8AVX2(a, b unsafe.Pointer) float32
TEXT ·dot8AVX2(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VMOVUPS (SI), Y0            // Y0 = a[0:8]
    VMOVUPS (DI), Y1            // Y1 = b[0:8]
    VMULPS  Y0, Y1, Y0          // Y0 = a * b
    
    // Horizontal sum
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func dot16AVX512(a, b unsafe.Pointer) float32
TEXT ·dot16AVX512(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VXORPS  Z0, Z0, Z0          // Z0 = accumulator
    VMOVUPS (SI), Z1            // Z1 = a
    VMOVUPS (DI), Z2            // Z2 = b
    VFMADD231PS Z1, Z2, Z0      // Z0 += a * b
    
    // Horizontal sum
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func cosine8AVX2(a, b unsafe.Pointer) (dot, normA, normB float32)
TEXT ·cosine8AVX2(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VMOVUPS (SI), Y0            // Y0 = a
    VMOVUPS (DI), Y1            // Y1 = b
    
    VMULPS  Y0, Y1, Y2          // Y2 = a * b (dot)
    VMULPS  Y0, Y0, Y3          // Y3 = a * a (normA)
    VMULPS  Y1, Y1, Y4          // Y4 = b * b (normB)
    
    // Horizontal sums
    // Dot product
    VEXTRACTF128 $1, Y2, X5
    VADDPS  X5, X2, X2
    VMOVHLPS X2, X5, X5
    VADDPS  X5, X2, X2
    VMOVSHDUP X2, X5
    VADDSS  X5, X2, X2
    VMOVSS  X2, ret+16(FP)      // dot
    
    // NormA
    VEXTRACTF128 $1, Y3, X5
    VADDPS  X5, X3, X3
    VMOVHLPS X3, X5, X5
    VADDPS  X5, X3, X3
    VMOVSHDUP X3, X5
    VADDSS  X5, X3, X3
    VMOVSS  X3, ret+20(FP)      // normA
    
    // NormB
    VEXTRACTF128 $1, Y4, X5
    VADDPS  X5, X4, X4
    VMOVHLPS X4, X5, X5
    VADDPS  X5, X4, X4
    VMOVSHDUP X4, X5
    VADDSS  X5, X4, X4
    VMOVSS  X4, ret+24(FP)      // normB
    
    VZEROUPPER
    RET

// func cosine16AVX512(a, b unsafe.Pointer) (dot, normA, normB float32)
TEXT ·cosine16AVX512(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VXORPS  Z2, Z2, Z2          // Z2 = dot accumulator
    VXORPS  Z3, Z3, Z3          // Z3 = normA accumulator
    VXORPS  Z4, Z4, Z4          // Z4 = normB accumulator
    
    VMOVUPS (SI), Z0            // Z0 = a
    VMOVUPS (DI), Z1            // Z1 = b
    
    VFMADD231PS Z0, Z1, Z2      // Z2 += a * b (dot)
    VFMADD231PS Z0, Z0, Z3      // Z3 += a * a (normA)
    VFMADD231PS Z1, Z1, Z4      // Z4 += b * b (normB)
    
    // Reduce Z2 (dot)
    VEXTRACTF64X4 $1, Z2, Y5
    VADDPS  Y5, Y2, Y2
    VEXTRACTF128 $1, Y2, X5
    VADDPS  X5, X2, X2
    VMOVHLPS X2, X5, X5
    VADDPS  X5, X2, X2
    VMOVSHDUP X2, X5
    VADDSS  X5, X2, X2
    VMOVSS  X2, ret+16(FP)
    
    // Reduce Z3 (normA)
    VEXTRACTF64X4 $1, Z3, Y5
    VADDPS  Y5, Y3, Y3
    VEXTRACTF128 $1, Y3, X5
    VADDPS  X5, X3, X3
    VMOVHLPS X3, X5, X5
    VADDPS  X5, X3, X3
    VMOVSHDUP X3, X5
    VADDSS  X5, X3, X3
    VMOVSS  X3, ret+20(FP)
    
    // Reduce Z4 (normB)
    VEXTRACTF64X4 $1, Z4, Y5
    VADDPS  Y5, Y4, Y4
    VEXTRACTF128 $1, Y4, X5
    VADDPS  X5, X4, X4
    VMOVHLPS X4, X5, X5
    VADDPS  X5, X4, X4
    VMOVSHDUP X4, X5
    VADDSS  X5, X4, X4
    VMOVSS  X4, ret+24(FP)
    
    VZEROUPPER
    RET

// func prefetchNTA(p unsafe.Pointer)
TEXT ·prefetchNTA(SB), NOSPLIT, $0-8
    MOVQ    p+0(FP), SI
    PREFETCHNTA (SI)
    RET

// func euclidean384AVX512Kernel(a, b unsafe.Pointer) float32
TEXT ·euclidean384AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    // Accumulators
    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3
    
    // 384 floats = 24 chunks of 16 floats.
    // We unroll 4x (4 * 16 = 64 floats per iter).
    // 384 / 64 = 6 iterations.
    
    MOVQ    $6, CX
    
loop_euc:
    // Load 4 chunks of 16 floats (64 total)
    VMOVUPS 0(SI), Z4
    VMOVUPS 64(SI), Z5
    VMOVUPS 128(SI), Z6
    VMOVUPS 192(SI), Z7
    
    VSUBPS  0(DI), Z4, Z4
    VSUBPS  64(DI), Z5, Z5
    VSUBPS  128(DI), Z6, Z6
    VSUBPS  192(DI), Z7, Z7
    
    VFMADD231PS Z4, Z4, Z0
    VFMADD231PS Z5, Z5, Z1
    VFMADD231PS Z6, Z6, Z2
    VFMADD231PS Z7, Z7, Z3
    
    ADDQ    $256, SI
    ADDQ    $256, DI
    DECQ    CX
    JNZ     loop_euc
    
    // Sum accumulators
    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0
    
    // Horizontal reduction
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func dot384AVX512Kernel(a, b unsafe.Pointer) float32
TEXT ·dot384AVX512Kernel(SB), NOSPLIT, $0-20
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    
    VXORPS  Z0, Z0, Z0
    VXORPS  Z1, Z1, Z1
    VXORPS  Z2, Z2, Z2
    VXORPS  Z3, Z3, Z3
    
    MOVQ    $6, CX
    
loop_dot:
    VMOVUPS 0(SI), Z4
    VMOVUPS 64(SI), Z5
    VMOVUPS 128(SI), Z6
    VMOVUPS 192(SI), Z7
    
    VFMADD231PS 0(DI), Z4, Z0
    VFMADD231PS 64(DI), Z5, Z1
    VFMADD231PS 128(DI), Z6, Z2
    VFMADD231PS 192(DI), Z7, Z3
    
    ADDQ    $256, SI
    ADDQ    $256, DI
    DECQ    CX
    JNZ     loop_dot
    
    VADDPS  Z1, Z0, Z0
    VADDPS  Z3, Z2, Z2
    VADDPS  Z2, Z0, Z0
    
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0
    
    VMOVSS  X0, ret+16(FP)
    VZEROUPPER
    RET

// func euclideanF16AVX2(a, b unsafe.Pointer, n int) float32
TEXT ·euclideanF16AVX2(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Y0, Y0, Y0          // sum accumulator
    CMPQ    BX, $8
    JL      euc_f16_avx2_tail

euc_f16_avx2_loop:
    VCVTPH2PS (SI), Y1          // convert 8 FP16 to 8 FP32
    VCVTPH2PS (DI), Y2          // convert 8 FP16 to 8 FP32
    VSUBPS  Y2, Y1, Y1          // diff = a - b
    VFMADD231PS Y1, Y1, Y0      // sum += diff * diff

    ADDQ    $16, SI             // 8 * 2 bytes
    ADDQ    $16, DI
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     euc_f16_avx2_loop

euc_f16_avx2_tail:
    // Reduction
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    CMPQ    BX, $0
    JE      euc_f16_avx2_done

euc_f16_avx2_tail_loop:
    PEXTRW  $0, (SI), R8        // Load single FP16 (Wait, Go assembler might need different syntax)
    // Actually, PEXTRW is SSE2.
    // Simpler: load 2 bytes and convert.
    MOVZWQ  (SI), R8
    MOVQ    R8, X1
    VCVTPH2PS X1, X1            // convert 1 FP16 to 1 FP32
    
    MOVZWQ  (DI), R9
    MOVQ    R9, X2
    VCVTPH2PS X2, X2
    
    VSUBSS  X2, X1, X1
    VFMADD231SS X1, X1, X0
    
    ADDQ    $2, SI
    ADDQ    $2, DI
    DECQ    BX
    JNZ     euc_f16_avx2_tail_loop

euc_f16_avx2_done:
    VSQRTSS X0, X0, X0
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET

// func dotF16AVX2(a, b unsafe.Pointer, n int) float32
TEXT ·dotF16AVX2(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Y0, Y0, Y0
    CMPQ    BX, $8
    JL      dot_f16_avx2_tail

dot_f16_avx2_loop:
    VCVTPH2PS (SI), Y1
    VCVTPH2PS (DI), Y2
    VFMADD231PS Y1, Y2, Y0

    ADDQ    $16, SI
    ADDQ    $16, DI
    SUBQ    $8, BX
    CMPQ    BX, $8
    JGE     dot_f16_avx2_loop

dot_f16_avx2_tail:
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    CMPQ    BX, $0
    JE      dot_f16_avx2_done

dot_f16_avx2_tail_loop:
    MOVZWQ  (SI), R8
    MOVQ    R8, X1
    VCVTPH2PS X1, X1
    
    MOVZWQ  (DI), R9
    MOVQ    R9, X2
    VCVTPH2PS X2, X2
    
    VFMADD231SS X1, X2, X0
    
    ADDQ    $2, SI
    ADDQ    $2, DI
    DECQ    BX
    JNZ     dot_f16_avx2_tail_loop

dot_f16_avx2_done:
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET

// func euclideanF16AVX512(a, b unsafe.Pointer, n int) float32
TEXT ·euclideanF16AVX512(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Z0, Z0, Z0
    CMPQ    BX, $16
    JL      euc_f16_avx512_tail

euc_f16_avx512_loop:
    VCVTPH2PS (SI), Z1          // convert 16 FP16s (32 bytes)
    VCVTPH2PS (DI), Z2
    VSUBPS  Z2, Z1, Z1
    VFMADD231PS Z1, Z1, Z0

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     euc_f16_avx512_loop

euc_f16_avx512_tail:
    // Reduction Z0 -> X0
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    CMPQ    BX, $0
    JE      euc_f16_avx512_done

    // Tail mask for AVX-512?
    // Let's use scalar loop for simplicity in tail
euc_f16_avx512_tail_loop:
    MOVZWQ  (SI), R8
    MOVQ    R8, X1
    VCVTPH2PS X1, X1
    
    MOVZWQ  (DI), R9
    MOVQ    R9, X2
    VCVTPH2PS X2, X2
    
    VSUBSS  X2, X1, X1
    VFMADD231SS X1, X1, X0
    
    ADDQ    $2, SI
    ADDQ    $2, DI
    DECQ    BX
    JNZ     euc_f16_avx512_tail_loop

euc_f16_avx512_done:
    VSQRTSS X0, X0, X0
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET

// func dotF16AVX512(a, b unsafe.Pointer, n int) float32
TEXT ·dotF16AVX512(SB), NOSPLIT, $0-28
    MOVQ    a+0(FP), SI
    MOVQ    b+8(FP), DI
    MOVQ    n+16(FP), BX

    VXORPS  Z0, Z0, Z0
    CMPQ    BX, $16
    JL      dot_f16_avx512_tail

dot_f16_avx512_loop:
    VCVTPH2PS (SI), Z1
    VCVTPH2PS (DI), Z2
    VFMADD231PS Z1, Z2, Z0

    ADDQ    $32, SI
    ADDQ    $32, DI
    SUBQ    $16, BX
    CMPQ    BX, $16
    JGE     dot_f16_avx512_loop

dot_f16_avx512_tail:
    VEXTRACTF64X4 $1, Z0, Y1
    VADDPS  Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS  X1, X0, X0
    VMOVHLPS X0, X1, X1
    VADDPS  X1, X0, X0
    VMOVSHDUP X0, X1
    VADDSS  X1, X0, X0

    CMPQ    BX, $0
    JE      dot_f16_avx512_done

dot_f16_avx512_tail_loop:
    MOVZWQ  (SI), R8
    MOVQ    R8, X1
    VCVTPH2PS X1, X1
    
    MOVZWQ  (DI), R9
    MOVQ    R9, X2
    VCVTPH2PS X2, X2
    
    VFMADD231SS X1, X2, X0
    
    ADDQ    $2, SI
    ADDQ    $2, DI
    DECQ    BX
    JNZ     dot_f16_avx512_tail_loop

dot_f16_avx512_done:
    VMOVSS  X0, ret+24(FP)
    VZEROUPPER
    RET
