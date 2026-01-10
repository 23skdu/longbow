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
