// +build amd64

#include "textflag.h"

// ----------------------------------------------------------------------------
// func adcBatchAVX2Kernel(table, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)
// ----------------------------------------------------------------------------
TEXT ·adcBatchAVX2Kernel(SB), NOSPLIT, $0-40
    MOVQ    table+0(FP), DI
    MOVQ    codes+8(FP), SI
    MOVQ    m+16(FP), DX
    MOVQ    results+24(FP), R8
    MOVQ    n+32(FP), R9

    CMPQ    R9, $8
    JL      tail_start

loop_8_vectors:
    VXORPS  Y0, Y0, Y0
    MOVQ    $0, CX

 subspace_loop:
    CMPQ    CX, DX
    JGE     subspace_done

    MOVQ    SI, R10
    ADDQ    CX, R10

    // Load 8 indices
    // This part is the bottleneck: gathering indices from 8 vectors
    MOVQ    $0, R11
    MOVB    (R10), R11L
    VPINSRD $0, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $1, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $2, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $3, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $4, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $5, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $6, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $7, R11, X1, X1

    // table_base
    MOVQ    CX, R10
    SHLQ    $10, R10
    ADDQ    DI, R10

    VPCMPEQD Y3, Y3, Y3
    VPGATHERDD (R10)(Y1*4), Y3, Y2
    VADDPS  Y2, Y0, Y0

    INCQ    CX
    JMP     subspace_loop

subspace_done:
    VMOVUPS Y0, (R8)
    ADDQ    $32, R8
    
    MOVQ    DX, R10
    SHLQ    $3, R10 // 8 * m
    ADDQ    R10, SI
    SUBQ    $8, R9
    CMPQ    R9, $8
    JGE     loop_8_vectors

tail_start:
    CMPQ    R9, $0
    JE      done

tail_vector_loop:
    VXORPS  X0, X0, X0
    MOVQ    $0, CX

tail_subspace_loop:
    CMPQ    CX, DX
    JGE     tail_subspace_done
    MOVQ    $0, R10
    MOVB    (SI)(CX*1), R10L
    MOVQ    CX, R11
    SHLQ    $10, R11
    ADDQ    DI, R11
    VMOVSS  (R11)(R10*4), X1
    VADDSS  X1, X0, X0
    INCQ    CX
    JMP     tail_subspace_loop

tail_subspace_done:
    VMOVSS  X0, (R8)
    ADDQ    $4, R8
    ADDQ    DX, SI
    DECQ    R9
    JNZ     tail_vector_loop

done:
    VZEROUPPER
    RET

// ----------------------------------------------------------------------------
// func adcBatchAVX512Kernel(table, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)
// ----------------------------------------------------------------------------
TEXT ·adcBatchAVX512Kernel(SB), NOSPLIT, $0-40
    MOVQ    table+0(FP), DI
    MOVQ    codes+8(FP), SI
    MOVQ    m+16(FP), DX
    MOVQ    results+24(FP), R8
    MOVQ    n+32(FP), R9

    CMPQ    R9, $16
    JL      tail512_check_8

loop_16_vectors:
    VXORPS  Z0, Z0, Z0
    MOVQ    $0, CX

subspace512_loop:
    CMPQ    CX, DX
    JGE     subspace512_done

    MOVQ    SI, R10
    ADDQ    CX, R10

    // Load 16 indices explicitly
    // (Optimized if m was known, but DX is variable)
    MOVQ    $0, R11
    MOVB    (R10), R11L
    VPINSRD $0, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $1, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $2, R11, X1, X1
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $3, R11, X1, X1

    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $0, R11, X2, X2
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $1, R11, X2, X2
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $2, R11, X2, X2
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $3, R11, X2, X2

    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $0, R11, X3, X3
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $1, R11, X3, X3
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $2, R11, X3, X3
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $3, R11, X3, X3

    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $0, R11, X4, X4
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $1, R11, X4, X4
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $2, R11, X4, X4
    ADDQ    DX, R10
    MOVB    (R10), R11L
    VPINSRD $3, R11, X4, X4

    VINSERTI128 $1, X2, Y1, Y1
    VINSERTI128 $1, X4, Y3, Y3
    VINSERTI64X4 $1, Y3, Z1, Z1 // Z1 has 16 indices

    // table_base
    MOVQ    CX, R10
    SHLQ    $10, R10
    ADDQ    DI, R10

    MOVW    $0xFFFF, R11
    KMOVW   R11, K1
    VPGATHERDD (R10)(Z1*4), K1, Z2
    VADDPS  Z2, Z0, Z0

    INCQ    CX
    JMP     subspace512_loop

subspace512_done:
    // Store 16 results (Z0 contains 16 float32 sums)
    VMOVUPS Z0, (R8)
    ADDQ    $64, R8
    
    // Advance codes ptr by 16 vectors
    MOVQ    DX, R10
    SHLQ    $4, R10 // 16 * m
    ADDQ    R10, SI
    
    SUBQ    $16, R9
    CMPQ    R9, $16
    JGE     loop_16_vectors

tail512_check_8:
    JMP ·adcBatchAVX2Kernel(SB)

// ----------------------------------------------------------------------------
// func adcBatchVNNIKernel(table, codes unsafe.Pointer, m int, results unsafe.Pointer, n int)
//
// Optimized for VNNI. Expects quantized uint8 table.
// ----------------------------------------------------------------------------
TEXT ·adcBatchVNNIKernel(SB), NOSPLIT, $0-40
    // Currently VNNI is implemented via AVX-512 fallback
    // as it requires specific data layout for maximum speed (interleaved subspaces).
    JMP ·adcBatchAVX512Kernel(SB)
