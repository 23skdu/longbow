//go:build amd64

#include "textflag.h"

// func clearSIMDAVX2(bits []uint64)
TEXT ·clearSIMDAVX2(SB), NOSPLIT, $0-24
    MOVQ bits+0(FP), AX    // bits pointer
    MOVQ bits+8(FP), CX    // len(bits)
    
    TESTQ CX, CX
    JZ done
    
    VPXOR Y0, Y0, Y0       // Zero register (256 bits)
    
    // Process 4 uint64s (256 bits) at a time
loop:
    CMPQ CX, $4
    JL remainder
    
    VMOVDQU Y0, (AX)       // Store 256 bits
    ADDQ $32, AX           // Advance 32 bytes
    SUBQ $4, CX
    JMP loop
    
remainder:
    // Handle remaining words with scalar stores
    TESTQ CX, CX
    JZ done
    
scalar_loop:
    MOVQ $0, (AX)
    ADDQ $8, AX
    DECQ CX
    JNZ scalar_loop
    
done:
    VZEROUPPER             // Clear upper 128 bits of YMM registers
    RET

// func andNotSIMDAVX2(result, a, b []uint64)
TEXT ·andNotSIMDAVX2(SB), NOSPLIT, $0-72
    MOVQ result+0(FP), DI  // result pointer
    MOVQ a+24(FP), SI      // a pointer
    MOVQ b+48(FP), DX      // b pointer
    MOVQ result+8(FP), CX  // len(result)
    
    TESTQ CX, CX
    JZ done_andnot
    
loop_andnot:
    CMPQ CX, $4
    JL remainder_andnot
    
    VMOVDQU (DX), Y0       // Load 4 uint64s from b
    VMOVDQU (SI), Y1       // Load 4 uint64s from a  
    VPANDN Y1, Y0, Y2      // Y2 = ~b & a (VPANDN does NOT(Y0) AND Y1)
    VMOVDQU Y2, (DI)       // Store result
    
    ADDQ $32, SI
    ADDQ $32, DX
    ADDQ $32, DI
    SUBQ $4, CX
    JMP loop_andnot
    
remainder_andnot:
    TESTQ CX, CX
    JZ done_andnot
    
scalar_loop_andnot:
    MOVQ (SI), R8
    MOVQ (DX), R9
    NOTQ R9
    ANDQ R8, R9
    MOVQ R9, (DI)
    
    ADDQ $8, SI
    ADDQ $8, DX
    ADDQ $8, DI
    DECQ CX
    JNZ scalar_loop_andnot
    
done_andnot:
    VZEROUPPER
    RET

// func popCountPOPCNT(bits []uint64) int
TEXT ·popCountPOPCNT(SB), NOSPLIT, $0-32
    MOVQ bits+0(FP), AX    // bits pointer
    MOVQ bits+8(FP), CX    // len(bits)
    XORQ DX, DX            // count = 0
    
    TESTQ CX, CX
    JZ done_popcount
    
loop_popcount:
    POPCNTQ (AX), R8
    ADDQ R8, DX
    ADDQ $8, AX
    DECQ CX
    JNZ loop_popcount
    
done_popcount:
    MOVQ DX, ret+24(FP)
    RET
