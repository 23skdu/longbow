//go:build amd64

#include "textflag.h"

// func dequantL2Float32Uint8AVX2Kernel(q, v unsafe.Pointer, n int, minV, scale float32) float32
TEXT ·dequantL2Float32Uint8AVX2Kernel(SB), NOSPLIT, $0-36
	MOVQ        q+0(FP), AX
	MOVQ        v+8(FP), CX
	MOVQ        n+16(FP), DX
	VBROADCASTSS minV+24(FP), Y4
	VBROADCASTSS scale+28(FP), Y5

	VXORPS      Y6, Y6, Y6
	VXORPS      Y7, Y7, Y7
	VXORPS      Y8, Y8, Y8
	VXORPS      Y9, Y9, Y9

loop32:
	CMPQ        DX, $32
	JL          loop8

	VPMOVZXBD   (CX), Y0
	VCVTDQ2PS   Y0, Y0
	VFMADD213PS Y4, Y5, Y0
	VMOVUPS     (AX), Y10
	VSUBPS      Y0, Y10, Y0
	VFMADD231PS Y0, Y0, Y6

	VPMOVZXBD   8(CX), Y1
	VCVTDQ2PS   Y1, Y1
	VFMADD213PS Y4, Y5, Y1
	VMOVUPS     32(AX), Y10
	VSUBPS      Y1, Y10, Y1
	VFMADD231PS Y1, Y1, Y7

	VPMOVZXBD   16(CX), Y2
	VCVTDQ2PS   Y2, Y2
	VFMADD213PS Y4, Y5, Y2
	VMOVUPS     64(AX), Y10
	VSUBPS      Y2, Y10, Y2
	VFMADD231PS Y2, Y2, Y8

	VPMOVZXBD   24(CX), Y3
	VCVTDQ2PS   Y3, Y3
	VFMADD213PS Y4, Y5, Y3
	VMOVUPS     96(AX), Y10
	VSUBPS      Y3, Y10, Y3
	VFMADD231PS Y3, Y3, Y9

	ADDQ        $32, CX
	ADDQ        $128, AX
	SUBQ        $32, DX
	JMP         loop32

loop8:
	CMPQ        DX, $8
	JL          reduce

	VPMOVZXBD   (CX), Y0
	VCVTDQ2PS   Y0, Y0
	VFMADD213PS Y4, Y5, Y0
	VMOVUPS     (AX), Y10
	VSUBPS      Y0, Y10, Y0
	VFMADD231PS Y0, Y0, Y6

	ADDQ        $8, CX
	ADDQ        $32, AX
	SUBQ        $8, DX
	JMP         loop8

reduce:
	VADDPS      Y7, Y6, Y6
	VADDPS      Y9, Y8, Y8
	VADDPS      Y8, Y6, Y6

	VEXTRACTF128 $0x01, Y6, X0
	VADDPS      X0, X6, X6
	VMOVHLPS    X6, X6, X0
	VADDPS      X0, X6, X6
	VMOVSHDUP   X6, X0
	VADDSS      X0, X6, X6

tail:
	TESTQ       DX, DX
	JZ          done

tail_loop:
	MOVBLZX     (CX), BX
	CVTSL2SS    BX, X0
	MULSS       X5, X0
	ADDSS       X4, X0
	MOVSS       (AX), X1
	SUBSS       X0, X1
	MULSS       X1, X1
	ADDSS       X1, X6

	INCQ        CX
	ADDQ        $4, AX
	DECQ        DX
	JNZ         tail_loop

done:
	MOVSS       X6, ret+32(FP)
	VZEROUPPER
	RET
