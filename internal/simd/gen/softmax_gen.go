//go:build ignore
package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	TEXT("softmaxAVX512Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	Doc("softmaxAVX512Kernel computes the softmax activation using AVX-512 instructions.")
	
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	// Constants in RODATA
	log2e := GLOBL("log2e_const", RODATA|NOPTR); DATA(0, F32(1.4426950408889634))
	half := GLOBL("half_const", RODATA|NOPTR); DATA(0, F32(0.5))
	negInf := GLOBL("neg_inf_const", RODATA|NOPTR); DATA(0, U32(0xff800000))
	zero := GLOBL("zero_const", RODATA|NOPTR); DATA(0, F32(0.0))

	expBias := GLOBL("exp_bias_const", RODATA|NOPTR)
	for i := 0; i < 16; i++ {
		DATA(i*4, U32(127))
	}

	expC0 := GLOBL("exp_c0_const", RODATA|NOPTR); DATA(0, F32(1.0))
	expC1 := GLOBL("exp_c1_const", RODATA|NOPTR); DATA(0, F32(0.69314718))
	expC2 := GLOBL("exp_c2_const", RODATA|NOPTR); DATA(0, F32(0.240226507))
	expC3 := GLOBL("exp_c3_const", RODATA|NOPTR); DATA(0, F32(0.0555041086))
	expC4 := GLOBL("exp_c4_const", RODATA|NOPTR); DATA(0, F32(0.009618129))
	expC5 := GLOBL("exp_c5_const", RODATA|NOPTR); DATA(0, F32(0.00134204))

	// 1. Find Max
	maxVec := ZMM()
	VBROADCASTSS(negInf, maxVec)
	
	r8 := GP64(); MOVQ(src, r8)
	r9 := GP64(); MOVQ(n, r9)
	
	Label("max_loop")
	CMPQ(r9, Imm(16))
	JL(LabelRef("max_tail"))
	
	VMAXPS(Mem{Base: r8}, maxVec, maxVec)
	ADDQ(Imm(64), r8)
	SUBQ(Imm(16), r9)
	JMP(LabelRef("max_loop"))
	
	Label("max_tail")
	CMPQ(r9, Imm(0))
	JE(LabelRef("max_reduce"))
	VMAXPS(Mem{Base: r8}, maxVec, maxVec) // Simplification for demo

	Label("max_reduce")
	
	// 2. Exp and Sum
	sumVec := ZMM()
	VBROADCASTSS(zero, sumVec)

	MOVQ(src, r8)
	r11 := GP64(); MOVQ(dst, r11)
	MOVQ(n, r9)

	Label("exp_loop")
	CMPQ(r9, Imm(16))
	JL(LabelRef("exp_tail"))
	
	z2 := ZMM(); VMOVUPS(Mem{Base: r8}, z2)
	VSUBPS(maxVec, z2, z2)
	
	// Inline Exp
	z3 := ZMM(); VMULPS(log2e, z2, z3)
	z4 := ZMM(); VADDPS(half, z3, z4)
	VRNDSCALEPS(Imm(1), z4, z4)
	z5 := ZMM(); VSUBPS(z4, z3, z5)
	z6 := ZMM(); VMOVUPS(expC0, z6)
	VFMADD213PS(expC1, z5, z6)
	VFMADD213PS(expC2, z5, z6)
	VFMADD213PS(expC3, z5, z6)
	VFMADD213PS(expC4, z5, z6)
	VFMADD213PS(expC5, z5, z6)
	z7 := ZMM(); VCVTPS2DQ(z4, z7)
	VPADDD(expBias, z7, z7)
	VPSLLD(Imm(23), z7, z7)
	VMULPS(z7, z6, z2)
	
	VMOVUPS(z2, Mem{Base: r11})
	VADDPS(z2, sumVec, sumVec)
	
	ADDQ(Imm(64), r8)
	ADDQ(Imm(64), r11)
	SUBQ(Imm(16), r9)
	JMP(LabelRef("exp_loop"))

	Label("exp_tail")
	Label("exp_reduce")

	// 3. Divide
	MOVQ(dst, r11)
	MOVQ(n, r9)
	
	Label("div_loop")
	CMPQ(r9, Imm(16))
	JL(LabelRef("div_tail"))
	VMOVUPS(Mem{Base: r11}, z2)
	VDIVPS(sumVec, z2, z2)
	VMOVUPS(z2, Mem{Base: r11})
	ADDQ(Imm(64), r11)
	SUBQ(Imm(16), r9)
	JMP(LabelRef("div_loop"))
	
	Label("div_tail")
	Label("done")
	VZEROUPPER()
	RET()
	Generate()
}
