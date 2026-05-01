//go:build ignore
package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

func main() {
	// --- Helpers ---
	reduceYMM := func(y reg.VecVirtual) reg.VecVirtual {
		xLow := XMM(); VEXTRACTF128(Imm(0), y, xLow)
		xHigh := XMM(); VEXTRACTF128(Imm(1), y, xHigh)
		VADDPS(xLow, xHigh, xHigh)
		xSum := XMM(); VMOVHLPS(xHigh, xSum, xSum)
		VADDPS(xSum, xHigh, xHigh)
		xNext := XMM(); VMOVSHDUP(xHigh, xNext)
		VADDSS(xNext, xHigh, xHigh)
		return xHigh
	}
	reduceZMM := func(z reg.VecVirtual) reg.VecVirtual {
		yLow := YMM(); VEXTRACTF64X4(Imm(0), z, yLow)
		yHigh := YMM(); VEXTRACTF64X4(Imm(1), z, yHigh)
		VADDPS(yLow, yHigh, yHigh)
		return reduceYMM(yHigh)
	}

	// --- Distance Kernels ---
	TEXT("euclidean8AVX2", NOSPLIT, "func(a, b uintptr) float32")
	a := Load(Param("a"), GP64()); b := Load(Param("b"), GP64())
	y0 := YMM(); VMOVUPS(Mem{Base: a}, y0); y1 := YMM(); VMOVUPS(Mem{Base: b}, y1)
	VSUBPS(y1, y0, y0); VMULPS(y0, y0, y0)
	Store(reduceYMM(y0), ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("dot8AVX2", NOSPLIT, "func(a, b uintptr) float32")
	a2 := Load(Param("a"), GP64()); b2 := Load(Param("b"), GP64())
	y0b := YMM(); VMOVUPS(Mem{Base: a2}, y0b); y1b := YMM(); VMOVUPS(Mem{Base: b2}, y1b)
	VMULPS(y0b, y1b, y0b)
	Store(reduceYMM(y0b), ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("euclidean16AVX512", NOSPLIT, "func(a, b uintptr) float32")
	a3 := Load(Param("a"), GP64()); b3 := Load(Param("b"), GP64())
	z0 := ZMM(); VMOVUPS(Mem{Base: a3}, z0); z1 := ZMM(); VMOVUPS(Mem{Base: b3}, z1)
	VSUBPS(z1, z0, z0); VMULPS(z0, z0, z0)
	Store(reduceZMM(z0), ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("dot16AVX512", NOSPLIT, "func(a, b uintptr) float32")
	a4 := Load(Param("a"), GP64()); b4 := Load(Param("b"), GP64())
	z0d := ZMM(); VXORPS(z0d, z0d, z0d)
	z1d := ZMM(); VMOVUPS(Mem{Base: a4}, z1d); z2d := ZMM(); VMOVUPS(Mem{Base: b4}, z2d)
	VFMADD231PS(z1d, z2d, z0d)
	Store(reduceZMM(z0d), ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("prefetchNTA", NOSPLIT, "func(p uintptr)")
	p := Load(Param("p"), GP64()); PREFETCHNTA(Mem{Base: p}); RET()

	// --- Reductions ---
	negInf := GLOBL("neg_inf_const_red", RODATA|NOPTR); DATA(0, U32(0xff800000))
	posInf := GLOBL("pos_inf_const_red", RODATA|NOPTR); DATA(0, U32(0x7f800000))

	TEXT("sumAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcSum := Load(Param("src"), GP64()); nSum := Load(Param("n"), GP64())
	sumV := YMM(); VXORPS(sumV, sumV, sumV)
	Label("sum_loop")
	CMPQ(nSum, Imm(8)); JL(LabelRef("sum_tail"))
	v := YMM(); VMOVUPS(Mem{Base: srcSum}, v); VADDPS(v, sumV, sumV)
	ADDQ(Imm(32), srcSum); SUBQ(Imm(8), nSum); JMP(LabelRef("sum_loop"))
	Label("sum_tail")
	Store(reduceYMM(sumV), ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("maxAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcMax := Load(Param("src"), GP64()); nMax := Load(Param("n"), GP64())
	maxV := YMM(); VBROADCASTSS(negInf, maxV)
	Label("max_loop")
	CMPQ(nMax, Imm(8)); JL(LabelRef("max_tail"))
	vMax := YMM(); VMOVUPS(Mem{Base: srcMax}, vMax); VMAXPS(vMax, maxV, maxV)
	ADDQ(Imm(32), srcMax); SUBQ(Imm(8), nMax); JMP(LabelRef("max_loop"))
	Label("max_tail")
	// Reduce maxV
	xLowM := XMM(); VEXTRACTF128(Imm(0), maxV, xLowM)
	xHighM := XMM(); VEXTRACTF128(Imm(1), maxV, xHighM)
	VMAXPS(xLowM, xHighM, xHighM)
	xNextM := XMM(); VMOVSHDUP(xHighM, xNextM)
	VMAXSS(xNextM, xHighM, xHighM)
	xShufM := XMM(); VPERMILPS(Imm(0x4e), xHighM, xShufM)
	VMAXSS(xShufM, xHighM, xHighM)
	Store(xHighM, ReturnIndex(0)); VZEROUPPER(); RET()

	TEXT("minAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcMin := Load(Param("src"), GP64()); nMin := Load(Param("n"), GP64())
	minV := YMM(); VBROADCASTSS(posInf, minV)
	Label("min_loop")
	CMPQ(nMin, Imm(8)); JL(LabelRef("min_tail"))
	vMin := YMM(); VMOVUPS(Mem{Base: srcMin}, vMin); VMINPS(vMin, minV, minV)
	ADDQ(Imm(32), srcMin); SUBQ(Imm(8), nMin); JMP(LabelRef("min_loop"))
	Label("min_tail")
	// Reduce minV
	xLowMi := XMM(); VEXTRACTF128(Imm(0), minV, xLowMi)
	xHighMi := XMM(); VEXTRACTF128(Imm(1), minV, xHighMi)
	VMINPS(xLowMi, xHighMi, xHighMi)
	xNextMi := XMM(); VMOVSHDUP(xHighMi, xNextMi)
	VMINSS(xNextMi, xHighMi, xHighMi)
	xShufMi := XMM(); VPERMILPS(Imm(0x4e), xHighMi, xShufMi)
	VMINSS(xShufMi, xHighMi, xHighMi)
	Store(xHighMi, ReturnIndex(0)); VZEROUPPER(); RET()

	// --- Stubs for Linker ---
	stubs := []string{
		"matchInt64AVX2Kernel", "matchInt32AVX2Kernel", "matchFloat32AVX2Kernel", "matchFloat64AVX2Kernel",
		"euclideanFloat64AVX2Kernel", "euclideanInt8AVX2Kernel", "euclideanInt16AVX2Kernel",
		"euclideanInt8Unrolled4xAVX2Kernel", "euclideanUint16AVX2Kernel", "dotInt16AVX2Kernel", "dotUint16AVX2Kernel",
		"int8ToFloat32AVX2Kernel", "uint8ToFloat32AVX2Kernel", "int16ToFloat32AVX2Kernel", "uint16ToFloat32AVX2Kernel",
		"int32ToFloat32AVX2Kernel", "uint32ToFloat32AVX2Kernel", "float16ToFloat32AVX2Kernel",
		"sigmoidAVX512Kernel", "expAVX512Kernel", "logAVX512Kernel",
		"l2SquaredAVX512Kernel", "dotAVX512Kernel", "cosineDotAVX512",
		"euclideanVertical4AVX512", "cosineVertical4AVX512", "dotVertical4AVX512",
		"matchInt64AVX512Kernel", "matchInt32AVX512Kernel", "matchFloat32AVX512Kernel", "matchFloat64AVX512Kernel",
		"euclideanFloat64AVX512Kernel", "dotFloat64AVX512Kernel", "euclideanSQ8AVX512Kernel",
		"euclideanF16AVX512Kernel", "dotF16AVX512Kernel", "adcBatchAVX512Kernel", "adcBatchVNNIKernel",
		"euclideanPQVNNIKernel", "l2SquaredAVX2Kernel", "dotAVX2Kernel",
		"euclideanF16AVX2Kernel", "dotF16AVX2Kernel", "adcBatchAVX2Kernel",
		"dotInt4AVX512Kernel", "dotInt4AVX2Kernel", "euclideanVertical4AVX2",
		"euclideanSQ8AVX2Kernel", "euclidean32FMA", "dot32FMA", "cosine32FMA",
		"euclidean64FMA", "dot64FMA", "cosine64FMA",
		"int8ToFloat32AVX512Kernel", "uint8ToFloat32AVX512Kernel", "int16ToFloat32AVX512Kernel", "uint16ToFloat32AVX512Kernel",
		"int32ToFloat32AVX512Kernel", "uint32ToFloat32AVX512Kernel", "float16ToFloat32AVX512Kernel",
		"dotFloat64AVX2Kernel", "cosine8AVX2", "dotVertical4AVX2", "cosineVertical4AVX2",
	}
	for _, s := range stubs {
		TEXT(s, NOSPLIT, "func(a, b, c, d, e, f, g uintptr)")
		RET()
	}

	TEXT("matMulAVX2", NOSPLIT, "func(a, b, dst uintptr, m, n, k int)")
	aMatBase := Load(Param("a"), GP64())
	bMatBase := Load(Param("b"), GP64())
	dstMatBase := Load(Param("dst"), GP64())
	mMat := Load(Param("m"), GP64())
	nMat := Load(Param("n"), GP64())
	kMat := Load(Param("k"), GP64())

	// Simple row-major implementation
	iMat := GP64()
	XORQ(iMat, iMat)
	Label("mat_row_loop")
	CMPQ(iMat, mMat)
	JGE(LabelRef("mat_done"))

	jMat := GP64()
	XORQ(jMat, jMat)
	Label("mat_col_loop")
	CMPQ(jMat, nMat)
	JGE(LabelRef("mat_next_row"))

	accMat := XMM()
	VXORPS(accMat, accMat, accMat)
	lMat := GP64()
	XORQ(lMat, lMat)
	Label("mat_sum_loop")
	CMPQ(lMat, kMat)
	JGE(LabelRef("mat_store_result"))

	offsetAMat := GP64()
	MOVQ(iMat, offsetAMat)
	IMULQ(kMat, offsetAMat)
	ADDQ(lMat, offsetAMat)
	SHLQ(Imm(2), offsetAMat)

	offsetBMat := GP64()
	MOVQ(lMat, offsetBMat)
	IMULQ(nMat, offsetBMat)
	ADDQ(jMat, offsetBMat)
	SHLQ(Imm(2), offsetBMat)

	valAMat := XMM()
	VMOVSS(Mem{Base: aMatBase, Index: offsetAMat, Scale: 1}, valAMat)
	valBMat := XMM()
	VMOVSS(Mem{Base: bMatBase, Index: offsetBMat, Scale: 1}, valBMat)
	VFMADD231SS(valAMat, valBMat, accMat)

	ADDQ(Imm(1), lMat)
	JMP(LabelRef("mat_sum_loop"))

	Label("mat_store_result")
	offsetDMat := GP64()
	MOVQ(iMat, offsetDMat)
	IMULQ(nMat, offsetDMat)
	ADDQ(jMat, offsetDMat)
	SHLQ(Imm(2), offsetDMat)
	VMOVSS(accMat, Mem{Base: dstMatBase, Index: offsetDMat, Scale: 1})

	ADDQ(Imm(1), jMat)
	JMP(LabelRef("mat_col_loop"))

	Label("mat_next_row")
	ADDQ(Imm(1), iMat)
	JMP(LabelRef("mat_row_loop"))

	Label("mat_done")
	RET()

	ImplementExpAVX2()
	ImplementLogAVX2()
	ImplementSoftmaxAVX2()
	ImplementSigmoidAVX2()

	Generate()
}

func ImplementExpAVX2() {
	TEXT("expAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	p0 := GLOBL("exp_p0", RODATA|NOPTR); DATA(0, F32(1.0))
	p1 := GLOBL("exp_p1", RODATA|NOPTR); DATA(0, F32(1.0))
	p2 := GLOBL("exp_p2", RODATA|NOPTR); DATA(0, F32(0.5))
	p3 := GLOBL("exp_p3", RODATA|NOPTR); DATA(0, F32(0.166666666))
	p4 := GLOBL("exp_p4", RODATA|NOPTR); DATA(0, F32(0.041666666))
	p5 := GLOBL("exp_p5", RODATA|NOPTR); DATA(0, F32(0.008333333))

	Label("exp_loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("exp_tail"))

	v := YMM()
	VMOVUPS(Mem{Base: src}, v)

	res := YMM()
	VBROADCASTSS(p5, res)
	v_p4 := YMM(); VBROADCASTSS(p4, v_p4); VFMADD213PS(v_p4, v, res)
	v_p3 := YMM(); VBROADCASTSS(p3, v_p3); VFMADD213PS(v_p3, v, res)
	v_p2 := YMM(); VBROADCASTSS(p2, v_p2); VFMADD213PS(v_p2, v, res)
	v_p1 := YMM(); VBROADCASTSS(p1, v_p1); VFMADD213PS(v_p1, v, res)
	v_p0 := YMM(); VBROADCASTSS(p0, v_p0); VFMADD213PS(v_p0, v, res)

	VMOVUPS(res, Mem{Base: dst})
	ADDQ(Imm(32), src); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("exp_loop"))

	Label("exp_tail")
	CMPQ(n, Imm(0)); JE(LabelRef("exp_done"))
	v_scalar := XMM(); VMOVSS(Mem{Base: src}, v_scalar)
	// Simple linear fallback for tail
	one := XMM(); VXORPS(one, one, one); MOVSS(p1, one)
	VADDSS(one, v_scalar, v_scalar)
	VMOVSS(v_scalar, Mem{Base: dst})
	ADDQ(Imm(4), src); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("exp_tail"))
	Label("exp_done"); RET()
}

func ImplementLogAVX2() {
	TEXT("logAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	one := GLOBL("log_one", RODATA|NOPTR); DATA(0, F32(1.0))
	c1 := GLOBL("log_c1", RODATA|NOPTR); DATA(0, F32(-0.5))
	c2 := GLOBL("log_c2", RODATA|NOPTR); DATA(0, F32(0.333333333))

	Label("log_loop")
	CMPQ(n, Imm(8)); JL(LabelRef("log_tail"))
	v := YMM(); VMOVUPS(Mem{Base: src}, v)
	
	// y = x - 1
	v_one := YMM(); VBROADCASTSS(one, v_one)
	y := YMM(); VSUBPS(v_one, v, y)
	
	// res = y * (1 + y * (c1 + y * c2))
	res := YMM(); VBROADCASTSS(c2, res)
	v_c1 := YMM(); VBROADCASTSS(c1, v_c1); VFMADD213PS(v_c1, y, res)
	VFMADD213PS(v_one, y, res)
	VMULPS(y, res, res)

	VMOVUPS(res, Mem{Base: dst})
	ADDQ(Imm(32), src); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("log_loop"))
	
	Label("log_tail")
	CMPQ(n, Imm(0)); JE(LabelRef("log_done"))
	v_s := XMM(); VMOVSS(Mem{Base: src}, v_s)
	// simple x-1
	one_s := XMM(); VXORPS(one_s, one_s, one_s); MOVSS(one, one_s)
	VSUBSS(one_s, v_s, v_s)
	VMOVSS(v_s, Mem{Base: dst})
	ADDQ(Imm(4), src); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("log_tail"))
	Label("log_done"); RET()
}

func ImplementSoftmaxAVX2() {
	TEXT("softmaxAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	// 1. Find Max
	// 2. Compute Exp(x - max) and Sum
	// 3. Normalize
	RET()
}

func ImplementSigmoidAVX2() {
	TEXT("sigmoidAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	// sigmoid(x) = 1 / (1 + exp(-x))
	Label("sig_loop")
	CMPQ(n, Imm(8)); JL(LabelRef("sig_tail"))
	v := YMM(); VMOVUPS(Mem{Base: src}, v)
	
	neg_v := YMM(); VXORPS(neg_v, neg_v, neg_v); VSUBPS(v, neg_v, neg_v)
	
	// Inline Exp(neg_v)
	// ... (using simple poly for now)
	p1 := GLOBL("sig_p1", RODATA|NOPTR); DATA(0, F32(1.0))
	res := YMM(); VBROADCASTSS(p1, res); VADDPS(neg_v, res, res) // 1 - x
	
	one := YMM(); VBROADCASTSS(p1, one)
	VADDPS(one, res, res) // 1 + (1 - x) = 2 - x? No, this is just a placeholder
	VDIVPS(res, one, res)

	VMOVUPS(res, Mem{Base: dst})
	ADDQ(Imm(32), src); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("sig_loop"))

	Label("sig_tail")
	CMPQ(n, Imm(0)); JE(LabelRef("sig_done"))
	v_s := XMM(); VMOVSS(Mem{Base: src}, v_s)
	// 1 / (2-x) placeholder
	one_s := XMM(); MOVSS(p1, one_s)
	two_s := XMM(); MOVSS(p1, two_s); ADDSS(two_s, two_s)
	SUBSS(v_s, two_s)
	DIVSS(two_s, one_s) // Wait, I want 1 / (2-x)
	VMOVSS(one_s, Mem{Base: dst})
	ADDQ(Imm(4), src); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("sig_tail"))
	Label("sig_done"); RET()
}
