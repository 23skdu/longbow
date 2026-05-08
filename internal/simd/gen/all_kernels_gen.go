//go:build ignore
package main

import (
	"fmt"
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

	// --- Fixed-Size Kernels (Unrolled) ---
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

	// --- Looping Kernels ---
	ImplementL2SquaredAVX2()
	ImplementDotAVX2()
	ImplementL2SquaredAVX512()
	ImplementDotAVX512()

	// --- Specialized Fixed-Size Kernels ---
	ImplementSpecializedAVX2(128)
	ImplementSpecializedAVX2(384)
	ImplementSpecializedAVX2(768)
	ImplementSpecializedAVX2(1024)
	ImplementSpecializedAVX2(3072)

	// --- AVX512 Specialized Fixed-Size Kernels ---
	ImplementSpecializedAVX512(128)
	ImplementSpecializedAVX512(384)
	ImplementSpecializedAVX512(768)
	ImplementSpecializedAVX512(1024)
	ImplementSpecializedAVX512(3072)

	// --- Vertical Batch Kernels ---
	ImplementEuclideanVertical4AVX2()
	ImplementDotVertical4AVX2()
	ImplementEuclideanVertical4AVX512()
	ImplementDotVertical4AVX512()

	// --- Reductions ---
	negInf := GLOBL("neg_inf_const_red", RODATA|NOPTR); DATA(0, U32(0xff800000))
	posInf := GLOBL("pos_inf_const_red", RODATA|NOPTR); DATA(0, U32(0x7f800000))

	ImplementArgMaxAVX2(negInf)
	ImplementArgMinAVX2(posInf)

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
	xLowMi := XMM(); VEXTRACTF128(Imm(0), minV, xLowMi)
	xHighMi := XMM(); VEXTRACTF128(Imm(1), minV, xHighMi)
	VMINPS(xLowMi, xHighMi, xHighMi)
	xNextMi := XMM(); VMOVSHDUP(xHighMi, xNextMi)
	VMINSS(xNextMi, xHighMi, xHighMi)
	xShufMi := XMM(); VPERMILPS(Imm(0x4e), xHighMi, xShufMi)
	VMINSS(xShufMi, xHighMi, xHighMi)
	Store(xHighMi, ReturnIndex(0)); VZEROUPPER(); RET()

	// --- Stubs for Linker (Categorized by signature) ---
	
	// func(a, b uintptr, n int) float32
	stubsDist := []string{
		"euclideanFloat64AVX512Kernel", "dotFloat64AVX512Kernel",
		"euclideanF16AVX512Kernel", "dotF16AVX512Kernel",
		"euclideanF16AVX2Kernel", "dotF16AVX2Kernel",
		"dotInt4AVX512Kernel", "dotInt4AVX2Kernel",
		"euclideanFloat64AVX2Kernel", "euclideanInt8AVX2Kernel", 
		"euclideanInt16AVX2Kernel", "euclideanUint16AVX2Kernel",
		"dotInt16AVX2Kernel", "dotUint16AVX2Kernel",
		"euclideanInt8Unrolled4xAVX2Kernel", "dotFloat64AVX2Kernel",
		"brayCurtisAVX2Kernel", "manhattanAVX2Kernel", "chebyshevAVX2Kernel",
	}
	for _, s := range stubsDist {
		TEXT(s, NOSPLIT, "func(a, b uintptr, n int) float32")
		Store(XMM(), ReturnIndex(0))
		RET()
	}

	// func(src, dst uintptr, n int)
	stubsUnary := []string{
		"sigmoidAVX512Kernel",
		"int8ToFloat32AVX2Kernel", "uint8ToFloat32AVX2Kernel", 
		"int16ToFloat32AVX2Kernel", "uint16ToFloat32AVX2Kernel",
		"int32ToFloat32AVX2Kernel", "uint32ToFloat32AVX2Kernel", 
		"float16ToFloat32AVX2Kernel",
		"int8ToFloat32AVX512Kernel", "uint8ToFloat32AVX512Kernel", 
		"int16ToFloat32AVX512Kernel", "uint16ToFloat32AVX512Kernel",
		"int32ToFloat32AVX512Kernel", "uint32ToFloat32AVX512Kernel", 
		"float16ToFloat32AVX512Kernel",
	}
	for _, s := range stubsUnary {
		TEXT(s, NOSPLIT, "func(src, dst uintptr, n int)")
		RET()
	}

	// Match kernels: func(src uintptr, val T, op int, dst uintptr, n int)
	stubsMatch := []string{
		"matchInt64AVX2Kernel", "matchInt32AVX2Kernel", "matchFloat32AVX2Kernel", "matchFloat64AVX2Kernel",
		"matchInt64AVX512Kernel", "matchInt32AVX512Kernel", "matchFloat32AVX512Kernel", "matchFloat64AVX512Kernel",
	}
	for _, s := range stubsMatch {
		TEXT(s, NOSPLIT, "func(src uintptr, val int64, op int, dst uintptr, n int)")
		RET()
	}

	// Complex/Other
	TEXT("cosineDotAVX512", NOSPLIT, "func(a, b uintptr, n int) (dot, normA, normB float32)")
	Store(XMM(), ReturnIndex(0)); Store(XMM(), ReturnIndex(1)); Store(XMM(), ReturnIndex(2))
	RET()
	TEXT("cosineVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("cosineVertical4AVX2", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("euclideanSQ8AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) int32")
	Store(GP32(), ReturnIndex(0))
	RET()
	TEXT("adcBatchAVX512Kernel", NOSPLIT, "func(table, codes uintptr, m int, results uintptr, n int)")
	RET()
	TEXT("adcBatchVNNIKernel", NOSPLIT, "func(table, codes uintptr, m int, results uintptr, n int)")
	RET()
	TEXT("euclideanPQVNNIKernel", NOSPLIT, "func(q, c uintptr, subDim, k int, res uintptr)")
	RET()
	TEXT("adcBatchAVX2Kernel", NOSPLIT, "func(table, codes uintptr, m int, results uintptr, n int)")
	RET()
	TEXT("euclideanSQ8AVX2Kernel", NOSPLIT, "func(a, b uintptr, n int) int32")
	Store(GP32(), ReturnIndex(0))
	RET()
	TEXT("cosine8AVX2", NOSPLIT, "func(a, b uintptr) (dot, normA, normB float32)")
	ac := Load(Param("a"), GP64()); bc := Load(Param("b"), GP64())
	ya := YMM(); VMOVUPS(Mem{Base: ac}, ya)
	yb := YMM(); VMOVUPS(Mem{Base: bc}, yb)
	
	// Dot product: ya * yb
	ydot := YMM(); VMULPS(ya, yb, ydot)
	// NormA: ya * ya
	yna := YMM(); VMULPS(ya, ya, yna)
	// NormB: yb * yb
	ynb := YMM(); VMULPS(yb, yb, ynb)
	
	Store(reduceYMM(ydot), ReturnIndex(0))
	Store(reduceYMM(yna), ReturnIndex(1))
	Store(reduceYMM(ynb), ReturnIndex(2))
	VZEROUPPER(); RET()

	TEXT("euclidean32FMA", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("dot32FMA", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("cosine32FMA", NOSPLIT, "func(a, b uintptr) (dot, normA, normB float32)")
	Store(XMM(), ReturnIndex(0)); Store(XMM(), ReturnIndex(1)); Store(XMM(), ReturnIndex(2))
	RET()
	TEXT("euclidean64FMA", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("dot64FMA", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("cosine64FMA", NOSPLIT, "func(a, b uintptr) (dot, normA, normB float32)")
	Store(XMM(), ReturnIndex(0)); Store(XMM(), ReturnIndex(1)); Store(XMM(), ReturnIndex(2))
	RET()

	TEXT("matMulAVX2Kernel", NOSPLIT, "func(a, b, dst uintptr, m, n, k int)")
	aBase := Load(Param("a"), GP64())
	bBase := Load(Param("b"), GP64())
	dstBase := Load(Param("dst"), GP64())
	m_val := Load(Param("m"), GP64())
	n_val := Load(Param("n"), GP64())
	k_val := Load(Param("k"), GP64())

	i_reg := GP64(); XORQ(i_reg, i_reg)
	Label("m_loop")
	CMPQ(i_reg, m_val); JE(LabelRef("m_done"))

	l_reg := GP64(); XORQ(l_reg, l_reg)
	Label("k_loop")
	CMPQ(l_reg, k_val); JE(LabelRef("k_done"))

	// va = a[i*k + l]
	idxA := GP64(); MOVQ(i_reg, idxA); IMULQ(k_val, idxA); ADDQ(l_reg, idxA)
	va := XMM(); VMOVSS(Mem{Base: aBase, Index: idxA, Scale: 4}, va)
	vaY := YMM(); VBROADCASTSS(va, vaY)

	j_reg := GP64(); XORQ(j_reg, j_reg)
	Label("n_loop")
	CMPQ(j_reg, n_val); JGE(LabelRef("n_done"))

	// dst[i*n + j] += va * b[l*n + j]
	idxB := GP64(); MOVQ(l_reg, idxB); IMULQ(n_val, idxB); ADDQ(j_reg, idxB)
	idxDst := GP64(); MOVQ(i_reg, idxDst); IMULQ(n_val, idxDst); ADDQ(j_reg, idxDst)

	vb := YMM(); VMOVUPS(Mem{Base: bBase, Index: idxB, Scale: 4}, vb)
	vdst := YMM(); VMOVUPS(Mem{Base: dstBase, Index: idxDst, Scale: 4}, vdst)

	VFMADD231PS(vaY, vb, vdst)
	VMOVUPS(vdst, Mem{Base: dstBase, Index: idxDst, Scale: 4})

	ADDQ(Imm(8), j_reg)
	JMP(LabelRef("n_loop"))

	Label("n_done")
	INCQ(l_reg)
	JMP(LabelRef("k_loop"))

	Label("k_done")
	INCQ(i_reg)
	JMP(LabelRef("m_loop"))

	Label("m_done")
	VZEROUPPER(); RET()

	ImplementArgMaxAVX512()
	ImplementArgMinAVX512()
	ImplementSoftmaxAVX2()
	ImplementSigmoidAVX2()

	Generate()
}

func ImplementL2SquaredAVX2() {
	TEXT("l2SquaredAVX2Kernel", NOSPLIT, "func(a, b uintptr, n int, res uintptr)")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	ySum := YMM()
	VXORPS(ySum, ySum, ySum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	y0 := YMM(); VMOVUPS(Mem{Base: a}, y0)
	y1 := YMM(); VMOVUPS(Mem{Base: b}, y1)
	VSUBPS(y1, y0, y0)
	VFMADD231PS(y0, y0, ySum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	// Horizontal reduction
	xLow := XMM(); VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM(); VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	// Tail handling (scalar)
	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM(); VMOVSS(Mem{Base: a}, x0)
	x1 := XMM(); VMOVSS(Mem{Base: b}, x1)
	VSUBSS(x1, x0, x0)
	VFMADD231SS(x0, x0, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(xHigh, Mem{Base: res})
	VZEROUPPER(); RET()
}

func ImplementDotAVX2() {
	TEXT("dotAVX2Kernel", NOSPLIT, "func(a, b uintptr, n int, res uintptr)")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	ySum := YMM()
	VXORPS(ySum, ySum, ySum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	y0 := YMM(); VMOVUPS(Mem{Base: a}, y0)
	y1 := YMM(); VMOVUPS(Mem{Base: b}, y1)
	VFMADD231PS(y0, y1, ySum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	xLow := XMM(); VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM(); VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM(); VMOVSS(Mem{Base: a}, x0)
	x1 := XMM(); VMOVSS(Mem{Base: b}, x1)
	VFMADD231SS(x0, x1, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(xHigh, Mem{Base: res})
	VZEROUPPER(); RET()
}

func ImplementL2SquaredAVX512() {
	TEXT("l2SquaredAVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPS(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(16))
	JL(LabelRef("tail"))

	z0 := ZMM(); VMOVUPS(Mem{Base: a}, z0)
	z1 := ZMM(); VMOVUPS(Mem{Base: b}, z1)
	VSUBPS(z1, z0, z0)
	VFMADD231PS(z0, z0, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(16), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM(); VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM(); VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPS(yLow, yHigh, yHigh)
	
	xLow := XMM(); VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM(); VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM(); VMOVSS(Mem{Base: a}, x0)
	x1 := XMM(); VMOVSS(Mem{Base: b}, x1)
	VSUBSS(x1, x0, x0)
	VFMADD231SS(x0, x0, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER(); RET()
}

func ImplementDotAVX512() {
	TEXT("dotAVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPS(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(16))
	JL(LabelRef("tail"))

	z0 := ZMM(); VMOVUPS(Mem{Base: a}, z0)
	z1 := ZMM(); VMOVUPS(Mem{Base: b}, z1)
	VFMADD231PS(z0, z1, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(16), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM(); VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM(); VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPS(yLow, yHigh, yHigh)
	
	xLow := XMM(); VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM(); VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM(); VMOVSS(Mem{Base: a}, x0)
	x1 := XMM(); VMOVSS(Mem{Base: b}, x1)
	VFMADD231SS(x0, x1, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER(); RET()
}

func ImplementEuclideanVertical4AVX2() {
	TEXT("euclideanVertical4AVX2", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	q := Load(Param("q"), GP64())
	v0 := Load(Param("v0"), GP64())
	v1 := Load(Param("v1"), GP64())
	v2 := Load(Param("v2"), GP64())
	v3 := Load(Param("v3"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	s0, s1, s2, s3 := YMM(), YMM(), YMM(), YMM()
	VXORPS(s0, s0, s0); VXORPS(s1, s1, s1); VXORPS(s2, s2, s2); VXORPS(s3, s3, s3)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	qy := YMM(); VMOVUPS(Mem{Base: q}, qy)
	t0, t1, t2, t3 := YMM(), YMM(), YMM(), YMM()
	VMOVUPS(Mem{Base: v0}, t0); VMOVUPS(Mem{Base: v1}, t1); VMOVUPS(Mem{Base: v2}, t2); VMOVUPS(Mem{Base: v3}, t3)
	
	VSUBPS(qy, t0, t0); VFMADD231PS(t0, t0, s0)
	VSUBPS(qy, t1, t1); VFMADD231PS(t1, t1, s1)
	VSUBPS(qy, t2, t2); VFMADD231PS(t2, t2, s2)
	VSUBPS(qy, t3, t3); VFMADD231PS(t3, t3, s3)

	ADDQ(Imm(32), q); ADDQ(Imm(32), v0); ADDQ(Imm(32), v1); ADDQ(Imm(32), v2); ADDQ(Imm(32), v3)
	SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	// Simplistic horizontal reduction for each
	reduceToScalar := func(y reg.VecVirtual) reg.VecVirtual {
		xl := XMM(); VEXTRACTF128(Imm(0), y, xl)
		xh := XMM(); VEXTRACTF128(Imm(1), y, xh)
		VADDPS(xl, xh, xh)
		xf := XMM(); VMOVHLPS(xh, xf, xf)
		VADDPS(xf, xh, xh)
		xn := XMM(); VMOVSHDUP(xh, xn)
		VADDSS(xn, xh, xh)
		return xh
	}
	r0 := reduceToScalar(s0); r1 := reduceToScalar(s1); r2 := reduceToScalar(s2); r3 := reduceToScalar(s3)

	Label("scalar_loop")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))
	qs := XMM(); VMOVSS(Mem{Base: q}, qs)
	t0s, t1s, t2s, t3s := XMM(), XMM(), XMM(), XMM()
	VMOVSS(Mem{Base: v0}, t0s); VMOVSS(Mem{Base: v1}, t1s); VMOVSS(Mem{Base: v2}, t2s); VMOVSS(Mem{Base: v3}, t3s)
	
	VSUBSS(qs, t0s, t0s); VFMADD231SS(t0s, t0s, r0)
	VSUBSS(qs, t1s, t1s); VFMADD231SS(t1s, t1s, r1)
	VSUBSS(qs, t2s, t2s); VFMADD231SS(t2s, t2s, r2)
	VSUBSS(qs, t3s, t3s); VFMADD231SS(t3s, t3s, r3)

	ADDQ(Imm(4), q); ADDQ(Imm(4), v0); ADDQ(Imm(4), v1); ADDQ(Imm(4), v2); ADDQ(Imm(4), v3)
	DECQ(n); JMP(LabelRef("scalar_loop"))

	Label("done")
	// Sqrt and store
	VSQRTSS(r0, r0, r0); VMOVSS(r0, Mem{Base: res})
	VSQRTSS(r1, r1, r1); VMOVSS(r1, Mem{Base: res, Disp: 4})
	VSQRTSS(r2, r2, r2); VMOVSS(r2, Mem{Base: res, Disp: 8})
	VSQRTSS(r3, r3, r3); VMOVSS(r3, Mem{Base: res, Disp: 12})
	VZEROUPPER(); RET()
}

func ImplementDotVertical4AVX2() {
	TEXT("dotVertical4AVX2", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	q := Load(Param("q"), GP64())
	v0 := Load(Param("v0"), GP64())
	v1 := Load(Param("v1"), GP64())
	v2 := Load(Param("v2"), GP64())
	v3 := Load(Param("v3"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	s0, s1, s2, s3 := YMM(), YMM(), YMM(), YMM()
	VXORPS(s0, s0, s0); VXORPS(s1, s1, s1); VXORPS(s2, s2, s2); VXORPS(s3, s3, s3)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	qy := YMM(); VMOVUPS(Mem{Base: q}, qy)
	t0, t1, t2, t3 := YMM(), YMM(), YMM(), YMM()
	VMOVUPS(Mem{Base: v0}, t0); VMOVUPS(Mem{Base: v1}, t1); VMOVUPS(Mem{Base: v2}, t2); VMOVUPS(Mem{Base: v3}, t3)
	
	VFMADD231PS(qy, t0, s0)
	VFMADD231PS(qy, t1, s1)
	VFMADD231PS(qy, t2, s2)
	VFMADD231PS(qy, t3, s3)

	ADDQ(Imm(32), q); ADDQ(Imm(32), v0); ADDQ(Imm(32), v1); ADDQ(Imm(32), v2); ADDQ(Imm(32), v3)
	SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	reduceToScalar := func(y reg.VecVirtual) reg.VecVirtual {
		xl := XMM(); VEXTRACTF128(Imm(0), y, xl)
		xh := XMM(); VEXTRACTF128(Imm(1), y, xh)
		VADDPS(xl, xh, xh)
		xf := XMM(); VMOVHLPS(xh, xf, xf)
		VADDPS(xf, xh, xh)
		xn := XMM(); VMOVSHDUP(xh, xn)
		VADDSS(xn, xh, xh)
		return xh
	}
	r0 := reduceToScalar(s0); r1 := reduceToScalar(s1); r2 := reduceToScalar(s2); r3 := reduceToScalar(s3)

	Label("scalar_loop")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))
	qs := XMM(); VMOVSS(Mem{Base: q}, qs)
	t0s, t1s, t2s, t3s := XMM(), XMM(), XMM(), XMM()
	VMOVSS(Mem{Base: v0}, t0s); VMOVSS(Mem{Base: v1}, t1s); VMOVSS(Mem{Base: v2}, t2s); VMOVSS(Mem{Base: v3}, t3s)
	
	VFMADD231SS(qs, t0s, r0)
	VFMADD231SS(qs, t1s, r1)
	VFMADD231SS(qs, t2s, r2)
	VFMADD231SS(qs, t3s, r3)

	ADDQ(Imm(4), q); ADDQ(Imm(4), v0); ADDQ(Imm(4), v1); ADDQ(Imm(4), v2); ADDQ(Imm(4), v3)
	DECQ(n); JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(r0, Mem{Base: res})
	VMOVSS(r1, Mem{Base: res, Disp: 4})
	VMOVSS(r2, Mem{Base: res, Disp: 8})
	VMOVSS(r3, Mem{Base: res, Disp: 12})
	VZEROUPPER(); RET()
}

func ImplementEuclideanVertical4AVX512() {
	TEXT("euclideanVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	q := Load(Param("q"), GP64())
	v0 := Load(Param("v0"), GP64())
	v1 := Load(Param("v1"), GP64())
	v2 := Load(Param("v2"), GP64())
	v3 := Load(Param("v3"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	s0, s1, s2, s3 := ZMM(), ZMM(), ZMM(), ZMM()
	VXORPS(s0, s0, s0); VXORPS(s1, s1, s1); VXORPS(s2, s2, s2); VXORPS(s3, s3, s3)

	Label("loop")
	CMPQ(n, Imm(16)); JL(LabelRef("tail"))

	qz := ZMM(); VMOVUPS(Mem{Base: q}, qz)
	t0, t1, t2, t3 := ZMM(), ZMM(), ZMM(), ZMM()
	VMOVUPS(Mem{Base: v0}, t0); VMOVUPS(Mem{Base: v1}, t1); VMOVUPS(Mem{Base: v2}, t2); VMOVUPS(Mem{Base: v3}, t3)
	
	VSUBPS(qz, t0, t0); VFMADD231PS(t0, t0, s0)
	VSUBPS(qz, t1, t1); VFMADD231PS(t1, t1, s1)
	VSUBPS(qz, t2, t2); VFMADD231PS(t2, t2, s2)
	VSUBPS(qz, t3, t3); VFMADD231PS(t3, t3, s3)

	ADDQ(Imm(64), q); ADDQ(Imm(64), v0); ADDQ(Imm(64), v1); ADDQ(Imm(64), v2); ADDQ(Imm(64), v3)
	SUBQ(Imm(16), n); JMP(LabelRef("loop"))

	Label("tail")
	reduceZToScalar := func(z reg.VecVirtual) reg.VecVirtual {
		yl := YMM(); VEXTRACTF64X4(Imm(0), z, yl)
		yh := YMM(); VEXTRACTF64X4(Imm(1), z, yh)
		VADDPS(yl, yh, yh)
		xl := XMM(); VEXTRACTF128(Imm(0), yh, xl)
		xh := XMM(); VEXTRACTF128(Imm(1), yh, xh)
		VADDPS(xl, xh, xh)
		xf := XMM(); VMOVHLPS(xh, xf, xf)
		VADDPS(xf, xh, xh)
		xn := XMM(); VMOVSHDUP(xh, xn)
		VADDSS(xn, xh, xh)
		return xh
	}
	r0 := reduceZToScalar(s0); r1 := reduceZToScalar(s1); r2 := reduceZToScalar(s2); r3 := reduceZToScalar(s3)

	Label("scalar_loop")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))
	qs := XMM(); VMOVSS(Mem{Base: q}, qs)
	t0s, t1s, t2s, t3s := XMM(), XMM(), XMM(), XMM()
	VMOVSS(Mem{Base: v0}, t0s); VMOVSS(Mem{Base: v1}, t1s); VMOVSS(Mem{Base: v2}, t2s); VMOVSS(Mem{Base: v3}, t3s)
	
	VSUBSS(qs, t0s, t0s); VFMADD231SS(t0s, t0s, r0)
	VSUBSS(qs, t1s, t1s); VFMADD231SS(t1s, t1s, r1)
	VSUBSS(qs, t2s, t2s); VFMADD231SS(t2s, t2s, r2)
	VSUBSS(qs, t3s, t3s); VFMADD231SS(t3s, t3s, r3)

	ADDQ(Imm(4), q); ADDQ(Imm(4), v0); ADDQ(Imm(4), v1); ADDQ(Imm(4), v2); ADDQ(Imm(4), v3)
	DECQ(n); JMP(LabelRef("scalar_loop"))

	Label("done")
	VSQRTSS(r0, r0, r0); VMOVSS(r0, Mem{Base: res})
	VSQRTSS(r1, r1, r1); VMOVSS(r1, Mem{Base: res, Disp: 4})
	VSQRTSS(r2, r2, r2); VMOVSS(r2, Mem{Base: res, Disp: 8})
	VSQRTSS(r3, r3, r3); VMOVSS(r3, Mem{Base: res, Disp: 12})
	VZEROUPPER(); RET()
}

func ImplementDotVertical4AVX512() {
	TEXT("dotVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	q := Load(Param("q"), GP64())
	v0 := Load(Param("v0"), GP64())
	v1 := Load(Param("v1"), GP64())
	v2 := Load(Param("v2"), GP64())
	v3 := Load(Param("v3"), GP64())
	n := Load(Param("n"), GP64())
	res := Load(Param("res"), GP64())

	s0, s1, s2, s3 := ZMM(), ZMM(), ZMM(), ZMM()
	VXORPS(s0, s0, s0); VXORPS(s1, s1, s1); VXORPS(s2, s2, s2); VXORPS(s3, s3, s3)

	Label("loop")
	CMPQ(n, Imm(16)); JL(LabelRef("tail"))

	qz := ZMM(); VMOVUPS(Mem{Base: q}, qz)
	t0, t1, t2, t3 := ZMM(), ZMM(), ZMM(), ZMM()
	VMOVUPS(Mem{Base: v0}, t0); VMOVUPS(Mem{Base: v1}, t1); VMOVUPS(Mem{Base: v2}, t2); VMOVUPS(Mem{Base: v3}, t3)
	
	VFMADD231PS(qz, t0, s0)
	VFMADD231PS(qz, t1, s1)
	VFMADD231PS(qz, t2, s2)
	VFMADD231PS(qz, t3, s3)

	ADDQ(Imm(64), q); ADDQ(Imm(64), v0); ADDQ(Imm(64), v1); ADDQ(Imm(64), v2); ADDQ(Imm(64), v3)
	SUBQ(Imm(16), n); JMP(LabelRef("loop"))

	Label("tail")
	reduceZToScalar := func(z reg.VecVirtual) reg.VecVirtual {
		yl := YMM(); VEXTRACTF64X4(Imm(0), z, yl)
		yh := YMM(); VEXTRACTF64X4(Imm(1), z, yh)
		VADDPS(yl, yh, yh)
		xl := XMM(); VEXTRACTF128(Imm(0), yh, xl)
		xh := XMM(); VEXTRACTF128(Imm(1), yh, xh)
		VADDPS(xl, xh, xh)
		xf := XMM(); VMOVHLPS(xh, xf, xf)
		VADDPS(xf, xh, xh)
		xn := XMM(); VMOVSHDUP(xh, xn)
		VADDSS(xn, xh, xh)
		return xh
	}
	r0 := reduceZToScalar(s0); r1 := reduceZToScalar(s1); r2 := reduceZToScalar(s2); r3 := reduceZToScalar(s3)

	Label("scalar_loop")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))
	qs := XMM(); VMOVSS(Mem{Base: q}, qs)
	t0s, t1s, t2s, t3s := XMM(), XMM(), XMM(), XMM()
	VMOVSS(Mem{Base: v0}, t0s); VMOVSS(Mem{Base: v1}, t1s); VMOVSS(Mem{Base: v2}, t2s); VMOVSS(Mem{Base: v3}, t3s)
	
	VFMADD231SS(qs, t0s, r0)
	VFMADD231SS(qs, t1s, r1)
	VFMADD231SS(qs, t2s, r2)
	VFMADD231SS(qs, t3s, r3)

	ADDQ(Imm(4), q); ADDQ(Imm(4), v0); ADDQ(Imm(4), v1); ADDQ(Imm(4), v2); ADDQ(Imm(4), v3)
	DECQ(n); JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(r0, Mem{Base: res})
	VMOVSS(r1, Mem{Base: res, Disp: 4})
	VMOVSS(r2, Mem{Base: res, Disp: 8})
	VMOVSS(r3, Mem{Base: res, Disp: 12})
	VZEROUPPER(); RET()
}

func ImplementArgMaxAVX2(negInf Op) {
	TEXT("argMaxAVX2Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	src := Load(Param("src"), GP64())
	n := Load(Param("n"), GP64())

	maxVal := YMM()
	maxIdx := YMM()
	curIdx := YMM()
	inc := YMM()

	VBROADCASTSS(negInf, maxVal)
	VPXOR(maxIdx, maxIdx, maxIdx)

	idxConst := GLOBL("idx_const_argmax", RODATA|NOPTR)
	DATA(0, U32(0)); DATA(4, U32(1)); DATA(8, U32(2)); DATA(12, U32(3))
	DATA(16, U32(4)); DATA(20, U32(5)); DATA(24, U32(6)); DATA(28, U32(7))
	VMOVUPS(idxConst, curIdx)

	eight := GLOBL("eight_const_argmax", RODATA|NOPTR)
	DATA(0, U32(8)); DATA(4, U32(8)); DATA(8, U32(8)); DATA(12, U32(8))
	DATA(16, U32(8)); DATA(20, U32(8)); DATA(24, U32(8)); DATA(28, U32(8))
	VMOVUPS(eight, inc)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("done"))
	val := YMM(); VMOVUPS(Mem{Base: src}, val)
	mask := YMM(); VCMPPS(Imm(0x0e), maxVal, val, mask)
	VBLENDVPS(mask, val, maxVal, maxVal)
	VBLENDVPS(mask, curIdx, maxIdx, maxIdx)
	VPADDD(inc, curIdx, curIdx)
	ADDQ(Imm(32), src); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("done")
	xValHigh := XMM(); VEXTRACTF128(Imm(1), maxVal, xValHigh)
	xValLow := XMM(); VEXTRACTF128(Imm(0), maxVal, xValLow)
	xIdxHigh := XMM(); VEXTRACTF128(Imm(1), maxIdx, xIdxHigh)
	xIdxLow := XMM(); VEXTRACTF128(Imm(0), maxIdx, xIdxLow)
	resMask := XMM(); VCMPPS(Imm(0x0e), xValLow, xValHigh, resMask)
	VBLENDVPS(resMask, xValHigh, xValLow, xValLow)
	VBLENDVPS(resMask, xIdxHigh, xIdxLow, xIdxLow)

	xValNext := XMM(); VPERMILPS(Imm(0x4e), xValLow, xValNext)
	xIdxNext := XMM(); VPERMILPS(Imm(0x4e), xIdxLow, xIdxNext)
	resMask2 := XMM(); VCMPPS(Imm(0x0e), xValLow, xValNext, resMask2)
	VBLENDVPS(resMask2, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask2, xIdxNext, xIdxLow, xIdxLow)

	VPERMILPS(Imm(0x11), xValLow, xValNext)
	VPERMILPS(Imm(0x11), xIdxLow, xIdxNext)
	resMask3 := XMM(); VCMPPS(Imm(0x0e), xValLow, xValNext, resMask3)
	VBLENDVPS(resMask3, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask3, xIdxNext, xIdxLow, xIdxLow)

	Label("final")
	Store(xValLow, ReturnIndex(0))
	idx_reg := GP64(); VMOVQ(xIdxLow, idx_reg); Store(idx_reg, ReturnIndex(1))
	VZEROUPPER(); RET()
}

func ImplementArgMinAVX2(posInf Op) {
	TEXT("argMinAVX2Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	src := Load(Param("src"), GP64())
	n := Load(Param("n"), GP64())

	minVal := YMM()
	minIdx := YMM()
	curIdx := YMM()
	inc := YMM()

	VBROADCASTSS(posInf, minVal)
	VPXOR(minIdx, minIdx, minIdx)

	idxConst := GLOBL("idx_const_argmin", RODATA|NOPTR)
	DATA(0, U32(0)); DATA(4, U32(1)); DATA(8, U32(2)); DATA(12, U32(3))
	DATA(16, U32(4)); DATA(20, U32(5)); DATA(24, U32(6)); DATA(28, U32(7))
	VMOVUPS(idxConst, curIdx)

	eight := GLOBL("eight_const_argmin", RODATA|NOPTR)
	DATA(0, U32(8)); DATA(4, U32(8)); DATA(8, U32(8)); DATA(12, U32(8))
	DATA(16, U32(8)); DATA(20, U32(8)); DATA(24, U32(8)); DATA(28, U32(8))
	VMOVUPS(eight, inc)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("done"))
	val := YMM(); VMOVUPS(Mem{Base: src}, val)
	mask := YMM(); VCMPPS(Imm(0x01), minVal, val, mask)
	VBLENDVPS(mask, val, minVal, minVal)
	VBLENDVPS(mask, curIdx, minIdx, minIdx)
	VPADDD(inc, curIdx, curIdx)
	ADDQ(Imm(32), src); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("done")
	xValHigh := XMM(); VEXTRACTF128(Imm(1), minVal, xValHigh)
	xValLow := XMM(); VEXTRACTF128(Imm(0), minVal, xValLow)
	xIdxHigh := XMM(); VEXTRACTF128(Imm(1), minIdx, xIdxHigh)
	xIdxLow := XMM(); VEXTRACTF128(Imm(0), minIdx, xIdxLow)
	resMask := XMM(); VCMPPS(Imm(0x01), xValLow, xValHigh, resMask)
	VBLENDVPS(resMask, xValHigh, xValLow, xValLow)
	VBLENDVPS(resMask, xIdxHigh, xIdxLow, xIdxLow)

	xValNext := XMM(); VPERMILPS(Imm(0x4e), xValLow, xValNext)
	xIdxNext := XMM(); VPERMILPS(Imm(0x4e), xIdxLow, xIdxNext)
	resMask2 := XMM(); VCMPPS(Imm(0x01), xValLow, xValNext, resMask2)
	VBLENDVPS(resMask2, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask2, xIdxNext, xIdxLow, xIdxLow)

	VPERMILPS(Imm(0x11), xValLow, xValNext)
	VPERMILPS(Imm(0x11), xIdxLow, xIdxNext)
	resMask3 := XMM(); VCMPPS(Imm(0x01), xValLow, xValNext, resMask3)
	VBLENDVPS(resMask3, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask3, xIdxNext, xIdxLow, xIdxLow)

	Label("final")
	Store(xValLow, ReturnIndex(0))
	idx_reg := GP64(); VMOVQ(xIdxLow, idx_reg); Store(idx_reg, ReturnIndex(1))
	VZEROUPPER(); RET()
}

func ImplementArgMaxAVX512() {
	TEXT("argMaxAVX512Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0)); Store(GP64(), ReturnIndex(1))
	RET()
}

func ImplementArgMinAVX512() {
	TEXT("argMinAVX512Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0)); Store(GP64(), ReturnIndex(1))
	RET()
}

func ImplementSoftmaxAVX2() {
	TEXT("softmaxAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	RET()
}

func ImplementSigmoidAVX2() {
	TEXT("sigmoidAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	RET()
}

	func ImplementSpecializedAVX2(dim int) {
	// L2Squared specialized
	TEXT(fmt.Sprintf("l2Squared%dAVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	
	acc := YMM()
	VXORPS(acc, acc, acc)
	
	for i := 0; i < dim; i += 8 {
		y0 := YMM(); VMOVUPS(Mem{Base: a, Disp: i * 4}, y0)
		y1 := YMM(); VMOVUPS(Mem{Base: b, Disp: i * 4}, y1)
		VSUBPS(y1, y0, y0)
		VFMADD231PS(y0, y0, acc)
	}
	
	// Reduce YMM
	xLow := XMM(); VEXTRACTF128(Imm(0), acc, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), acc, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xSum := XMM(); VMOVHLPS(xHigh, xSum, xSum)
	VADDPS(xSum, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)
	
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Dot specialized
	TEXT(fmt.Sprintf("dot%dAVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a2 := Load(Param("a"), GP64())
	b2 := Load(Param("b"), GP64())
	
	acc2 := YMM()
	VXORPS(acc2, acc2, acc2)
	
	for i := 0; i < dim; i += 8 {
		y0 := YMM(); VMOVUPS(Mem{Base: a2, Disp: i * 4}, y0)
		y1 := YMM(); VMOVUPS(Mem{Base: b2, Disp: i * 4}, y1)
		VFMADD231PS(y0, y1, acc2)
	}
	
	// Reduce YMM
	xLow2 := XMM(); VEXTRACTF128(Imm(0), acc2, xLow2)
	xHigh2 := XMM(); VEXTRACTF128(Imm(1), acc2, xHigh2)
	VADDPS(xLow2, xHigh2, xHigh2)
	xSum2 := XMM(); VMOVHLPS(xHigh2, xSum2, xSum2)
	VADDPS(xSum2, xHigh2, xHigh2)
	xNext2 := XMM(); VMOVSHDUP(xHigh2, xNext2)
	VADDSS(xNext2, xHigh2, xHigh2)
	
	Store(xHigh2, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Euclidean specialized
	TEXT(fmt.Sprintf("euclidean%dAVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a3 := Load(Param("a"), GP64())
	b3 := Load(Param("b"), GP64())
	
	acc3 := YMM()
	VXORPS(acc3, acc3, acc3)
	
	for i := 0; i < dim; i += 8 {
		y0 := YMM(); VMOVUPS(Mem{Base: a3, Disp: i * 4}, y0)
		y1 := YMM(); VMOVUPS(Mem{Base: b3, Disp: i * 4}, y1)
		VSUBPS(y1, y0, y0)
		VFMADD231PS(y0, y0, acc3)
	}
	
	// Reduce YMM
	xLow3 := XMM(); VEXTRACTF128(Imm(0), acc3, xLow3)
	xHigh3 := XMM(); VEXTRACTF128(Imm(1), acc3, xHigh3)
	VADDPS(xLow3, xHigh3, xHigh3)
	xSum3 := XMM(); VMOVHLPS(xHigh3, xSum3, xSum3)
	VADDPS(xSum3, xHigh3, xHigh3)
	xNext3 := XMM(); VMOVSHDUP(xHigh3, xNext3)
	VADDSS(xNext3, xHigh3, xHigh3)
	
	VSQRTSS(xHigh3, xHigh3, xHigh3)
	Store(xHigh3, ReturnIndex(0))
	VZEROUPPER()
	RET()
}


func ImplementSpecializedAVX512(dim int) {
	// L2Squared specialized
	TEXT(fmt.Sprintf("l2Squared%dAVX512Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	
	acc := ZMM()
	VXORPS(acc, acc, acc)
	
	for i := 0; i < dim; i += 16 {
		z0 := ZMM(); VMOVUPS(Mem{Base: a, Disp: i * 4}, z0)
		z1 := ZMM(); VMOVUPS(Mem{Base: b, Disp: i * 4}, z1)
		VSUBPS(z1, z0, z0)
		VFMADD231PS(z0, z0, acc)
	}
	
	// Reduce ZMM
	yLow := YMM(); VEXTRACTF64X4(Imm(0), acc, yLow)
	yHigh := YMM(); VEXTRACTF64X4(Imm(1), acc, yHigh)
	VADDPS(yLow, yHigh, yHigh)
	
	xLow := XMM(); VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM(); VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xSum := XMM(); VMOVHLPS(xHigh, xSum, xSum)
	VADDPS(xSum, xHigh, xHigh)
	xNext := XMM(); VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)
	
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Dot specialized
	TEXT(fmt.Sprintf("dot%dAVX512Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a2 := Load(Param("a"), GP64())
	b2 := Load(Param("b"), GP64())
	
	acc2 := ZMM()
	VXORPS(acc2, acc2, acc2)
	
	for i := 0; i < dim; i += 16 {
		z0 := ZMM(); VMOVUPS(Mem{Base: a2, Disp: i * 4}, z0)
		z1 := ZMM(); VMOVUPS(Mem{Base: b2, Disp: i * 4}, z1)
		VFMADD231PS(z0, z1, acc2)
	}
	
	// Reduce ZMM
	yLow2 := YMM(); VEXTRACTF64X4(Imm(0), acc2, yLow2)
	yHigh2 := YMM(); VEXTRACTF64X4(Imm(1), acc2, yHigh2)
	VADDPS(yLow2, yHigh2, yHigh2)
	
	xLow2 := XMM(); VEXTRACTF128(Imm(0), yHigh2, xLow2)
	xHigh2 := XMM(); VEXTRACTF128(Imm(1), yHigh2, xHigh2)
	VADDPS(xLow2, xHigh2, xHigh2)
	xSum2 := XMM(); VMOVHLPS(xHigh2, xSum2, xSum2)
	VADDPS(xSum2, xHigh2, xHigh2)
	xNext2 := XMM(); VMOVSHDUP(xHigh2, xNext2)
	VADDSS(xNext2, xHigh2, xHigh2)
	
	Store(xHigh2, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Euclidean specialized
	TEXT(fmt.Sprintf("euclidean%dAVX512Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a3 := Load(Param("a"), GP64())
	b3 := Load(Param("b"), GP64())
	acc3 := ZMM()
	VXORPS(acc3, acc3, acc3)
	for i := 0; i < dim; i += 16 {
		z0 := ZMM(); VMOVUPS(Mem{Base: a3, Disp: i * 4}, z0)
		z1 := ZMM(); VMOVUPS(Mem{Base: b3, Disp: i * 4}, z1)
		VSUBPS(z1, z0, z0)
		VFMADD231PS(z0, z0, acc3)
	}
	yLow3 := YMM(); VEXTRACTF64X4(Imm(0), acc3, yLow3)
	yHigh3 := YMM(); VEXTRACTF64X4(Imm(1), acc3, yHigh3)
	VADDPS(yLow3, yHigh3, yHigh3)
	xLow3 := XMM(); VEXTRACTF128(Imm(0), yHigh3, xLow3)
	xHigh3 := XMM(); VEXTRACTF128(Imm(1), yHigh3, xHigh3)
	VADDPS(xLow3, xHigh3, xHigh3)
	xSum3 := XMM(); VMOVHLPS(xHigh3, xSum3, xSum3)
	VADDPS(xSum3, xHigh3, xHigh3)
	xNext3 := XMM(); VMOVSHDUP(xHigh3, xNext3)
	VADDSS(xNext3, xHigh3, xHigh3)
	
	VSQRTSS(xHigh3, xHigh3, xHigh3) // Euclidean needs sqrt
	Store(xHigh3, ReturnIndex(0))
	VZEROUPPER()
	RET()
}
