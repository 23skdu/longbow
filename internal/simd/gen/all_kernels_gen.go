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
		"l2SquaredAVX512Kernel", "dotAVX512Kernel",
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
		"sigmoidAVX512Kernel", "expAVX512Kernel", "logAVX512Kernel",
		"int8ToFloat32AVX2Kernel", "uint8ToFloat32AVX2Kernel", 
		"int16ToFloat32AVX2Kernel", "uint16ToFloat32AVX2Kernel",
		"int32ToFloat32AVX2Kernel", "uint32ToFloat32AVX2Kernel", 
		"float16ToFloat32AVX2Kernel",
		"int8ToFloat32AVX512Kernel", "uint8ToFloat32AVX512Kernel", 
		"int16ToFloat32AVX512Kernel", "uint16ToFloat32AVX512Kernel",
		"int32ToFloat32AVX512Kernel", "uint32ToFloat32AVX512Kernel", 
		"float16ToFloat32AVX512Kernel", "softmaxAVX512Kernel",
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
	TEXT("euclideanVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("cosineVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("dotVertical4AVX512", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("euclideanVertical4AVX2", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
	RET()
	TEXT("dotVertical4AVX2", NOSPLIT, "func(q, v0, v1, v2, v3 uintptr, n int, res uintptr)")
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
	Store(XMM(), ReturnIndex(0)); Store(XMM(), ReturnIndex(1)); Store(XMM(), ReturnIndex(2))
	RET()
	TEXT("l2SquaredAVX2Kernel", NOSPLIT, "func(a, b uintptr, n int, res uintptr)")
	RET()
	TEXT("dotAVX2Kernel", NOSPLIT, "func(a, b uintptr, n int, res uintptr)")
	RET()
	TEXT("euclidean384AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("euclidean768AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("euclidean1536AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("dot384AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("dot768AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
	TEXT("dot1536AVX512Kernel", NOSPLIT, "func(a, b uintptr) float32")
	Store(XMM(), ReturnIndex(0))
	RET()
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

	TEXT("matMulAVX2", NOSPLIT, "func(a, b, dst uintptr, m, n, k int)")
	RET()

	ImplementArgMaxAVX2()
	ImplementArgMinAVX2()
	ImplementArgMaxAVX512()
	ImplementArgMinAVX512()
	ImplementExpAVX2()
	ImplementLogAVX2()
	ImplementSoftmaxAVX2()
	ImplementSigmoidAVX2()

	Generate()
}

func ImplementArgMaxAVX2() {
	TEXT("argMaxAVX2Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0)); Store(GP64(), ReturnIndex(1))
	RET()
}
func ImplementArgMinAVX2() {
	TEXT("argMinAVX2Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0)); Store(GP64(), ReturnIndex(1))
	RET()
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
func ImplementExpAVX2() {
	TEXT("expAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	RET()
}
func ImplementLogAVX2() {
	TEXT("logAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
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
