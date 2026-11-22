package families

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

// EmitReductionKernels emits argmax/argmin/sum/max/min/softmax/sigmoid and stub kernels.
func EmitReductionKernels() {
	// --- Reductions ---
	negInf := GLOBL("neg_inf_const_red", RODATA|NOPTR)
	DATA(0, U32(0xff800000))
	posInf := GLOBL("pos_inf_const_red", RODATA|NOPTR)
	DATA(0, U32(0x7f800000))

	ImplementArgMaxAVX2(negInf)
	ImplementArgMinAVX2(posInf)

	TEXT("sumAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcSum := Load(Param("src"), GP64())
	nSum := Load(Param("n"), GP64())
	sumV := YMM()
	VXORPS(sumV, sumV, sumV)
	Label("sum_loop")
	CMPQ(nSum, Imm(8))
	JL(LabelRef("sum_tail"))
	v := YMM()
	VMOVUPS(Mem{Base: srcSum}, v)
	VADDPS(v, sumV, sumV)
	ADDQ(Imm(32), srcSum)
	SUBQ(Imm(8), nSum)
	JMP(LabelRef("sum_loop"))
	Label("sum_tail")
	Store(ReduceYMM(sumV), ReturnIndex(0))
	VZEROUPPER()
	RET()

	TEXT("maxAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcMax := Load(Param("src"), GP64())
	nMax := Load(Param("n"), GP64())
	maxV := YMM()
	VBROADCASTSS(negInf, maxV)
	Label("max_loop")
	CMPQ(nMax, Imm(8))
	JL(LabelRef("max_tail"))
	vMax := YMM()
	VMOVUPS(Mem{Base: srcMax}, vMax)
	VMAXPS(vMax, maxV, maxV)
	ADDQ(Imm(32), srcMax)
	SUBQ(Imm(8), nMax)
	JMP(LabelRef("max_loop"))
	Label("max_tail")
	xLowM := XMM()
	VEXTRACTF128(Imm(0), maxV, xLowM)
	xHighM := XMM()
	VEXTRACTF128(Imm(1), maxV, xHighM)
	VMAXPS(xLowM, xHighM, xHighM)
	xNextM := XMM()
	VMOVSHDUP(xHighM, xNextM)
	VMAXSS(xNextM, xHighM, xHighM)
	xShufM := XMM()
	VPERMILPS(Imm(0x4e), xHighM, xShufM)
	VMAXSS(xShufM, xHighM, xHighM)
	Store(xHighM, ReturnIndex(0))
	VZEROUPPER()
	RET()

	TEXT("minAVX2Kernel", NOSPLIT, "func(src uintptr, n int) float32")
	srcMin := Load(Param("src"), GP64())
	nMin := Load(Param("n"), GP64())
	minV := YMM()
	VBROADCASTSS(posInf, minV)
	Label("min_loop")
	CMPQ(nMin, Imm(8))
	JL(LabelRef("min_tail"))
	vMin := YMM()
	VMOVUPS(Mem{Base: srcMin}, vMin)
	VMINPS(vMin, minV, minV)
	ADDQ(Imm(32), srcMin)
	SUBQ(Imm(8), nMin)
	JMP(LabelRef("min_loop"))
	Label("min_tail")
	xLowMi := XMM()
	VEXTRACTF128(Imm(0), minV, xLowMi)
	xHighMi := XMM()
	VEXTRACTF128(Imm(1), minV, xHighMi)
	VMINPS(xLowMi, xHighMi, xHighMi)
	xNextMi := XMM()
	VMOVSHDUP(xHighMi, xNextMi)
	VMINSS(xNextMi, xHighMi, xHighMi)
	xShufMi := XMM()
	VPERMILPS(Imm(0x4e), xHighMi, xShufMi)
	VMINSS(xShufMi, xHighMi, xHighMi)
	Store(xHighMi, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// --- Stubs for Linker (Categorized by signature) ---

	// func(a, b uintptr, n int) float32
	stubsDist := []string{
		"euclideanInt8Unrolled4xAVX2Kernel",
	}
	for _, s := range stubsDist {
		TEXT(s, NOSPLIT, "func(a, b uintptr, n int) float32")
		Store(XMM(), ReturnIndex(0))
		RET()
	}

	// func(src, dst uintptr, n int)
	stubsUnary := []string{}
	for _, s := range stubsUnary {
		TEXT(s, NOSPLIT, "func(src, dst uintptr, n int)")
		RET()
	}

	// Match kernels: func(src uintptr, val T, op int, dst uintptr, n int)
	stubsMatch := []string{}
	for _, s := range stubsMatch {
		TEXT(s, NOSPLIT, "func(src uintptr, val int64, op int, dst uintptr, n int)")
		RET()
	}

	// Complex/Other
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

	ImplementArgMaxAVX512()
	ImplementArgMinAVX512()
	ImplementSoftmaxAVX2()
	ImplementSigmoidAVX2()
}

// ImplementArgMaxAVX2 runs the implement arg max avx2 operation.
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
	DATA(0, U32(0))
	DATA(4, U32(1))
	DATA(8, U32(2))
	DATA(12, U32(3))
	DATA(16, U32(4))
	DATA(20, U32(5))
	DATA(24, U32(6))
	DATA(28, U32(7))
	VMOVUPS(idxConst, curIdx)

	eight := GLOBL("eight_const_argmax", RODATA|NOPTR)
	DATA(0, U32(8))
	DATA(4, U32(8))
	DATA(8, U32(8))
	DATA(12, U32(8))
	DATA(16, U32(8))
	DATA(20, U32(8))
	DATA(24, U32(8))
	DATA(28, U32(8))
	VMOVUPS(eight, inc)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("done"))
	val := YMM()
	VMOVUPS(Mem{Base: src}, val)
	mask := YMM()
	VCMPPS(Imm(0x0e), maxVal, val, mask)
	VBLENDVPS(mask, val, maxVal, maxVal)
	VBLENDVPS(mask, curIdx, maxIdx, maxIdx)
	VPADDD(inc, curIdx, curIdx)
	ADDQ(Imm(32), src)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("done")
	xValHigh := XMM()
	VEXTRACTF128(Imm(1), maxVal, xValHigh)
	xValLow := XMM()
	VEXTRACTF128(Imm(0), maxVal, xValLow)
	xIdxHigh := XMM()
	VEXTRACTF128(Imm(1), maxIdx, xIdxHigh)
	xIdxLow := XMM()
	VEXTRACTF128(Imm(0), maxIdx, xIdxLow)
	resMask := XMM()
	VCMPPS(Imm(0x0e), xValLow, xValHigh, resMask)
	VBLENDVPS(resMask, xValHigh, xValLow, xValLow)
	VBLENDVPS(resMask, xIdxHigh, xIdxLow, xIdxLow)

	xValNext := XMM()
	VPERMILPS(Imm(0x4e), xValLow, xValNext)
	xIdxNext := XMM()
	VPERMILPS(Imm(0x4e), xIdxLow, xIdxNext)
	resMask2 := XMM()
	VCMPPS(Imm(0x0e), xValLow, xValNext, resMask2)
	VBLENDVPS(resMask2, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask2, xIdxNext, xIdxLow, xIdxLow)

	VPERMILPS(Imm(0x11), xValLow, xValNext)
	VPERMILPS(Imm(0x11), xIdxLow, xIdxNext)
	resMask3 := XMM()
	VCMPPS(Imm(0x0e), xValLow, xValNext, resMask3)
	VBLENDVPS(resMask3, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask3, xIdxNext, xIdxLow, xIdxLow)

	Label("final")
	Store(xValLow, ReturnIndex(0))
	idx_reg := GP64()
	VMOVD(xIdxLow, idx_reg.As32())
	Store(idx_reg, ReturnIndex(1))
	VZEROUPPER()
	RET()
}

// ImplementArgMinAVX2 runs the implement arg min avx2 operation.
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
	DATA(0, U32(0))
	DATA(4, U32(1))
	DATA(8, U32(2))
	DATA(12, U32(3))
	DATA(16, U32(4))
	DATA(20, U32(5))
	DATA(24, U32(6))
	DATA(28, U32(7))
	VMOVUPS(idxConst, curIdx)

	eight := GLOBL("eight_const_argmin", RODATA|NOPTR)
	DATA(0, U32(8))
	DATA(4, U32(8))
	DATA(8, U32(8))
	DATA(12, U32(8))
	DATA(16, U32(8))
	DATA(20, U32(8))
	DATA(24, U32(8))
	DATA(28, U32(8))
	VMOVUPS(eight, inc)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("done"))
	val := YMM()
	VMOVUPS(Mem{Base: src}, val)
	mask := YMM()
	VCMPPS(Imm(0x01), minVal, val, mask)
	VBLENDVPS(mask, val, minVal, minVal)
	VBLENDVPS(mask, curIdx, minIdx, minIdx)
	VPADDD(inc, curIdx, curIdx)
	ADDQ(Imm(32), src)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("done")
	xValHigh := XMM()
	VEXTRACTF128(Imm(1), minVal, xValHigh)
	xValLow := XMM()
	VEXTRACTF128(Imm(0), minVal, xValLow)
	xIdxHigh := XMM()
	VEXTRACTF128(Imm(1), minIdx, xIdxHigh)
	xIdxLow := XMM()
	VEXTRACTF128(Imm(0), minIdx, xIdxLow)
	resMask := XMM()
	VCMPPS(Imm(0x01), xValLow, xValHigh, resMask)
	VBLENDVPS(resMask, xValHigh, xValLow, xValLow)
	VBLENDVPS(resMask, xIdxHigh, xIdxLow, xIdxLow)

	xValNext := XMM()
	VPERMILPS(Imm(0x4e), xValLow, xValNext)
	xIdxNext := XMM()
	VPERMILPS(Imm(0x4e), xIdxLow, xIdxNext)
	resMask2 := XMM()
	VCMPPS(Imm(0x01), xValLow, xValNext, resMask2)
	VBLENDVPS(resMask2, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask2, xIdxNext, xIdxLow, xIdxLow)

	VPERMILPS(Imm(0x11), xValLow, xValNext)
	VPERMILPS(Imm(0x11), xIdxLow, xIdxNext)
	resMask3 := XMM()
	VCMPPS(Imm(0x01), xValLow, xValNext, resMask3)
	VBLENDVPS(resMask3, xValNext, xValLow, xValLow)
	VBLENDVPS(resMask3, xIdxNext, xIdxLow, xIdxLow)

	Label("final")
	Store(xValLow, ReturnIndex(0))
	idx_reg := GP64()
	VMOVD(xIdxLow, idx_reg.As32())
	Store(idx_reg, ReturnIndex(1))
	VZEROUPPER()
	RET()
}

// ImplementArgMaxAVX512 runs the implement arg max avx512 operation.
func ImplementArgMaxAVX512() {
	TEXT("argMaxAVX512Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0))
	Store(GP64(), ReturnIndex(1))
	RET()
}

// ImplementArgMinAVX512 runs the implement arg min avx512 operation.
func ImplementArgMinAVX512() {
	TEXT("argMinAVX512Kernel", NOSPLIT, "func(src uintptr, n int) (val float32, idx int)")
	Store(XMM(), ReturnIndex(0))
	Store(GP64(), ReturnIndex(1))
	RET()
}

// ImplementSoftmaxAVX2 runs the implement softmax avx2 operation.
func ImplementSoftmaxAVX2() {
	TEXT("softmaxAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	RET()
}

// ImplementSigmoidAVX2 runs the implement sigmoid avx2 operation.
func ImplementSigmoidAVX2() {
	TEXT("sigmoidAVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	RET()
}
