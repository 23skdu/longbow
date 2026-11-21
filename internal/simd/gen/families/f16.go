package families

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func ImplementEuclideanF16AVX2() {
	TEXT("euclideanF16AVX2Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	ySum := YMM()
	VXORPS(ySum, ySum, ySum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	y0 := YMM()
	VCVTPH2PS(Mem{Base: a}, y0)
	y1 := YMM()
	VCVTPH2PS(Mem{Base: b}, y1)
	VSUBPS(y1, y0, y0)
	VFMADD231PS(y0, y0, ySum)

	ADDQ(Imm(16), a)
	ADDQ(Imm(16), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	xLow := XMM()
	VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDPS(xNext, xHigh, xHigh)
	xSum := XMM()
	VMOVHLPS(xHigh, xHigh, xSum)
	VADDSS(xSum, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	tempX := XMM()
	VXORPS(tempX, tempX, tempX)
	tempY := XMM()
	VXORPS(tempY, tempY, tempY)

	regA := GP32()
	MOVWLZX(Mem{Base: a}, regA)
	PINSRW(Imm(0), regA, tempX)
	regB := GP32()
	MOVWLZX(Mem{Base: b}, regB)
	PINSRW(Imm(0), regB, tempY)

	fA := XMM()
	VCVTPH2PS(tempX, fA)
	fB := XMM()
	VCVTPH2PS(tempY, fB)

	diff := XMM()
	VSUBSS(fB, fA, diff)
	VFMADD231SS(diff, diff, xHigh)

	ADDQ(Imm(2), a)
	ADDQ(Imm(2), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VSQRTSS(xHigh, xHigh, xHigh)
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

func ImplementDotF16AVX2() {
	TEXT("dotF16AVX2Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	ySum := YMM()
	VXORPS(ySum, ySum, ySum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	y0 := YMM()
	VCVTPH2PS(Mem{Base: a}, y0)
	y1 := YMM()
	VCVTPH2PS(Mem{Base: b}, y1)
	VFMADD231PS(y0, y1, ySum)

	ADDQ(Imm(16), a)
	ADDQ(Imm(16), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	xLow := XMM()
	VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDPS(xNext, xHigh, xHigh)
	xSum := XMM()
	VMOVHLPS(xHigh, xHigh, xSum)
	VADDSS(xSum, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	tempX := XMM()
	VXORPS(tempX, tempX, tempX)
	tempY := XMM()
	VXORPS(tempY, tempY, tempY)

	regA := GP32()
	MOVWLZX(Mem{Base: a}, regA)
	PINSRW(Imm(0), regA, tempX)
	regB := GP32()
	MOVWLZX(Mem{Base: b}, regB)
	PINSRW(Imm(0), regB, tempY)

	fA := XMM()
	VCVTPH2PS(tempX, fA)
	fB := XMM()
	VCVTPH2PS(tempY, fB)

	VFMADD231SS(fA, fB, xHigh)

	ADDQ(Imm(2), a)
	ADDQ(Imm(2), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

func ImplementEuclideanF16AVX512() {
	TEXT("euclideanF16AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPS(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(16))
	JL(LabelRef("tail"))

	z0 := ZMM()
	VCVTPH2PS(Mem{Base: a}, z0)
	z1 := ZMM()
	VCVTPH2PS(Mem{Base: b}, z1)
	VSUBPS(z1, z0, z0)
	VFMADD231PS(z0, z0, zSum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(16), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPS(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM()
	VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	tempX := XMM()
	VXORPS(tempX, tempX, tempX)
	tempY := XMM()
	VXORPS(tempY, tempY, tempY)

	regA := GP32()
	MOVWLZX(Mem{Base: a}, regA)
	PINSRW(Imm(0), regA, tempX)
	regB := GP32()
	MOVWLZX(Mem{Base: b}, regB)
	PINSRW(Imm(0), regB, tempY)

	fA := XMM()
	VCVTPH2PS(tempX, fA)
	fB := XMM()
	VCVTPH2PS(tempY, fB)

	diff := XMM()
	VSUBSS(fB, fA, diff)
	VFMADD231SS(diff, diff, xHigh)

	ADDQ(Imm(2), a)
	ADDQ(Imm(2), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VSQRTSS(xHigh, xHigh, xHigh)
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

func ImplementDotF16AVX512() {
	TEXT("dotF16AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPS(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(16))
	JL(LabelRef("tail"))

	z0 := ZMM()
	VCVTPH2PS(Mem{Base: a}, z0)
	z1 := ZMM()
	VCVTPH2PS(Mem{Base: b}, z1)
	VFMADD231PS(z0, z1, zSum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(16), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPS(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM()
	VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	tempX := XMM()
	VXORPS(tempX, tempX, tempX)
	tempY := XMM()
	VXORPS(tempY, tempY, tempY)

	regA := GP32()
	MOVWLZX(Mem{Base: a}, regA)
	PINSRW(Imm(0), regA, tempX)
	regB := GP32()
	MOVWLZX(Mem{Base: b}, regB)
	PINSRW(Imm(0), regB, tempY)

	fA := XMM()
	VCVTPH2PS(tempX, fA)
	fB := XMM()
	VCVTPH2PS(tempY, fB)

	VFMADD231SS(fA, fB, xHigh)

	ADDQ(Imm(2), a)
	ADDQ(Imm(2), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()
}
