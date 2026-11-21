package families

import (
	"fmt"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

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

	y0 := YMM()
	VMOVUPS(Mem{Base: a}, y0)
	y1 := YMM()
	VMOVUPS(Mem{Base: b}, y1)
	VSUBPS(y1, y0, y0)
	VFMADD231PS(y0, y0, ySum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	// Horizontal reduction
	xLow := XMM()
	VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xFinal := XMM()
	VMOVHLPS(xHigh, xFinal, xFinal)
	VADDPS(xFinal, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDSS(xNext, xHigh, xHigh)

	// Tail handling (scalar)
	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM()
	VMOVSS(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSS(Mem{Base: b}, x1)
	VSUBSS(x1, x0, x0)
	VFMADD231SS(x0, x0, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(xHigh, Mem{Base: res})
	VZEROUPPER()
	RET()
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

	y0 := YMM()
	VMOVUPS(Mem{Base: a}, y0)
	y1 := YMM()
	VMOVUPS(Mem{Base: b}, y1)
	VFMADD231PS(y0, y1, ySum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	xLow := XMM()
	VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), ySum, xHigh)
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

	x0 := XMM()
	VMOVSS(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSS(Mem{Base: b}, x1)
	VFMADD231SS(x0, x1, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	VMOVSS(xHigh, Mem{Base: res})
	VZEROUPPER()
	RET()
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

	z0 := ZMM()
	VMOVUPS(Mem{Base: a}, z0)
	z1 := ZMM()
	VMOVUPS(Mem{Base: b}, z1)
	VSUBPS(z1, z0, z0)
	VFMADD231PS(z0, z0, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
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

	x0 := XMM()
	VMOVSS(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSS(Mem{Base: b}, x1)
	VSUBSS(x1, x0, x0)
	VFMADD231SS(x0, x0, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
	RET()
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

	z0 := ZMM()
	VMOVUPS(Mem{Base: a}, z0)
	z1 := ZMM()
	VMOVUPS(Mem{Base: b}, z1)
	VFMADD231PS(z0, z1, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
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

	x0 := XMM()
	VMOVSS(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSS(Mem{Base: b}, x1)
	VFMADD231SS(x0, x1, xHigh)

	ADDQ(Imm(4), a)
	ADDQ(Imm(4), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	Store(xHigh, ReturnIndex(0))
	VZEROUPPER()
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
		y0 := YMM()
		VMOVUPS(Mem{Base: a, Disp: i * 4}, y0)
		y1 := YMM()
		VMOVUPS(Mem{Base: b, Disp: i * 4}, y1)
		VSUBPS(y1, y0, y0)
		VFMADD231PS(y0, y0, acc)
	}

	// Reduce YMM
	xLow := XMM()
	VEXTRACTF128(Imm(0), acc, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), acc, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xSum := XMM()
	VMOVHLPS(xHigh, xSum, xSum)
	VADDPS(xSum, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
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
		y0 := YMM()
		VMOVUPS(Mem{Base: a2, Disp: i * 4}, y0)
		y1 := YMM()
		VMOVUPS(Mem{Base: b2, Disp: i * 4}, y1)
		VFMADD231PS(y0, y1, acc2)
	}

	// Reduce YMM
	xLow2 := XMM()
	VEXTRACTF128(Imm(0), acc2, xLow2)
	xHigh2 := XMM()
	VEXTRACTF128(Imm(1), acc2, xHigh2)
	VADDPS(xLow2, xHigh2, xHigh2)
	xSum2 := XMM()
	VMOVHLPS(xHigh2, xSum2, xSum2)
	VADDPS(xSum2, xHigh2, xHigh2)
	xNext2 := XMM()
	VMOVSHDUP(xHigh2, xNext2)
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
		y0 := YMM()
		VMOVUPS(Mem{Base: a3, Disp: i * 4}, y0)
		y1 := YMM()
		VMOVUPS(Mem{Base: b3, Disp: i * 4}, y1)
		VSUBPS(y1, y0, y0)
		VFMADD231PS(y0, y0, acc3)
	}

	// Reduce YMM
	xLow3 := XMM()
	VEXTRACTF128(Imm(0), acc3, xLow3)
	xHigh3 := XMM()
	VEXTRACTF128(Imm(1), acc3, xHigh3)
	VADDPS(xLow3, xHigh3, xHigh3)
	xSum3 := XMM()
	VMOVHLPS(xHigh3, xSum3, xSum3)
	VADDPS(xSum3, xHigh3, xHigh3)
	xNext3 := XMM()
	VMOVSHDUP(xHigh3, xNext3)
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
		z0 := ZMM()
		VMOVUPS(Mem{Base: a, Disp: i * 4}, z0)
		z1 := ZMM()
		VMOVUPS(Mem{Base: b, Disp: i * 4}, z1)
		VSUBPS(z1, z0, z0)
		VFMADD231PS(z0, z0, acc)
	}

	// Reduce ZMM
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), acc, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), acc, yHigh)
	VADDPS(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xSum := XMM()
	VMOVHLPS(xHigh, xSum, xSum)
	VADDPS(xSum, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
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
		z0 := ZMM()
		VMOVUPS(Mem{Base: a2, Disp: i * 4}, z0)
		z1 := ZMM()
		VMOVUPS(Mem{Base: b2, Disp: i * 4}, z1)
		VFMADD231PS(z0, z1, acc2)
	}

	// Reduce ZMM
	yLow2 := YMM()
	VEXTRACTF64X4(Imm(0), acc2, yLow2)
	yHigh2 := YMM()
	VEXTRACTF64X4(Imm(1), acc2, yHigh2)
	VADDPS(yLow2, yHigh2, yHigh2)

	xLow2 := XMM()
	VEXTRACTF128(Imm(0), yHigh2, xLow2)
	xHigh2 := XMM()
	VEXTRACTF128(Imm(1), yHigh2, xHigh2)
	VADDPS(xLow2, xHigh2, xHigh2)
	xSum2 := XMM()
	VMOVHLPS(xHigh2, xSum2, xSum2)
	VADDPS(xSum2, xHigh2, xHigh2)
	xNext2 := XMM()
	VMOVSHDUP(xHigh2, xNext2)
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
		z0 := ZMM()
		VMOVUPS(Mem{Base: a3, Disp: i * 4}, z0)
		z1 := ZMM()
		VMOVUPS(Mem{Base: b3, Disp: i * 4}, z1)
		VSUBPS(z1, z0, z0)
		VFMADD231PS(z0, z0, acc3)
	}
	yLow3 := YMM()
	VEXTRACTF64X4(Imm(0), acc3, yLow3)
	yHigh3 := YMM()
	VEXTRACTF64X4(Imm(1), acc3, yHigh3)
	VADDPS(yLow3, yHigh3, yHigh3)
	xLow3 := XMM()
	VEXTRACTF128(Imm(0), yHigh3, xLow3)
	xHigh3 := XMM()
	VEXTRACTF128(Imm(1), yHigh3, xHigh3)
	VADDPS(xLow3, xHigh3, xHigh3)
	xSum3 := XMM()
	VMOVHLPS(xHigh3, xSum3, xSum3)
	VADDPS(xSum3, xHigh3, xHigh3)
	xNext3 := XMM()
	VMOVSHDUP(xHigh3, xNext3)
	VADDSS(xNext3, xHigh3, xHigh3)

	VSQRTSS(xHigh3, xHigh3, xHigh3) // Euclidean needs sqrt
	Store(xHigh3, ReturnIndex(0))
	VZEROUPPER()
	RET()
}
