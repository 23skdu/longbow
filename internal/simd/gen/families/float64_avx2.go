package families

import (
	"fmt"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// ImplementEuclideanFloat64AVX2 runs the implement euclidean float64 avx2 operation.
func ImplementEuclideanFloat64AVX2() {
	implementFloat64AVX2Pairwise(
		"euclideanFloat64AVX2Kernel",
		true,
		func(y0, y1, acc reg.VecVirtual) {
			VSUBPD(y1, y0, y0)
			VFMADD231PD(y0, y0, acc)
		},
		func(x0, x1, xHigh reg.VecVirtual) {
			VSUBSD(x1, x0, x0)
			VFMADD231SD(x0, x0, xHigh)
		},
	)
}

// ImplementDotFloat64AVX2 runs the implement dot float64 avx2 operation.
func ImplementDotFloat64AVX2() {
	implementFloat64AVX2Pairwise(
		"dotFloat64AVX2Kernel",
		false,
		func(y0, y1, acc reg.VecVirtual) {
			VFMADD231PD(y0, y1, acc)
		},
		func(x0, x1, xHigh reg.VecVirtual) {
			VFMADD231SD(x0, x1, xHigh)
		},
	)
}

func implementFloat64AVX2Pairwise(
	name string,
	doSqrt bool,
	vecStep func(y0, y1, acc reg.VecVirtual),
	scalarStep func(x0, x1, xHigh reg.VecVirtual),
) {
	TEXT(name, NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	const unroll = 8
	acc := make([]reg.VecVirtual, unroll)
	for i := range acc {
		acc[i] = YMM()
		VXORPD(acc[i], acc[i], acc[i])
	}

	Label("loop")
	CMPQ(n, Imm(4*unroll))
	JL(LabelRef("tail"))

	for i := 0; i < unroll; i++ {
		y0 := YMM()
		VMOVUPD(Mem{Base: a, Disp: 32 * i}, y0)
		y1 := YMM()
		VMOVUPD(Mem{Base: b, Disp: 32 * i}, y1)
		vecStep(y0, y1, acc[i])
	}

	ADDQ(U32(32*unroll), a)
	ADDQ(U32(32*unroll), b)
	SUBQ(U32(4*unroll), n)
	JMP(LabelRef("loop"))

	Label("tail")
	CMPQ(n, Imm(4))
	JL(LabelRef("reduce"))

	y0 := YMM()
	VMOVUPD(Mem{Base: a}, y0)
	y1 := YMM()
	VMOVUPD(Mem{Base: b}, y1)
	vecStep(y0, y1, acc[0])

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(4), n)
	JMP(LabelRef("tail"))

	Label("reduce")
	for i := 0; i+1 < len(acc); i += 2 {
		VADDPD(acc[i+1], acc[i], acc[i])
	}
	for i := 0; i+2 < len(acc); i += 4 {
		VADDPD(acc[i+2], acc[i], acc[i])
	}
	if len(acc) > 4 {
		VADDPD(acc[4], acc[0], acc[0])
	}

	xLow := XMM()
	VEXTRACTF128(Imm(0), acc[0], xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), acc[0], xHigh)
	VADDPD(xLow, xHigh, xHigh)

	xSwap := XMM()
	VUNPCKHPD(xHigh, xHigh, xSwap)
	VADDSD(xSwap, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM()
	VMOVSD(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSD(Mem{Base: b}, x1)
	scalarStep(x0, x1, xHigh)

	ADDQ(Imm(8), a)
	ADDQ(Imm(8), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	if doSqrt {
		VSQRTSD(xHigh, xHigh, xHigh)
	}
	xResult := XMM()
	VCVTSD2SS(xHigh, xHigh, xResult)
	Store(xResult, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// ImplementL2SquaredFloat64AVX2 runs the implement l2 squared float64 avx2 operation.
func ImplementL2SquaredFloat64AVX2() {
	TEXT("l2SquaredFloat64AVX2Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	ySum := YMM()
	VXORPD(ySum, ySum, ySum)

	Label("loop")
	CMPQ(n, Imm(4))
	JL(LabelRef("tail"))

	y0 := YMM()
	VMOVUPD(Mem{Base: a}, y0)
	y1 := YMM()
	VMOVUPD(Mem{Base: b}, y1)
	VSUBPD(y1, y0, y0)
	VFMADD231PD(y0, y0, ySum)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(4), n)
	JMP(LabelRef("loop"))

	Label("tail")
	xLow := XMM()
	VEXTRACTF128(Imm(0), ySum, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), ySum, xHigh)
	VADDPD(xLow, xHigh, xHigh)

	xSwap := XMM()
	VUNPCKHPD(xHigh, xHigh, xSwap)
	VADDSD(xSwap, xHigh, xHigh)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("done"))

	x0 := XMM()
	VMOVSD(Mem{Base: a}, x0)
	x1 := XMM()
	VMOVSD(Mem{Base: b}, x1)
	VSUBSD(x1, x0, x0)
	VFMADD231SD(x0, x0, xHigh)

	ADDQ(Imm(8), a)
	ADDQ(Imm(8), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("done")
	xResult := XMM()
	VCVTSD2SS(xHigh, xHigh, xResult)
	Store(xResult, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// ImplementCosineFloat64AVX2 runs the implement cosine float64 avx2 operation.
func ImplementCosineFloat64AVX2() {
	TEXT("cosineFloat64AVX2Kernel", NOSPLIT, "func(a, b uintptr, n int) (dot, normA, normB float32)")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	yDot := YMM()
	yNormA := YMM()
	yNormB := YMM()
	VXORPD(yDot, yDot, yDot)
	VXORPD(yNormA, yNormA, yNormA)
	VXORPD(yNormB, yNormB, yNormB)

	Label("loop")
	CMPQ(n, Imm(4))
	JL(LabelRef("tail"))

	ya := YMM()
	VMOVUPD(Mem{Base: a}, ya)
	yb := YMM()
	VMOVUPD(Mem{Base: b}, yb)

	VFMADD231PD(ya, yb, yDot)
	VFMADD231PD(ya, ya, yNormA)
	VFMADD231PD(yb, yb, yNormB)

	ADDQ(Imm(32), a)
	ADDQ(Imm(32), b)
	SUBQ(Imm(4), n)
	JMP(LabelRef("loop"))

	Label("tail")
	reducePD := func(y reg.VecVirtual) reg.VecVirtual {
		xL := XMM()
		VEXTRACTF128(Imm(0), y, xL)
		xH := XMM()
		VEXTRACTF128(Imm(1), y, xH)
		VADDPD(xL, xH, xH)
		xS := XMM()
		VUNPCKHPD(xH, xH, xS)
		VADDSD(xS, xH, xH)
		return xH
	}
	xDot := reducePD(yDot)
	xNormA := reducePD(yNormA)
	xNormB := reducePD(yNormB)

	Label("scalar_loop")
	CMPQ(n, Imm(0))
	JE(LabelRef("store"))

	xs0 := XMM()
	VMOVSD(Mem{Base: a}, xs0)
	xs1 := XMM()
	VMOVSD(Mem{Base: b}, xs1)
	VFMADD231SD(xs0, xs1, xDot)
	VFMADD231SD(xs0, xs0, xNormA)
	VFMADD231SD(xs1, xs1, xNormB)

	ADDQ(Imm(8), a)
	ADDQ(Imm(8), b)
	DECQ(n)
	JMP(LabelRef("scalar_loop"))

	Label("store")
	xResDot := XMM()
	VCVTSD2SS(xDot, xDot, xResDot)
	xResNormA := XMM()
	VCVTSD2SS(xNormA, xNormA, xResNormA)
	xResNormB := XMM()
	VCVTSD2SS(xNormB, xNormB, xResNormB)
	Store(xResDot, ReturnIndex(0))
	Store(xResNormA, ReturnIndex(1))
	Store(xResNormB, ReturnIndex(2))
	VZEROUPPER()
	RET()
}

// ImplementSpecializedFloat64AVX2 runs the implement specialized float64 avx2 operation.
func ImplementSpecializedFloat64AVX2(dim int) {
	// L2Squared specialized (no sqrt)
	TEXT(fmt.Sprintf("l2Squared%dFloat64AVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())

	acc := YMM()
	VXORPD(acc, acc, acc)

	for i := 0; i < dim; i += 4 {
		y0 := YMM()
		VMOVUPD(Mem{Base: a, Disp: i * 8}, y0)
		y1 := YMM()
		VMOVUPD(Mem{Base: b, Disp: i * 8}, y1)
		VSUBPD(y1, y0, y0)
		VFMADD231PD(y0, y0, acc)
	}

	xLow := XMM()
	VEXTRACTF128(Imm(0), acc, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), acc, xHigh)
	VADDPD(xLow, xHigh, xHigh)
	xSwap := XMM()
	VUNPCKHPD(xHigh, xHigh, xSwap)
	VADDSD(xSwap, xHigh, xHigh)

	xRes := XMM()
	VCVTSD2SS(xHigh, xHigh, xRes)
	Store(xRes, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Dot specialized
	TEXT(fmt.Sprintf("dot%dFloat64AVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a2 := Load(Param("a"), GP64())
	b2 := Load(Param("b"), GP64())

	acc2 := YMM()
	VXORPD(acc2, acc2, acc2)

	for i := 0; i < dim; i += 4 {
		y0 := YMM()
		VMOVUPD(Mem{Base: a2, Disp: i * 8}, y0)
		y1 := YMM()
		VMOVUPD(Mem{Base: b2, Disp: i * 8}, y1)
		VFMADD231PD(y0, y1, acc2)
	}

	xLow2 := XMM()
	VEXTRACTF128(Imm(0), acc2, xLow2)
	xHigh2 := XMM()
	VEXTRACTF128(Imm(1), acc2, xHigh2)
	VADDPD(xLow2, xHigh2, xHigh2)
	xSwap2 := XMM()
	VUNPCKHPD(xHigh2, xHigh2, xSwap2)
	VADDSD(xSwap2, xHigh2, xHigh2)

	xRes2 := XMM()
	VCVTSD2SS(xHigh2, xHigh2, xRes2)
	Store(xRes2, ReturnIndex(0))
	VZEROUPPER()
	RET()

	// Euclidean specialized (with sqrt)
	TEXT(fmt.Sprintf("euclidean%dFloat64AVX2Kernel", dim), NOSPLIT, "func(a, b uintptr) float32")
	a3 := Load(Param("a"), GP64())
	b3 := Load(Param("b"), GP64())

	acc3 := YMM()
	VXORPD(acc3, acc3, acc3)

	for i := 0; i < dim; i += 4 {
		y0 := YMM()
		VMOVUPD(Mem{Base: a3, Disp: i * 8}, y0)
		y1 := YMM()
		VMOVUPD(Mem{Base: b3, Disp: i * 8}, y1)
		VSUBPD(y1, y0, y0)
		VFMADD231PD(y0, y0, acc3)
	}

	xLow3 := XMM()
	VEXTRACTF128(Imm(0), acc3, xLow3)
	xHigh3 := XMM()
	VEXTRACTF128(Imm(1), acc3, xHigh3)
	VADDPD(xLow3, xHigh3, xHigh3)
	xSwap3 := XMM()
	VUNPCKHPD(xHigh3, xHigh3, xSwap3)
	VADDSD(xSwap3, xHigh3, xHigh3)

	VSQRTSD(xHigh3, xHigh3, xHigh3)
	xRes3 := XMM()
	VCVTSD2SS(xHigh3, xHigh3, xRes3)
	Store(xRes3, ReturnIndex(0))
	VZEROUPPER()
	RET()
}
