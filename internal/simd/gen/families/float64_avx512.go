package families

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// ImplementEuclideanFloat64AVX512 runs the implement euclidean float64 avx512 operation.
func ImplementEuclideanFloat64AVX512() {
	TEXT("euclideanFloat64AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPD(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	z0 := ZMM()
	VMOVUPD(Mem{Base: a}, z0)
	z1 := ZMM()
	VMOVUPD(Mem{Base: b}, z1)
	VSUBPD(z1, z0, z0)
	VFMADD231PD(z0, z0, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPD(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
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
	VSQRTSD(xHigh, xHigh, xHigh)
	xResult := XMM()
	VCVTSD2SS(xHigh, xHigh, xResult)
	Store(xResult, ReturnIndex(0))
	VZEROUPPER()
	RET()
}

// ImplementDotFloat64AVX512 runs the implement dot float64 avx512 operation.
func ImplementDotFloat64AVX512() {
	TEXT("dotFloat64AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPD(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	z0 := ZMM()
	VMOVUPD(Mem{Base: a}, z0)
	z1 := ZMM()
	VMOVUPD(Mem{Base: b}, z1)
	VFMADD231PD(z0, z1, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPD(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
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
	VFMADD231SD(x0, x1, xHigh)

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

// ImplementL2SquaredFloat64AVX512 runs the implement l2 squared float64 avx512 operation.
func ImplementL2SquaredFloat64AVX512() {
	TEXT("l2SquaredFloat64AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zSum := ZMM()
	VXORPD(zSum, zSum, zSum)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	z0 := ZMM()
	VMOVUPD(Mem{Base: a}, z0)
	z1 := ZMM()
	VMOVUPD(Mem{Base: b}, z1)
	VSUBPD(z1, z0, z0)
	VFMADD231PD(z0, z0, zSum)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), zSum, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), zSum, yHigh)
	VADDPD(yLow, yHigh, yHigh)

	xLow := XMM()
	VEXTRACTF128(Imm(0), yHigh, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), yHigh, xHigh)
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

// ImplementCosineFloat64AVX512 runs the implement cosine float64 avx512 operation.
func ImplementCosineFloat64AVX512() {
	TEXT("cosineFloat64AVX512Kernel", NOSPLIT, "func(a, b uintptr, n int) (dot, normA, normB float32)")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	n := Load(Param("n"), GP64())

	zDot := ZMM()
	zNormA := ZMM()
	zNormB := ZMM()
	VXORPD(zDot, zDot, zDot)
	VXORPD(zNormA, zNormA, zNormA)
	VXORPD(zNormB, zNormB, zNormB)

	Label("loop")
	CMPQ(n, Imm(8))
	JL(LabelRef("tail"))

	za := ZMM()
	VMOVUPD(Mem{Base: a}, za)
	zb := ZMM()
	VMOVUPD(Mem{Base: b}, zb)

	VFMADD231PD(za, zb, zDot)
	VFMADD231PD(za, za, zNormA)
	VFMADD231PD(zb, zb, zNormB)

	ADDQ(Imm(64), a)
	ADDQ(Imm(64), b)
	SUBQ(Imm(8), n)
	JMP(LabelRef("loop"))

	Label("tail")
	reduceZMMtoScalar := func(z reg.VecVirtual) reg.VecVirtual {
		yl := YMM()
		VEXTRACTF64X4(Imm(0), z, yl)
		yh := YMM()
		VEXTRACTF64X4(Imm(1), z, yh)
		VADDPD(yl, yh, yh)
		xl := XMM()
		VEXTRACTF128(Imm(0), yh, xl)
		xh := XMM()
		VEXTRACTF128(Imm(1), yh, xh)
		VADDPD(xl, xh, xh)
		xs := XMM()
		VUNPCKHPD(xh, xh, xs)
		VADDSD(xs, xh, xh)
		return xh
	}
	xDot := reduceZMMtoScalar(zDot)
	xNormA := reduceZMMtoScalar(zNormA)
	xNormB := reduceZMMtoScalar(zNormB)

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
