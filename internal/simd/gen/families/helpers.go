package families

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// ReduceYMM horizontally sums all 8 float32 lanes in y into an XMM scalar.
func ReduceYMM(y reg.VecVirtual) reg.VecVirtual {
	xLow := XMM()
	VEXTRACTF128(Imm(0), y, xLow)
	xHigh := XMM()
	VEXTRACTF128(Imm(1), y, xHigh)
	VADDPS(xLow, xHigh, xHigh)
	xNext := XMM()
	VMOVSHDUP(xHigh, xNext)
	VADDPS(xNext, xHigh, xHigh)
	xSum := XMM()
	VMOVHLPS(xHigh, xHigh, xSum)
	VADDSS(xSum, xHigh, xHigh)
	return xHigh
}

// ReduceZMM horizontally sums all 16 float32 lanes in z into an XMM scalar.
func ReduceZMM(z reg.VecVirtual) reg.VecVirtual {
	yLow := YMM()
	VEXTRACTF64X4(Imm(0), z, yLow)
	yHigh := YMM()
	VEXTRACTF64X4(Imm(1), z, yHigh)
	VADDPS(yLow, yHigh, yHigh)
	return ReduceYMM(yHigh)
}

// EmitFloat32MicroKernels emits fixed 8/16-element micro-kernels.
func EmitFloat32MicroKernels() {
	TEXT("euclidean8AVX2", NOSPLIT, "func(a, b uintptr) float32")
	a := Load(Param("a"), GP64())
	b := Load(Param("b"), GP64())
	y0 := YMM()
	VMOVUPS(Mem{Base: a}, y0)
	y1 := YMM()
	VMOVUPS(Mem{Base: b}, y1)
	VSUBPS(y1, y0, y0)
	VMULPS(y0, y0, y0)
	Store(ReduceYMM(y0), ReturnIndex(0))
	VZEROUPPER()
	RET()

	TEXT("dot8AVX2", NOSPLIT, "func(a, b uintptr) float32")
	a2 := Load(Param("a"), GP64())
	b2 := Load(Param("b"), GP64())
	y0b := YMM()
	VMOVUPS(Mem{Base: a2}, y0b)
	y1b := YMM()
	VMOVUPS(Mem{Base: b2}, y1b)
	VMULPS(y0b, y1b, y0b)
	Store(ReduceYMM(y0b), ReturnIndex(0))
	VZEROUPPER()
	RET()

	TEXT("euclidean16AVX512", NOSPLIT, "func(a, b uintptr) float32")
	a3 := Load(Param("a"), GP64())
	b3 := Load(Param("b"), GP64())
	z0 := ZMM()
	VMOVUPS(Mem{Base: a3}, z0)
	z1 := ZMM()
	VMOVUPS(Mem{Base: b3}, z1)
	VSUBPS(z1, z0, z0)
	VMULPS(z0, z0, z0)
	Store(ReduceZMM(z0), ReturnIndex(0))
	VZEROUPPER()
	RET()

	TEXT("dot16AVX512", NOSPLIT, "func(a, b uintptr) float32")
	a4 := Load(Param("a"), GP64())
	b4 := Load(Param("b"), GP64())
	z0d := ZMM()
	VXORPS(z0d, z0d, z0d)
	z1d := ZMM()
	VMOVUPS(Mem{Base: a4}, z1d)
	z2d := ZMM()
	VMOVUPS(Mem{Base: b4}, z2d)
	VFMADD231PS(z1d, z2d, z0d)
	Store(ReduceZMM(z0d), ReturnIndex(0))
	VZEROUPPER()
	RET()

}

// EmitCosine8AVX2 emits the fixed 8-element cosine kernel.
func EmitCosine8AVX2() {
	TEXT("cosine8AVX2", NOSPLIT, "func(a, b uintptr) (dot, normA, normB float32)")
	ac := Load(Param("a"), GP64())
	bc := Load(Param("b"), GP64())
	ya := YMM()
	VMOVUPS(Mem{Base: ac}, ya)
	yb := YMM()
	VMOVUPS(Mem{Base: bc}, yb)

	// Dot product: ya * yb
	ydot := YMM()
	VMULPS(ya, yb, ydot)
	// NormA: ya * ya
	yna := YMM()
	VMULPS(ya, ya, yna)
	// NormB: yb * yb
	ynb := YMM()
	VMULPS(yb, yb, ynb)

	Store(ReduceYMM(ydot), ReturnIndex(0))
	Store(ReduceYMM(yna), ReturnIndex(1))
	Store(ReduceYMM(ynb), ReturnIndex(2))
	VZEROUPPER()
	RET()
}

// EmitMatMulAVX2 emits the matrix-multiply and prefetch kernels.
func EmitMatMulAVX2() {
	TEXT("prefetchNTA", NOSPLIT, "func(p uintptr)")
	p := Load(Param("p"), GP64())
	PREFETCHNTA(Mem{Base: p})
	RET()

	TEXT("matMulAVX2Kernel", NOSPLIT, "func(a, b, dst uintptr, m, n, k int)")
	aBase := Load(Param("a"), GP64())
	bBase := Load(Param("b"), GP64())
	dstBase := Load(Param("dst"), GP64())
	m_val := Load(Param("m"), GP64())
	n_val := Load(Param("n"), GP64())
	k_val := Load(Param("k"), GP64())

	i_reg := GP64()
	XORQ(i_reg, i_reg)
	Label("m_loop")
	CMPQ(i_reg, m_val)
	JE(LabelRef("m_done"))

	l_reg := GP64()
	XORQ(l_reg, l_reg)
	Label("k_loop")
	CMPQ(l_reg, k_val)
	JE(LabelRef("k_done"))

	// va = a[i*k + l]
	idxA := GP64()
	MOVQ(i_reg, idxA)
	IMULQ(k_val, idxA)
	ADDQ(l_reg, idxA)
	va := XMM()
	VMOVSS(Mem{Base: aBase, Index: idxA, Scale: 4}, va)
	vaY := YMM()
	VBROADCASTSS(va, vaY)

	j_reg := GP64()
	XORQ(j_reg, j_reg)
	Label("n_loop")
	CMPQ(j_reg, n_val)
	JGE(LabelRef("n_done"))

	// dst[i*n + j] += va * b[l*n + j]
	idxB := GP64()
	MOVQ(l_reg, idxB)
	IMULQ(n_val, idxB)
	ADDQ(j_reg, idxB)
	idxDst := GP64()
	MOVQ(i_reg, idxDst)
	IMULQ(n_val, idxDst)
	ADDQ(j_reg, idxDst)

	vb := YMM()
	VMOVUPS(Mem{Base: bBase, Index: idxB, Scale: 4}, vb)
	vdst := YMM()
	VMOVUPS(Mem{Base: dstBase, Index: idxDst, Scale: 4}, vdst)

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
	VZEROUPPER()
	RET()
}
