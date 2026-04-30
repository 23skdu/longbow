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

	// --- Stubs for Linker ---
	stubs := []string{
		"matchInt64AVX2Kernel", "matchInt32AVX2Kernel", "matchFloat32AVX2Kernel", "matchFloat64AVX2Kernel",
		"euclideanFloat64AVX2Kernel", "euclideanInt8AVX2Kernel", "euclideanInt16AVX2Kernel",
		"euclideanInt8Unrolled4xAVX2Kernel", "euclideanUint16AVX2Kernel", "dotInt16AVX2Kernel", "dotUint16AVX2Kernel",
		"int8ToFloat32AVX2Kernel", "uint8ToFloat32AVX2Kernel", "int16ToFloat32AVX2Kernel", "uint16ToFloat32AVX2Kernel",
		"int32ToFloat32AVX2Kernel", "uint32ToFloat32AVX2Kernel", "float16ToFloat32AVX2Kernel",
		"sigmoidAVX2Kernel", "softmaxAVX2Kernel", "expAVX2Kernel", "logAVX2Kernel",
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

	Generate()
}
