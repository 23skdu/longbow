//go:build ignore

package main

import (
	"math"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	ImplementSinFloat32AVX2()
	ImplementCosFloat32AVX2()
	ImplementSincosFloat32AVX2()
	ImplementAtan2Float32AVX2()
	Generate()
}

func ImplementSinFloat32AVX2() {
	TEXT("SinFloat32AVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	// c1 = -1/6
	// c2 = 1/120
	// c3 = -1/5040
	c1 := GLOBL("sin_c1", RODATA|NOPTR)
	DATA(0, F32(-1.0/6.0)); DATA(4, F32(-1.0/6.0)); DATA(8, F32(-1.0/6.0)); DATA(12, F32(-1.0/6.0))
	DATA(16, F32(-1.0/6.0)); DATA(20, F32(-1.0/6.0)); DATA(24, F32(-1.0/6.0)); DATA(28, F32(-1.0/6.0))
	
	c2 := GLOBL("sin_c2", RODATA|NOPTR)
	DATA(0, F32(1.0/120.0)); DATA(4, F32(1.0/120.0)); DATA(8, F32(1.0/120.0)); DATA(12, F32(1.0/120.0))
	DATA(16, F32(1.0/120.0)); DATA(20, F32(1.0/120.0)); DATA(24, F32(1.0/120.0)); DATA(28, F32(1.0/120.0))

	c3 := GLOBL("sin_c3", RODATA|NOPTR)
	DATA(0, F32(-1.0/5040.0)); DATA(4, F32(-1.0/5040.0)); DATA(8, F32(-1.0/5040.0)); DATA(12, F32(-1.0/5040.0))
	DATA(16, F32(-1.0/5040.0)); DATA(20, F32(-1.0/5040.0)); DATA(24, F32(-1.0/5040.0)); DATA(28, F32(-1.0/5040.0))

	yC1, yC2, yC3 := YMM(), YMM(), YMM()
	VMOVUPS(c1, yC1); VMOVUPS(c2, yC2); VMOVUPS(c3, yC3)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	x := YMM(); VMOVUPS(Mem{Base: src}, x)
	s := YMM(); VMULPS(x, x, s) // s = x*x

	// p = c3
	p := YMM(); VMOVUPS(yC3, p)
	// p = c2 + s*p
	VFMADD213PS(yC2, s, p)
	// p = c1 + s*p
	VFMADD213PS(yC1, s, p)
	// res = x + (x*s)*p
	xs := YMM(); VMULPS(x, s, xs)
	VFMADD213PS(x, xs, p)

	VMOVUPS(p, Mem{Base: dst})

	ADDQ(Imm(32), src); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))

	x1 := XMM(); VMOVSS(Mem{Base: src}, x1)
	s1 := XMM(); VMULSS(x1, x1, s1)
	p1 := XMM(); VMOVSS(c3, p1)
	c2x := XMM(); VMOVSS(c2, c2x)
	VFMADD213SS(c2x, s1, p1)
	c1x := XMM(); VMOVSS(c1, c1x)
	VFMADD213SS(c1x, s1, p1)
	xs1 := XMM(); VMULSS(x1, s1, xs1)
	VFMADD213SS(x1, xs1, p1)

	VMOVSS(p1, Mem{Base: dst})

	ADDQ(Imm(4), src); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("tail"))

	Label("done")
	VZEROUPPER(); RET()
}

func ImplementCosFloat32AVX2() {
	TEXT("CosFloat32AVX2Kernel", NOSPLIT, "func(src, dst uintptr, n int)")
	src := Load(Param("src"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	c0 := GLOBL("cos_c0", RODATA|NOPTR)
	DATA(0, F32(1.0)); DATA(4, F32(1.0)); DATA(8, F32(1.0)); DATA(12, F32(1.0))
	DATA(16, F32(1.0)); DATA(20, F32(1.0)); DATA(24, F32(1.0)); DATA(28, F32(1.0))

	c1 := GLOBL("cos_c1", RODATA|NOPTR)
	DATA(0, F32(-1.0/2.0)); DATA(4, F32(-1.0/2.0)); DATA(8, F32(-1.0/2.0)); DATA(12, F32(-1.0/2.0))
	DATA(16, F32(-1.0/2.0)); DATA(20, F32(-1.0/2.0)); DATA(24, F32(-1.0/2.0)); DATA(28, F32(-1.0/2.0))
	
	c2 := GLOBL("cos_c2", RODATA|NOPTR)
	DATA(0, F32(1.0/24.0)); DATA(4, F32(1.0/24.0)); DATA(8, F32(1.0/24.0)); DATA(12, F32(1.0/24.0))
	DATA(16, F32(1.0/24.0)); DATA(20, F32(1.0/24.0)); DATA(24, F32(1.0/24.0)); DATA(28, F32(1.0/24.0))

	c3 := GLOBL("cos_c3", RODATA|NOPTR)
	DATA(0, F32(-1.0/720.0)); DATA(4, F32(-1.0/720.0)); DATA(8, F32(-1.0/720.0)); DATA(12, F32(-1.0/720.0))
	DATA(16, F32(-1.0/720.0)); DATA(20, F32(-1.0/720.0)); DATA(24, F32(-1.0/720.0)); DATA(28, F32(-1.0/720.0))

	yC0, yC1, yC2, yC3 := YMM(), YMM(), YMM(), YMM()
	VMOVUPS(c0, yC0); VMOVUPS(c1, yC1); VMOVUPS(c2, yC2); VMOVUPS(c3, yC3)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	x := YMM(); VMOVUPS(Mem{Base: src}, x)
	s := YMM(); VMULPS(x, x, s) // s = x*x

	p := YMM(); VMOVUPS(yC3, p)
	VFMADD213PS(yC2, s, p)
	VFMADD213PS(yC1, s, p)
	VFMADD213PS(yC0, s, p)

	VMOVUPS(p, Mem{Base: dst})

	ADDQ(Imm(32), src); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))

	x1 := XMM(); VMOVSS(Mem{Base: src}, x1)
	s1 := XMM(); VMULSS(x1, x1, s1)
	p1 := XMM(); VMOVSS(c3, p1)
	c2x := XMM(); VMOVSS(c2, c2x)
	VFMADD213SS(c2x, s1, p1)
	c1x := XMM(); VMOVSS(c1, c1x)
	VFMADD213SS(c1x, s1, p1)
	c0x := XMM(); VMOVSS(c0, c0x)
	VFMADD213SS(c0x, s1, p1)

	VMOVSS(p1, Mem{Base: dst})

	ADDQ(Imm(4), src); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("tail"))

	Label("done")
	VZEROUPPER(); RET()
}

func ImplementSincosFloat32AVX2() {
	TEXT("SincosFloat32AVX2Kernel", NOSPLIT, "func(src, sinDst, cosDst uintptr, n int)")
	src := Load(Param("src"), GP64())
	sinDst := Load(Param("sinDst"), GP64())
	cosDst := Load(Param("cosDst"), GP64())
	n := Load(Param("n"), GP64())

	sinC1 := GLOBL("sincos_sin_c1", RODATA|NOPTR)
	DATA(0, F32(-1.0/6.0)); DATA(4, F32(-1.0/6.0)); DATA(8, F32(-1.0/6.0)); DATA(12, F32(-1.0/6.0))
	DATA(16, F32(-1.0/6.0)); DATA(20, F32(-1.0/6.0)); DATA(24, F32(-1.0/6.0)); DATA(28, F32(-1.0/6.0))
	sinC2 := GLOBL("sincos_sin_c2", RODATA|NOPTR)
	DATA(0, F32(1.0/120.0)); DATA(4, F32(1.0/120.0)); DATA(8, F32(1.0/120.0)); DATA(12, F32(1.0/120.0))
	DATA(16, F32(1.0/120.0)); DATA(20, F32(1.0/120.0)); DATA(24, F32(1.0/120.0)); DATA(28, F32(1.0/120.0))
	sinC3 := GLOBL("sincos_sin_c3", RODATA|NOPTR)
	DATA(0, F32(-1.0/5040.0)); DATA(4, F32(-1.0/5040.0)); DATA(8, F32(-1.0/5040.0)); DATA(12, F32(-1.0/5040.0))
	DATA(16, F32(-1.0/5040.0)); DATA(20, F32(-1.0/5040.0)); DATA(24, F32(-1.0/5040.0)); DATA(28, F32(-1.0/5040.0))

	cosC0 := GLOBL("sincos_cos_c0", RODATA|NOPTR)
	DATA(0, F32(1.0)); DATA(4, F32(1.0)); DATA(8, F32(1.0)); DATA(12, F32(1.0))
	DATA(16, F32(1.0)); DATA(20, F32(1.0)); DATA(24, F32(1.0)); DATA(28, F32(1.0))
	cosC1 := GLOBL("sincos_cos_c1", RODATA|NOPTR)
	DATA(0, F32(-1.0/2.0)); DATA(4, F32(-1.0/2.0)); DATA(8, F32(-1.0/2.0)); DATA(12, F32(-1.0/2.0))
	DATA(16, F32(-1.0/2.0)); DATA(20, F32(-1.0/2.0)); DATA(24, F32(-1.0/2.0)); DATA(28, F32(-1.0/2.0))
	cosC2 := GLOBL("sincos_cos_c2", RODATA|NOPTR)
	DATA(0, F32(1.0/24.0)); DATA(4, F32(1.0/24.0)); DATA(8, F32(1.0/24.0)); DATA(12, F32(1.0/24.0))
	DATA(16, F32(1.0/24.0)); DATA(20, F32(1.0/24.0)); DATA(24, F32(1.0/24.0)); DATA(28, F32(1.0/24.0))
	cosC3 := GLOBL("sincos_cos_c3", RODATA|NOPTR)
	DATA(0, F32(-1.0/720.0)); DATA(4, F32(-1.0/720.0)); DATA(8, F32(-1.0/720.0)); DATA(12, F32(-1.0/720.0))
	DATA(16, F32(-1.0/720.0)); DATA(20, F32(-1.0/720.0)); DATA(24, F32(-1.0/720.0)); DATA(28, F32(-1.0/720.0))

	ySC1, ySC2, ySC3 := YMM(), YMM(), YMM()
	VMOVUPS(sinC1, ySC1); VMOVUPS(sinC2, ySC2); VMOVUPS(sinC3, ySC3)
	yCC0, yCC1, yCC2, yCC3 := YMM(), YMM(), YMM(), YMM()
	VMOVUPS(cosC0, yCC0); VMOVUPS(cosC1, yCC1); VMOVUPS(cosC2, yCC2); VMOVUPS(cosC3, yCC3)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	x := YMM(); VMOVUPS(Mem{Base: src}, x)
	s := YMM(); VMULPS(x, x, s)

	// sin
	pSin := YMM(); VMOVUPS(ySC3, pSin)
	VFMADD213PS(ySC2, s, pSin)
	VFMADD213PS(ySC1, s, pSin)
	xs := YMM(); VMULPS(x, s, xs)
	VFMADD213PS(x, xs, pSin)
	VMOVUPS(pSin, Mem{Base: sinDst})

	// cos
	pCos := YMM(); VMOVUPS(yCC3, pCos)
	VFMADD213PS(yCC2, s, pCos)
	VFMADD213PS(yCC1, s, pCos)
	VFMADD213PS(yCC0, s, pCos)
	VMOVUPS(pCos, Mem{Base: cosDst})

	ADDQ(Imm(32), src); ADDQ(Imm(32), sinDst); ADDQ(Imm(32), cosDst); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))

	x1 := XMM(); VMOVSS(Mem{Base: src}, x1)
	s1 := XMM(); VMULSS(x1, x1, s1)
	
	pSin1 := XMM(); VMOVSS(sinC3, pSin1)
	sc2x := XMM(); VMOVSS(sinC2, sc2x); VFMADD213SS(sc2x, s1, pSin1)
	sc1x := XMM(); VMOVSS(sinC1, sc1x); VFMADD213SS(sc1x, s1, pSin1)
	xs1 := XMM(); VMULSS(x1, s1, xs1)
	VFMADD213SS(x1, xs1, pSin1)
	VMOVSS(pSin1, Mem{Base: sinDst})

	pCos1 := XMM(); VMOVSS(cosC3, pCos1)
	cc2x := XMM(); VMOVSS(cosC2, cc2x); VFMADD213SS(cc2x, s1, pCos1)
	cc1x := XMM(); VMOVSS(cosC1, cc1x); VFMADD213SS(cc1x, s1, pCos1)
	cc0x := XMM(); VMOVSS(cosC0, cc0x); VFMADD213SS(cc0x, s1, pCos1)
	VMOVSS(pCos1, Mem{Base: cosDst})

	ADDQ(Imm(4), src); ADDQ(Imm(4), sinDst); ADDQ(Imm(4), cosDst); DECQ(n); JMP(LabelRef("tail"))

	Label("done")
	VZEROUPPER(); RET()
}

func ImplementAtan2Float32AVX2() {
	TEXT("Atan2Float32AVX2Kernel", NOSPLIT, "func(y, x, dst uintptr, n int)")
	yBase := Load(Param("y"), GP64())
	xBase := Load(Param("x"), GP64())
	dst := Load(Param("dst"), GP64())
	n := Load(Param("n"), GP64())

	// Constants
	c1 := GLOBL("atan_c1", RODATA|NOPTR); DATA(0, F32(-1.0/3.0)); DATA(4, F32(-1.0/3.0)); DATA(8, F32(-1.0/3.0)); DATA(12, F32(-1.0/3.0))
	DATA(16, F32(-1.0/3.0)); DATA(20, F32(-1.0/3.0)); DATA(24, F32(-1.0/3.0)); DATA(28, F32(-1.0/3.0))
	c2 := GLOBL("atan_c2", RODATA|NOPTR); DATA(0, F32(1.0/5.0)); DATA(4, F32(1.0/5.0)); DATA(8, F32(1.0/5.0)); DATA(12, F32(1.0/5.0))
	DATA(16, F32(1.0/5.0)); DATA(20, F32(1.0/5.0)); DATA(24, F32(1.0/5.0)); DATA(28, F32(1.0/5.0))
	c3 := GLOBL("atan_c3", RODATA|NOPTR); DATA(0, F32(-1.0/7.0)); DATA(4, F32(-1.0/7.0)); DATA(8, F32(-1.0/7.0)); DATA(12, F32(-1.0/7.0))
	DATA(16, F32(-1.0/7.0)); DATA(20, F32(-1.0/7.0)); DATA(24, F32(-1.0/7.0)); DATA(28, F32(-1.0/7.0))
	c4 := GLOBL("atan_c4", RODATA|NOPTR); DATA(0, F32(1.0/9.0)); DATA(4, F32(1.0/9.0)); DATA(8, F32(1.0/9.0)); DATA(12, F32(1.0/9.0))
	DATA(16, F32(1.0/9.0)); DATA(20, F32(1.0/9.0)); DATA(24, F32(1.0/9.0)); DATA(28, F32(1.0/9.0))
	piOver2 := GLOBL("pi_over_2", RODATA|NOPTR); DATA(0, F32(math.Pi/2)); DATA(4, F32(math.Pi/2)); DATA(8, F32(math.Pi/2)); DATA(12, F32(math.Pi/2))
	DATA(16, F32(math.Pi/2)); DATA(20, F32(math.Pi/2)); DATA(24, F32(math.Pi/2)); DATA(28, F32(math.Pi/2))
	pi := GLOBL("pi", RODATA|NOPTR); DATA(0, F32(math.Pi)); DATA(4, F32(math.Pi)); DATA(8, F32(math.Pi)); DATA(12, F32(math.Pi))
	DATA(16, F32(math.Pi)); DATA(20, F32(math.Pi)); DATA(24, F32(math.Pi)); DATA(28, F32(math.Pi))
	signMask := GLOBL("sign_mask", RODATA|NOPTR); DATA(0, U32(0x80000000)); DATA(4, U32(0x80000000)); DATA(8, U32(0x80000000)); DATA(12, U32(0x80000000))
	DATA(16, U32(0x80000000)); DATA(20, U32(0x80000000)); DATA(24, U32(0x80000000)); DATA(28, U32(0x80000000))

	yC1, yC2, yC3, yC4 := YMM(), YMM(), YMM(), YMM()
	VMOVUPS(c1, yC1); VMOVUPS(c2, yC2); VMOVUPS(c3, yC3); VMOVUPS(c4, yC4)
	yPiO2, yPi, ySign := YMM(), YMM(), YMM()
	VMOVUPS(piOver2, yPiO2); VMOVUPS(pi, yPi); VMOVUPS(signMask, ySign)

	Label("loop")
	CMPQ(n, Imm(8)); JL(LabelRef("tail"))

	vy := YMM(); VMOVUPS(Mem{Base: yBase}, vy)
	vx := YMM(); VMOVUPS(Mem{Base: xBase}, vx)

	absY := YMM(); VANDNPS(vy, ySign, absY)
	absX := YMM(); VANDNPS(vx, ySign, absX)

	// Invert logic: ratio = min(absY, absX) / max(absY, absX)
	vMin := YMM(); VMINPS(absX, absY, vMin)
	vMax := YMM(); VMAXPS(absX, absY, vMax)
	vRatio := YMM(); VDIVPS(vMax, vMin, vRatio)

	// Polynomial on vRatio
	s := YMM(); VMULPS(vRatio, vRatio, s)
	p := YMM(); VMOVUPS(yC4, p)
	VFMADD213PS(yC3, s, p)
	VFMADD213PS(yC2, s, p)
	VFMADD213PS(yC1, s, p)
	xs := YMM(); VMULPS(vRatio, s, xs)
	VFMADD213PS(vRatio, xs, p) // p is now atan(ratio)

	// if |y| > |x|, p = pi/2 - p
	cmpYX := YMM(); VCMPPS(Imm(0x0e), absX, absY, cmpYX) // absY > absX
	pSub := YMM(); VSUBPS(p, yPiO2, pSub)
	VBLENDVPS(cmpYX, pSub, p, p)

	// Adjust quadrant based on signs of x and y
	cmpX0 := YMM(); VXORPS(cmpX0, cmpX0, cmpX0); VCMPPS(Imm(0x01), cmpX0, vx, cmpX0) // x < 0
	piSubP := YMM(); VSUBPS(p, yPi, piSubP)
	VBLENDVPS(cmpX0, piSubP, p, p)

	// Now apply sign of y to p
	ySignBit := YMM(); VANDPS(vy, ySign, ySignBit)
	VXORPS(p, ySignBit, p)

	VMOVUPS(p, Mem{Base: dst})

	ADDQ(Imm(32), yBase); ADDQ(Imm(32), xBase); ADDQ(Imm(32), dst); SUBQ(Imm(8), n); JMP(LabelRef("loop"))

	Label("tail")
	CMPQ(n, Imm(0)); JE(LabelRef("done"))

	vy1 := XMM(); VMOVSS(Mem{Base: yBase}, vy1)
	vx1 := XMM(); VMOVSS(Mem{Base: xBase}, vx1)

	xSign := XMM(); VMOVUPS(signMask, xSign)
	absY1 := XMM(); VANDNPS(vy1, xSign, absY1)
	absX1 := XMM(); VANDNPS(vx1, xSign, absX1)
	vMin1 := XMM(); VMINSS(absX1, absY1, vMin1)
	vMax1 := XMM(); VMAXSS(absX1, absY1, vMax1)
	vRatio1 := XMM(); VDIVSS(vMax1, vMin1, vRatio1)

	s1 := XMM(); VMULSS(vRatio1, vRatio1, s1)
	p1 := XMM(); VMOVSS(c4, p1)
	c3x := XMM(); VMOVSS(c3, c3x); VFMADD213SS(c3x, s1, p1)
	c2x := XMM(); VMOVSS(c2, c2x); VFMADD213SS(c2x, s1, p1)
	c1x := XMM(); VMOVSS(c1, c1x); VFMADD213SS(c1x, s1, p1)
	xs1 := XMM(); VMULSS(vRatio1, s1, xs1)
	VFMADD213SS(vRatio1, xs1, p1)

	cmpYX1 := XMM(); VCMPSS(Imm(0x0e), absX1, absY1, cmpYX1)
	piO2x := XMM(); VMOVSS(piOver2, piO2x)
	pSub1 := XMM(); VSUBSS(p1, piO2x, pSub1)
	VBLENDVPS(cmpYX1, pSub1, p1, p1)

	cmpX01 := XMM(); VXORPS(cmpX01, cmpX01, cmpX01); VCMPSS(Imm(0x01), cmpX01, vx1, cmpX01)
	pix := XMM(); VMOVSS(pi, pix)
	piSubP1 := XMM(); VSUBSS(p1, pix, piSubP1)
	VBLENDVPS(cmpX01, piSubP1, p1, p1)

	ySignBit1 := XMM(); VANDPS(vy1, xSign, ySignBit1)
	VXORPS(p1, ySignBit1, p1)

	VMOVSS(p1, Mem{Base: dst})

	ADDQ(Imm(4), yBase); ADDQ(Imm(4), xBase); ADDQ(Imm(4), dst); DECQ(n); JMP(LabelRef("tail"))

	Label("done")
	VZEROUPPER(); RET()
}
