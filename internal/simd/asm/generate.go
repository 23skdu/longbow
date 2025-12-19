//go:build ignore

package main

import (
. "github.com/mmcloughlin/avo/build"
. "github.com/mmcloughlin/avo/operand"
. "github.com/mmcloughlin/avo/reg"
)

func main() {
Package("github.com/23skdu/longbow/internal/simd")

generateDot32FMA()
generateDot64FMA()
generateEuclidean32FMA()
generateEuclidean64FMA()
generateCosine32FMA()

Generate()
}

func generateDot32FMA() {
TEXT("dot32FMA", NOSPLIT, "func(a, b uintptr) float32")
Doc("dot32FMA computes dot product of 32 float32s using AVX-512 FMA")

a := GP64()
b := GP64()
Load(Param("a"), a)
Load(Param("b"), b)

z0 := ZMM()
z1 := ZMM()
z2 := ZMM()
z3 := ZMM()
acc := ZMM()

VXORPS(acc, acc, acc)

VMOVUPS(Mem{Base: a}, z0)
VMOVUPS(Mem{Base: b}, z1)
VFMADD231PS(z0, z1, acc)

VMOVUPS(Mem{Base: a, Disp: 64}, z2)
VMOVUPS(Mem{Base: b, Disp: 64}, z3)
VFMADD231PS(z2, z3, acc)

horizontalSum512ToXMM(acc)

Store(acc.AsX(), ReturnIndex(0))
VZEROUPPER()
RET()
}

func generateDot64FMA() {
TEXT("dot64FMA", NOSPLIT, "func(a, b uintptr) float32")
Doc("dot64FMA computes dot product of 64 float32s using AVX-512 FMA")

a := GP64()
b := GP64()
Load(Param("a"), a)
Load(Param("b"), b)

acc0 := ZMM()
acc1 := ZMM()
acc2 := ZMM()
acc3 := ZMM()

VXORPS(acc0, acc0, acc0)
VXORPS(acc1, acc1, acc1)
VXORPS(acc2, acc2, acc2)
VXORPS(acc3, acc3, acc3)

tmpA := ZMM()
tmpB := ZMM()

VMOVUPS(Mem{Base: a, Disp: 0}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 0}, tmpB)
VFMADD231PS(tmpA, tmpB, acc0)

VMOVUPS(Mem{Base: a, Disp: 64}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 64}, tmpB)
VFMADD231PS(tmpA, tmpB, acc1)

VMOVUPS(Mem{Base: a, Disp: 128}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 128}, tmpB)
VFMADD231PS(tmpA, tmpB, acc2)

VMOVUPS(Mem{Base: a, Disp: 192}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 192}, tmpB)
VFMADD231PS(tmpA, tmpB, acc3)

VADDPS(acc1, acc0, acc0)
VADDPS(acc3, acc2, acc2)
VADDPS(acc2, acc0, acc0)

horizontalSum512ToXMM(acc0)

Store(acc0.AsX(), ReturnIndex(0))
VZEROUPPER()
RET()
}

func generateEuclidean32FMA() {
TEXT("euclidean32FMA", NOSPLIT, "func(a, b uintptr) float32")
Doc("euclidean32FMA computes sum of squared differences for 32 float32s")

a := GP64()
b := GP64()
Load(Param("a"), a)
Load(Param("b"), b)

acc := ZMM()
VXORPS(acc, acc, acc)

z0 := ZMM()
z1 := ZMM()
z2 := ZMM()
z3 := ZMM()
diff0 := ZMM()
diff1 := ZMM()

VMOVUPS(Mem{Base: a}, z0)
VMOVUPS(Mem{Base: b}, z1)
VSUBPS(z1, z0, diff0)
VFMADD231PS(diff0, diff0, acc)

VMOVUPS(Mem{Base: a, Disp: 64}, z2)
VMOVUPS(Mem{Base: b, Disp: 64}, z3)
VSUBPS(z3, z2, diff1)
VFMADD231PS(diff1, diff1, acc)

horizontalSum512ToXMM(acc)

Store(acc.AsX(), ReturnIndex(0))
VZEROUPPER()
RET()
}

func generateEuclidean64FMA() {
TEXT("euclidean64FMA", NOSPLIT, "func(a, b uintptr) float32")
Doc("euclidean64FMA computes sum of squared differences for 64 float32s")

a := GP64()
b := GP64()
Load(Param("a"), a)
Load(Param("b"), b)

acc0 := ZMM()
acc1 := ZMM()
VXORPS(acc0, acc0, acc0)
VXORPS(acc1, acc1, acc1)

tmpA := ZMM()
tmpB := ZMM()
diff := ZMM()

VMOVUPS(Mem{Base: a, Disp: 0}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 0}, tmpB)
VSUBPS(tmpB, tmpA, diff)
VFMADD231PS(diff, diff, acc0)

VMOVUPS(Mem{Base: a, Disp: 64}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 64}, tmpB)
VSUBPS(tmpB, tmpA, diff)
VFMADD231PS(diff, diff, acc1)

VMOVUPS(Mem{Base: a, Disp: 128}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 128}, tmpB)
VSUBPS(tmpB, tmpA, diff)
VFMADD231PS(diff, diff, acc0)

VMOVUPS(Mem{Base: a, Disp: 192}, tmpA)
VMOVUPS(Mem{Base: b, Disp: 192}, tmpB)
VSUBPS(tmpB, tmpA, diff)
VFMADD231PS(diff, diff, acc1)

VADDPS(acc1, acc0, acc0)

horizontalSum512ToXMM(acc0)

Store(acc0.AsX(), ReturnIndex(0))
VZEROUPPER()
RET()
}

func generateCosine32FMA() {
TEXT("cosine32FMA", NOSPLIT, "func(a, b uintptr) (dot, normA, normB float32)")
Doc("cosine32FMA computes dot product and squared norms for 32 float32s")

a := GP64()
b := GP64()
Load(Param("a"), a)
Load(Param("b"), b)

accDot := ZMM()
accNormA := ZMM()
accNormB := ZMM()
VXORPS(accDot, accDot, accDot)
VXORPS(accNormA, accNormA, accNormA)
VXORPS(accNormB, accNormB, accNormB)

z0 := ZMM()
z1 := ZMM()
z2 := ZMM()
z3 := ZMM()

VMOVUPS(Mem{Base: a}, z0)
VMOVUPS(Mem{Base: b}, z1)
VFMADD231PS(z0, z1, accDot)
VFMADD231PS(z0, z0, accNormA)
VFMADD231PS(z1, z1, accNormB)

VMOVUPS(Mem{Base: a, Disp: 64}, z2)
VMOVUPS(Mem{Base: b, Disp: 64}, z3)
VFMADD231PS(z2, z3, accDot)
VFMADD231PS(z2, z2, accNormA)
VFMADD231PS(z3, z3, accNormB)

horizontalSum512ToXMM(accDot)
horizontalSum512ToXMM(accNormA)
horizontalSum512ToXMM(accNormB)

Store(accDot.AsX(), Return("dot"))
Store(accNormA.AsX(), Return("normA"))
Store(accNormB.AsX(), Return("normB"))
VZEROUPPER()
RET()
}

func horizontalSum512ToXMM(z VecVirtual) {
y := YMM()
x := XMM()
tmp := XMM()

VEXTRACTF32X8(U8(1), z, y)
VADDPS(y, z.AsY(), y)

VEXTRACTF128(U8(1), y, x)
VADDPS(x, y.AsX(), x)

VMOVHLPS(x, x, tmp)
VADDPS(tmp, x, x)
VMOVSHDUP(x, tmp)
VADDSS(tmp, x, z.AsX())
}
