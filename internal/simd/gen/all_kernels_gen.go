//go:build ignore

package main

import (
	"flag"
	"fmt"
	"os"

	. "github.com/mmcloughlin/avo/build"

	"github.com/23skdu/longbow/internal/simd/gen/families"
)

func emitFloat32() {
	families.EmitFloat32MicroKernels()
	families.ImplementL2SquaredAVX2()
	families.ImplementDotAVX2()
	families.ImplementL2SquaredAVX512()
	families.ImplementDotAVX512()
	for _, dim := range []int{128, 384, 768, 1024, 3072} {
		families.ImplementSpecializedAVX2(dim)
	}
	for _, dim := range []int{128, 384, 768, 1024, 3072} {
		families.ImplementSpecializedAVX512(dim)
	}
}

func emitVertical() {
	families.ImplementEuclideanVertical4AVX2()
	families.ImplementDotVertical4AVX2()
	families.ImplementEuclideanVertical4AVX512()
	families.ImplementDotVertical4AVX512()
	families.EmitCosine8AVX2()
}

func emitFloat64() {
	families.ImplementEuclideanFloat64AVX2()
	families.ImplementDotFloat64AVX2()
	families.ImplementL2SquaredFloat64AVX2()
	families.ImplementCosineFloat64AVX2()
	for _, dim := range []int{128, 384, 768} {
		families.ImplementSpecializedFloat64AVX2(dim)
	}
	families.ImplementEuclideanFloat64AVX512()
	families.ImplementDotFloat64AVX512()
	families.ImplementL2SquaredFloat64AVX512()
	families.ImplementCosineFloat64AVX512()
}

func emitF16() {
	families.ImplementEuclideanF16AVX2()
	families.ImplementDotF16AVX2()
	families.ImplementEuclideanF16AVX512()
	families.ImplementDotF16AVX512()
}

func emitReductions() {
	families.EmitReductionKernels()
	families.EmitMatMulAVX2()
}

func main() {
	family := flag.String("family", "all", "kernel family to generate (all, float32, vertical, float64, f16, reductions)")
	flag.Parse()

	switch *family {
	case "all":
		emitFloat32()
		emitVertical()
		emitFloat64()
		emitF16()
		emitReductions()
	case "float32":
		emitFloat32()
	case "vertical":
		emitVertical()
	case "float64":
		emitFloat64()
	case "f16":
		emitF16()
	case "reductions":
		emitReductions()
	default:
		fmt.Fprintf(os.Stderr, "unknown family %q\n", *family)
		os.Exit(2)
	}

	Generate()
}
