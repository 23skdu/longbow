package families

import (
	"sync"
	"testing"
)

var (
	emitMu   sync.Mutex
	seenDims = map[int]bool{}
)

// TestFamiliesEmitAll verifies that all SIMD Avo kernel family emitters construct
// their instruction IR cleanly without panicking.
func TestFamiliesEmitAll(t *testing.T) {
	emitMu.Lock()
	defer emitMu.Unlock()

	EmitFloat32MicroKernels()
	ImplementL2SquaredAVX2()
	ImplementDotAVX2()
	ImplementL2SquaredAVX512()
	ImplementDotAVX512()

	ImplementEuclideanVertical4AVX2()
	ImplementDotVertical4AVX2()
	ImplementEuclideanVertical4AVX512()
	ImplementDotVertical4AVX512()
	EmitCosine8AVX2()

	ImplementEuclideanFloat64AVX2()
	ImplementDotFloat64AVX2()
	ImplementL2SquaredFloat64AVX2()
	ImplementCosineFloat64AVX2()
	ImplementEuclideanFloat64AVX512()
	ImplementDotFloat64AVX512()
	ImplementL2SquaredFloat64AVX512()
	ImplementCosineFloat64AVX512()

	ImplementEuclideanF16AVX2()
	ImplementDotF16AVX2()
	ImplementEuclideanF16AVX512()
	ImplementDotF16AVX512()

	EmitReductionKernels()
	EmitMatMulAVX2()

	for _, dim := range []int{128, 384, 768} {
		if !seenDims[dim] {
			seenDims[dim] = true
			ImplementSpecializedAVX2(dim)
			ImplementSpecializedAVX512(dim)
			ImplementSpecializedFloat64AVX2(dim)
		}
	}
}

// FuzzImplementSpecializedDim property-tests dimension-specialized Avo emitters
// across valid SIMD vector multiples of 16 without duplicate symbol registration.
func FuzzImplementSpecializedDim(f *testing.F) {
	for _, dim := range []int{16, 64, 128, 256, 384, 512, 768} {
		f.Add(dim)
	}

	f.Fuzz(func(t *testing.T, dim int) {
		if dim < 16 || dim > 2048 || dim%16 != 0 {
			t.Skip("dimension must be a multiple of 16 in [16, 2048]")
		}

		emitMu.Lock()
		defer emitMu.Unlock()
		if seenDims[dim] {
			return
		}
		seenDims[dim] = true

		ImplementSpecializedAVX2(dim)
		ImplementSpecializedAVX512(dim)
		ImplementSpecializedFloat64AVX2(dim)
	})
}
