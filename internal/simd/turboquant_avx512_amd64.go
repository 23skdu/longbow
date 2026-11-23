//go:build amd64

package simd

import "unsafe" // #nosec G103

// AVX-512 specialized implementations

// UnpackTQ2AVX512 stubs
func UnpackTQ2AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ2AVX2(src, dst, scale, bias)
}

// UnpackTQ4AVX512 runs the unpack tq4 avx512 operation.
func UnpackTQ4AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ4AVX2(src, dst, scale, bias)
}

// UnpackTQ8AVX512 runs the unpack tq8 avx512 operation.
func UnpackTQ8AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ8AVX2(src, dst, scale, bias)
}

// PackTQ2AVX512 runs the pack tq2 avx512 operation.
func PackTQ2AVX512(src []float32, dst []byte) {
	PackTQ2Generic(src, dst)
}

// PackTQ4AVX512 runs the pack tq4 avx512 operation.
func PackTQ4AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 {
		PackTQ4AVX2(src, dst)
		return
	}
	packTQ4AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// PackTQ8AVX512 runs the pack tq8 avx512 operation.
func PackTQ8AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 {
		PackTQ8AVX2(src, dst)
		return
	}
	packTQ8AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// UnpackTQ2AVX512VBMI decodes a 2-bit TurboQuant payload.
func UnpackTQ2AVX512VBMI(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	UnpackTQ2(src, dst, scale, bias)
}

// PackTQ2AVX512VBMI packs via VPMULTISHIFTQB on the same 2-bit midpoint grid
// as PackTQ2Generic: q = floor(clamp((v + 3*PI/4) * 2/PI, 0, 3) + 0.5),
// four codes per output byte in element order. TestPackTQ2AVX512VBMIMatchesGeneric
// checks bit-identity against PackTQ2Generic, including the grid boundaries.
func PackTQ2AVX512VBMI(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	if !features.HasAVX512 || !features.HasVBMI {
		PackTQ2(src, dst)
		return
	}
	packTQ2AVX512VBMIKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// Assembly kernel stubs

//go:noescape
func unpackTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int, scale, bias float32) // #nosec G103

//go:noescape
func packTQ2AVX512VBMIKernel(src, dst unsafe.Pointer, n int) // #nosec G103

var (
	_ = unpackTQ2AVX512VBMIKernel
	_ = packTQ2AVX512VBMIKernel
)

//go:noescape
//nolint:unused
func packTQ2AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103

//go:noescape
func packTQ4AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103

//go:noescape
func packTQ8AVX512Kernel(src, dst unsafe.Pointer, n int) // #nosec G103
