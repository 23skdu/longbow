//go:build amd64 && avx512

package simd

import "unsafe"

// AVX-512 specialized implementations

// AVX-512 stubs
func UnpackTQ2AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ2AVX2(src, dst, scale, bias)
}
func UnpackTQ4AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ4AVX2(src, dst, scale, bias)
}
func UnpackTQ8AVX512(src []byte, dst []float32, scale, bias float32) {
	UnpackTQ8AVX2(src, dst, scale, bias)
}
func PackTQ2AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ2AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

func PackTQ4AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ4AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

func PackTQ8AVX512(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ8AVX512Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

// AVX-512 VBMI specialized
func UnpackTQ2AVX512VBMI(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ2AVX512VBMIKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias)
}

func PackTQ2AVX512VBMI(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ2AVX512VBMIKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

