//go:build amd64
package simd

import "unsafe"

// UnpackTQ2AVX2 is implemented in assembly.
func UnpackTQ2AVX2(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ2AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias)
}

// UnpackTQ4AVX2 is implemented in assembly.
func UnpackTQ4AVX2(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ4AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias)
}

// UnpackTQ8AVX2 is implemented in assembly.
func UnpackTQ8AVX2(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ8AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias)
}

// PackTQ2AVX2 is implemented in assembly.
func PackTQ2AVX2(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ2AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

// PackTQ4AVX2 is implemented in assembly.
func PackTQ4AVX2(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ4AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

// PackTQ8AVX2 is implemented in assembly.
func PackTQ8AVX2(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ8AVX2Kernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src))
}

// AVX-512 VBMI specialized (handled in turboquant_avx512_amd64.go)

// Assembly kernel stubs
//go:noescape
func unpackTQ2AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
//go:noescape
func unpackTQ4AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
//go:noescape
func unpackTQ8AVX2Kernel(src, dst unsafe.Pointer, n int, scale, bias float32)
//go:noescape
func packTQ2AVX2Kernel(src, dst unsafe.Pointer, n int)
//go:noescape
func packTQ4AVX2Kernel(src, dst unsafe.Pointer, n int)
//go:noescape
func packTQ8AVX2Kernel(src, dst unsafe.Pointer, n int)
