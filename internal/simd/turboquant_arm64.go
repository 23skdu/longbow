//go:build arm64

package simd

import (
	"unsafe"
)

// PackTQ2NEON falls back to the generic implementation, which uses the
// updated 2-bit angle grid (midpoints) shared with the rest of the codec.
func PackTQ2NEON(src []float32, dst []byte) {
	PackTQ2Generic(src, dst)
}

// PackTQ4NEON is implemented in assembly.
func PackTQ4NEON(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ4NEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// PackTQ8NEON is implemented in assembly.
func PackTQ8NEON(src []float32, dst []byte) {
	if len(src) == 0 {
		return
	}
	packTQ8NEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(src)) // #nosec G103
}

// UnpackTQ2NEON is implemented in assembly.
func UnpackTQ2NEON(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ2NEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias) // #nosec G103
}

// UnpackTQ4NEON is implemented in assembly.
func UnpackTQ4NEON(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ4NEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias) // #nosec G103
}

// UnpackTQ8NEON is implemented in assembly.
func UnpackTQ8NEON(src []byte, dst []float32, scale, bias float32) {
	if len(dst) == 0 {
		return
	}
	unpackTQ8NEONKernel(unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0]), len(dst), scale, bias) // #nosec G103
}

//go:noescape
func packTQ2NEONKernel(src, dst unsafe.Pointer, n int)

//go:noescape
func packTQ4NEONKernel(src, dst unsafe.Pointer, n int)

//go:noescape
func packTQ8NEONKernel(src, dst unsafe.Pointer, n int)

//go:noescape
func unpackTQ2NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)

//go:noescape
func unpackTQ4NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)

//go:noescape
func unpackTQ8NEONKernel(src, dst unsafe.Pointer, n int, scale, bias float32)
