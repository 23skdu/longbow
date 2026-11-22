//go:build amd64

package simd

// SinFloat32AVX2Kernel runs the sin float32 avx2 kernel operation.
//
//go:noescape
func SinFloat32AVX2Kernel(src, dst uintptr, n int)

// CosFloat32AVX2Kernel runs the cos float32 avx2 kernel operation.
//
//go:noescape
func CosFloat32AVX2Kernel(src, dst uintptr, n int)

// SincosFloat32AVX2Kernel runs the sincos float32 avx2 kernel operation.
//
//go:noescape
func SincosFloat32AVX2Kernel(src, sinDst, cosDst uintptr, n int)

// Atan2Float32AVX2Kernel runs the atan2 float32 avx2 kernel operation.
//
//go:noescape
func Atan2Float32AVX2Kernel(y, x, dst uintptr, n int)
