//go:build amd64

package simd

//go:noescape
func SinFloat32AVX2Kernel(src, dst uintptr, n int)

//go:noescape
func CosFloat32AVX2Kernel(src, dst uintptr, n int)

//go:noescape
func SincosFloat32AVX2Kernel(src, sinDst, cosDst uintptr, n int)

//go:noescape
func Atan2Float32AVX2Kernel(y, x, dst uintptr, n int)
