//go:build amd64

package simd

import "unsafe"

// sqrtFloat32AVX2Kernel computes the square root of each element in src
// and stores the result in dst. Uses VSQRTPS for 4-wide SIMD.
func sqrtFloat32AVX2Kernel(src, dst unsafe.Pointer, n int)
