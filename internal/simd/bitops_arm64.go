//go:build arm64

package simd

import (
	"unsafe"
)

func init() {
	// NEON is mandatory on ARM64
	hammingImpl = hammingNEON
}

//go:noescape
func hammingNEONKernel(a, b unsafe.Pointer, n int) int

func hammingNEON(a, b []uint64) int {
	if len(a) == 0 {
		return 0
	}
	return hammingNEONKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}
