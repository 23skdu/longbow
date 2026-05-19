//go:build arm64

package simd

import (
	"unsafe"
)

func init() {
	// NEON is mandatory on ARM64
	hammingImpl = hammingNEON
	andBitVectorsImpl = andBitVectorsNEON
	countBitVectorImpl = countBitVectorNEON
}

//go:noescape
func hammingNEONKernel(a, b unsafe.Pointer, n int) int

//go:noescape
func andBitVectorNEON(dst, src unsafe.Pointer, n int)

//go:noescape
func countBitVectorNEONKernel(src unsafe.Pointer, n int) int

func hammingNEON(a, b []uint64) int {
	if len(a) == 0 {
		return 0
	}
	return hammingNEONKernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a)) // #nosec G103
}

// andBitVectorsNEON applies bitwise AND: dst &= src
func andBitVectorsNEON(dst, src []uint64) {
	if len(dst) == 0 || len(src) == 0 {
		return
	}
	n := len(dst)
	if len(src) < n {
		n = len(src)
	}
	andBitVectorNEON(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), n) // #nosec G103
}

// countBitVectorNEON returns the number of set bits in the vector
func countBitVectorNEON(src []uint64) int {
	if len(src) == 0 {
		return 0
	}
	return countBitVectorNEONKernel(unsafe.Pointer(&src[0]), len(src)) // #nosec G103
}
