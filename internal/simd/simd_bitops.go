package simd

// andBytesGeneric performs bitwise AND: dst[i] &= src[i]
// Implementation uses 8x loop unrolling to allow compiler to optimize
// (possibly vectorizing on supported arches) and reduce loop overhead.
func andBytesGeneric(dst, src []byte) {
	i := 0
	n := len(dst)

	// Unroll 8 bytes at a time
	for ; i <= n-8; i += 8 {
		dst[i] &= src[i]
		dst[i+1] &= src[i+1]
		dst[i+2] &= src[i+2]
		dst[i+3] &= src[i+3]
		dst[i+4] &= src[i+4]
		dst[i+5] &= src[i+5]
		dst[i+6] &= src[i+6]
		dst[i+7] &= src[i+7]
	}

	// Handle remaining bytes
	for ; i < n; i++ {
		dst[i] &= src[i]
	}
}

// Popcount returns the population count (number of set bits) of x.
// This usually maps to the POPCNT instruction on x86-64 (if supported and enabled)
// or an efficient software fallback.
func Popcount(x uint64) int {
	// Implementation note: Go's math/bits.OnesCount64 is intrinsic-optimized.
	// We duplicate the logic here or rely on the caller to use math/bits?
	// To keep this package as the SIMD abstraction layer, we can import math/bits.
	// But we can't import math/bits if we want to stay dependency-free?
	// Standard library imports are fine.
	return onesCount64(x)
}

// HammingDistance computes the Hamming distance between two packed bit vectors.
// a and b must have the same length.
func HammingDistance(a, b []uint64) int {
	dist := 0
	for i := 0; i < len(a); i++ {
		dist += onesCount64(a[i] ^ b[i])
	}
	return dist
}

// fallback implementation if math/bits is not used directly to avoid import cycles
// (though math/bits is StdLib so it's fine).
// Copied from math/bits to avoid import if we want to be strict, but importing is better.
const m0 = 0x5555555555555555 // 01010101 ...
const m1 = 0x3333333333333333 // 00110011 ...
const m2 = 0x0f0f0f0f0f0f0f0f // 00001111 ...

func onesCount64(x uint64) int {
	const m0 = 0x5555555555555555 // 01010101 ...
	const m1 = 0x3333333333333333 // 00110011 ...
	const m2 = 0x0f0f0f0f0f0f0f0f // 00001111 ...
	const m3 = 0x00ff00ff00ff00ff // 00000000 11111111 ...
	const m4 = 0x0000ffff0000ffff // ...

	// Implementation: Parallel summing
	x -= (x >> 1) & m0
	x = (x & m1) + ((x >> 2) & m1)
	x = (x + (x >> 4)) & m2
	x += x >> 8
	x += x >> 16
	x += x >> 32
	return int(x & 0x7f)
}
