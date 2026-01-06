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
