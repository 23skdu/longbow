//go:build amd64

package hnsw2

import "golang.org/x/sys/cpu"

// clearSIMD uses AVX2 instructions to zero the bitset if available.
func clearSIMD(bits []uint64) {
	if cpu.X86.HasAVX2 && len(bits) >= 4 {
		clearSIMDAVX2(bits)
	} else {
		// Fallback to scalar
		for i := range bits {
			bits[i] = 0
		}
	}
}

// andNotSIMD performs vectorized AND-NOT operation using AVX2.
func andNotSIMD(result, a, b []uint64) {
	if cpu.X86.HasAVX2 && len(a) >= 4 {
		andNotSIMDAVX2(result, a, b)
	} else {
		// Fallback to scalar
		for i := range result {
			result[i] = a[i] &^ b[i]
		}
	}
}

// popCountSIMD uses SIMD instructions to count set bits.
func popCountSIMD(bits []uint64) int {
	if cpu.X86.HasPOPCNT {
		return popCountPOPCNT(bits)
	}
	
	// Fallback to scalar
	count := 0
	for _, word := range bits {
		count += popCountScalar(word)
	}
	return count
}

// popCountScalar counts bits in a single uint64 using bit manipulation.
func popCountScalar(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

//go:noescape
func clearSIMDAVX2(bits []uint64)

//go:noescape
func andNotSIMDAVX2(result, a, b []uint64)

//go:noescape
func popCountPOPCNT(bits []uint64) int
