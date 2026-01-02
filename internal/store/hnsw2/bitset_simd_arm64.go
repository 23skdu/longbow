//go:build arm64

package hnsw2

// clearSIMD uses optimized Go implementation for ARM64.
// TODO: Add NEON assembly when Go assembler syntax is clarified.
func clearSIMD(bits []uint64) {
	// Compiler will optimize this loop well on ARM64
	for i := range bits {
		bits[i] = 0
	}
}

// andNotSIMD performs AND-NOT using optimized Go.
func andNotSIMD(result, a, b []uint64) {
	// Process in chunks for better cache utilization
	for i := range result {
		result[i] = a[i] &^ b[i]
	}
}

// popCountSIMD uses optimized bit counting.
func popCountSIMD(bits []uint64) int {
	count := 0
	for _, word := range bits {
		// Use bits.OnesCount64 which may use POPCNT instruction
		count += popCountWord(word)
	}
	return count
}

// popCountWord counts bits in a single uint64 using efficient algorithm.
func popCountWord(x uint64) int {
	// Parallel bit counting (SWAR algorithm)
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((x * 0x0101010101010101) >> 56)
}
