//go:build !amd64 && !arm64

package hnsw2

// clearSIMD falls back to scalar implementation on unsupported platforms.
func clearSIMD(bits []uint64) {
	for i := range bits {
		bits[i] = 0
	}
}

// andNotSIMD falls back to scalar implementation.
func andNotSIMD(result, a, b []uint64) {
	for i := range result {
		result[i] = a[i] &^ b[i]
	}
}

// popCountSIMD falls back to scalar implementation.
func popCountSIMD(bits []uint64) int {
	count := 0
	for _, word := range bits {
		count += popCountScalar(word)
	}
	return count
}

// popCountScalar counts bits in a single uint64.
func popCountScalar(x uint64) int {
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}
