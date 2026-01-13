package simd

import "math"

const blockedSimdThreshold = 1024

// DotProductFloat32Blocked calculates dot product using blocked loop processing
// optimized for vectors larger than L1 cache lines or for specific instruction pipeelining.
// It iterates in chunks to ensure data fits in L1 cache and to potentially allow
// better prefetching efficiency.
func DotProductFloat32Blocked(a, b []float32) float32 {
	if len(a) <= blockedSimdThreshold {
		return DotProduct(a, b)
	}

	var sum float32
	// Use the internal implementation directly to avoid dispatch overhead inside loop
	impl := dotProductImpl
	if impl == nil {
		impl = dotUnrolled4x // Fallback logic if init issues, though init() should run
	}

	i := 0
	for ; i <= len(a)-blockedSimdThreshold; i += blockedSimdThreshold {
		chunkA := a[i : i+blockedSimdThreshold]
		chunkB := b[i : i+blockedSimdThreshold]
		sum += impl(chunkA, chunkB)
	}

	// Remainder
	if i < len(a) {
		sum += impl(a[i:], b[i:])
	}

	return sum
}

// L2Float32Blocked calculates Euclidean distance using blocked loop processing.
func L2Float32Blocked(a, b []float32) float32 {
	if len(a) <= blockedSimdThreshold {
		return EuclideanDistance(a, b)
	}

	var sum float32
	i := 0
	for ; i <= len(a)-blockedSimdThreshold; i += blockedSimdThreshold {
		sum += L2SquaredFloat32(a[i:i+blockedSimdThreshold], b[i:i+blockedSimdThreshold])
	}

	// Remainder
	if i < len(a) {
		sum += L2SquaredFloat32(a[i:], b[i:])
	}

	return float32(math.Sqrt(float64(sum)))
}
