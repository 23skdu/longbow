package simd

import (
	"math"

	"github.com/23skdu/longbow/internal/metrics"
)

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

// EuclideanDistanceTiledBatch calculates distances for multiple vectors by tiling the dimension loop.
// This keeps chunks of the query vector in L1/L2 cache while processing multiple data vectors.
func EuclideanDistanceTiledBatch(query []float32, vectors [][]float32, results []float32) {
	if len(query) <= blockedSimdThreshold {
		EuclideanDistanceBatch(query, vectors, results)
		return
	}

	metrics.SimdTiledDistanceBatchTotal.Inc()

	// Initialize results to zero
	for i := range results {
		results[i] = 0
	}

	numVecs := len(vectors)
	dims := len(query)

	// Outer loop over dimension tiles
	for i := 0; i < dims; i += blockedSimdThreshold {
		end := i + blockedSimdThreshold
		if end > dims {
			end = dims
		}
		qTile := query[i:end]

		// Inner loop over vectors
		for j := 0; j < numVecs; j++ {
			vTile := vectors[j][i:end]
			results[j] += L2SquaredFloat32(qTile, vTile)
		}
	}

	// Final Sqrt pass
	for i := range results {
		results[i] = float32(math.Sqrt(float64(results[i])))
	}
}

// DotProductTiledBatch calculates dot products for multiple vectors by tiling the dimension loop.
func DotProductTiledBatch(query []float32, vectors [][]float32, results []float32) {
	if len(query) <= blockedSimdThreshold {
		DotProductBatch(query, vectors, results)
		return
	}

	// Initialize results to zero
	for i := range results {
		results[i] = 0
	}

	numVecs := len(vectors)
	dims := len(query)
	impl := dotProductImpl

	// Outer loop over dimension tiles
	for i := 0; i < dims; i += blockedSimdThreshold {
		end := i + blockedSimdThreshold
		if end > dims {
			end = dims
		}
		qTile := query[i:end]

		// Inner loop over vectors
		for j := 0; j < numVecs; j++ {
			results[j] += impl(qTile, vectors[j][i:end])
		}
	}
}
