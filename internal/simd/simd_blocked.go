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
func DotProductFloat32Blocked(a, b []float32) (float32, error) {
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
		d, err := impl(chunkA, chunkB)
		if err != nil {
			return 0, err
		}
		sum += d
	}

	// Remainder
	if i < len(a) {
		d, err := impl(a[i:], b[i:])
		if err != nil {
			return 0, err
		}
		sum += d
	}

	return sum, nil
}

// L2Float32Blocked calculates Euclidean distance using blocked loop processing.
func L2Float32Blocked(a, b []float32) (float32, error) {
	if len(a) <= blockedSimdThreshold {
		return EuclideanDistance(a, b)
	}

	var sum float32
	i := 0
	for ; i <= len(a)-blockedSimdThreshold; i += blockedSimdThreshold {
		d, err := L2SquaredFloat32(a[i:i+blockedSimdThreshold], b[i:i+blockedSimdThreshold])
		if err != nil {
			return 0, err
		}
		sum += d
	}

	// Remainder
	if i < len(a) {
		d, err := L2SquaredFloat32(a[i:], b[i:])
		if err != nil {
			return 0, err
		}
		sum += d
	}

	return float32(math.Sqrt(float64(sum))), nil
}

// EuclideanDistanceTiledBatch calculates distances for multiple vectors by tiling the dimension loop.
// This keeps chunks of the query vector in L1/L2 cache while processing multiple data vectors.
func EuclideanDistanceTiledBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(query) <= blockedSimdThreshold {
		return EuclideanDistanceBatch(query, vectors, results)
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
			d, err := L2SquaredFloat32(qTile, vTile)
			if err != nil {
				return err
			}
			results[j] += d
		}
	}

	// Final Sqrt pass
	for i := range results {
		results[i] = float32(math.Sqrt(float64(results[i])))
	}
	return nil
}

// DotProductTiledBatch calculates dot products for multiple vectors by tiling the dimension loop.
func DotProductTiledBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(query) <= blockedSimdThreshold {
		return DotProductBatch(query, vectors, results)
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
			d, err := impl(qTile, vectors[j][i:end])
			if err != nil {
				return err
			}
			results[j] += d
		}
	}
	return nil
}
