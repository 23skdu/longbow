package simd

import (
	"errors"
	"math"

	"github.com/23skdu/longbow/internal/metrics"
)

const (
	blockedSimdThreshold     = 1024
	blockedSimdThreshold256  = 256
	blockedSimdThreshold512  = 512
	blockedSimdThreshold768  = 768
	blockedSimdThreshold1536 = 1536
	blockedSimdThreshold2048 = 2048
	blockedSimdThreshold3072 = 3072
)

// DotProductFloat32Blocked calculates dot product using blocked loop processing
// optimized for vectors larger than L1 cache lines or for specific instruction pipeelining.
// It iterates in chunks to ensure data fits in L1 cache and to potentially allow
// better prefetching efficiency.
func DotProductFloat32Blocked(a, b []float32) (float32, error) {
	if len(a) < blockedSimdThreshold {
		return currentDispatch.DotProduct(a, b)
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
	if len(a) < blockedSimdThreshold {
		return l2SquaredImpl(a, b)
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

func euclidean384Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold256)
}

func euclidean768Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold256)
}

func euclidean1024Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold256)
}

func euclidean1536Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold256)
}

func euclidean2048Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold512)
}

func euclidean3072Blocked(a, b []float32) (float32, error) {
	return euclideanBlockedGeneric(a, b, blockedSimdThreshold512)
}

func euclideanBlockedGeneric(a, b []float32, blockSize int) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) <= blockSize {
		sum, err := L2SquaredFloat32(a, b)
		if err != nil {
			return 0, err
		}
		return float32(math.Sqrt(float64(sum))), nil
	}

	var totalSum float32
	dim := len(a)

	for i := 0; i < dim; i += blockSize {
		end := i + blockSize
		if end > dim {
			end = dim
		}
		chunkA := a[i:end]
		chunkB := b[i:end]
		chunkSum, err := L2SquaredFloat32(chunkA, chunkB)
		if err != nil {
			return 0, err
		}
		totalSum += chunkSum
	}

	return float32(math.Sqrt(float64(totalSum))), nil
}

func euclideanBlocked(a, b []float32) (float32, error) {
	dim := len(a)
	if dim >= 2048 {
		return euclideanBlockedGeneric(a, b, blockedSimdThreshold512)
	}
	if dim >= 1024 {
		return euclideanBlockedGeneric(a, b, blockedSimdThreshold256)
	}
	if len(a) == 0 {
		return 0, nil
	}
	sum, err := L2SquaredFloat32(a, b)
	if err != nil {
		return 0, err
	}
	return float32(math.Sqrt(float64(sum))), nil
}
