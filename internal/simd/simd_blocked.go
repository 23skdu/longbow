package simd

import (
	"errors"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

const (
	blockedSimdThreshold     = 1024
	blockedSimdThreshold256  = 256
	blockedSimdThreshold512  = 512
	blockedSimdThreshold1536 = 1536
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

// DotProductFloat32BlockedPrefetch calculates dot product using blocked processing
// with prefetch hints for very high dimensions (1536+). This improves cache utilization
// by prefetching the next block while computing the current block.
func DotProductFloat32BlockedPrefetch(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) < blockedSimdThreshold1536 {
		return DotProductFloat32Blocked(a, b)
	}

	var sum float32
	impl := dotProductImpl
	if impl == nil {
		impl = dotUnrolled4x
	}

	blockSize := blockedSimdThreshold512
	prefetchAhead := 1 // Prefetch 1 block ahead

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		// Prefetch next block while processing current
		nextIdx := i + (prefetchAhead * blockSize)
		if nextIdx < len(a) {
			Prefetch(unsafe.Pointer(&a[nextIdx]))
			Prefetch(unsafe.Pointer(&b[nextIdx]))
		}

		chunkA := a[i : i+blockSize]
		chunkB := b[i : i+blockSize]
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

// EuclideanFloat32BlockedPrefetch calculates Euclidean distance using blocked processing
// with prefetch hints for very high dimensions (1536+).
func EuclideanFloat32BlockedPrefetch(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) < blockedSimdThreshold1536 {
		return L2Float32Blocked(a, b)
	}

	var sum float32
	blockSize := blockedSimdThreshold512
	prefetchAhead := 1

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		// Prefetch next block
		nextIdx := i + (prefetchAhead * blockSize)
		if nextIdx < len(a) {
			Prefetch(unsafe.Pointer(&a[nextIdx]))
			Prefetch(unsafe.Pointer(&b[nextIdx]))
		}

		d, err := L2SquaredFloat32(a[i:i+blockSize], b[i:i+blockSize])
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

// =============================================================================
// Float64 Blocked Implementations
// =============================================================================

// DotProductFloat64Blocked calculates dot product for float64 vectors using blocked processing.
// Optimized for high dimensions (768+) by processing in cache-friendly blocks.
func DotProductFloat64Blocked(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return dotFloat64Unrolled4x(a, b)
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		chunkA := a[i : i+blockSize]
		chunkB := b[i : i+blockSize]
		d, err := dotFloat64Unrolled4x(chunkA, chunkB)
		if err != nil {
			return 0, err
		}
		sum += float64(d)
	}

	// Remainder
	if i < len(a) {
		d, err := dotFloat64Unrolled4x(a[i:], b[i:])
		if err != nil {
			return 0, err
		}
		sum += float64(d)
	}

	return float32(sum), nil
}

// EuclideanFloat64Blocked calculates Euclidean distance for float64 vectors using blocked processing.
func EuclideanFloat64Blocked(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return euclideanFloat64Unrolled4x(a, b)
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		chunkA := a[i : i+blockSize]
		chunkB := b[i : i+blockSize]
		d, err := euclideanFloat64Unrolled4x(chunkA, chunkB)
		if err != nil {
			return 0, err
		}
		sum += float64(d) * float64(d) // Square the distance, we'll sqrt at the end
	}

	// Remainder
	if i < len(a) {
		d, err := euclideanFloat64Unrolled4x(a[i:], b[i:])
		if err != nil {
			return 0, err
		}
		sum += float64(d) * float64(d)
	}

	return float32(math.Sqrt(sum)), nil
}

// =============================================================================
// Int32 Blocked Implementations
// =============================================================================

// DotProductInt32Blocked calculates dot product for int32 vectors using blocked processing.
func DotProductInt32Blocked(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return dotInt32Unrolled4x(a, b)
	}

	var sum int64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += int64(a[j]) * int64(b[j])
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		sum += int64(a[i]) * int64(b[i])
	}

	return float32(sum), nil
}

// EuclideanInt32Blocked calculates Euclidean distance for int32 vectors using blocked processing.
func EuclideanInt32Blocked(a, b []int32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return euclideanInt32Unrolled4x(a, b)
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// =============================================================================
// Int16 Blocked Implementations
// =============================================================================

// DotProductInt16Blocked calculates dot product for int16 vectors using blocked processing.
func DotProductInt16Blocked(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return dotInt16Unrolled4x(a, b)
	}

	var sum int64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += int64(a[j]) * int64(b[j])
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		sum += int64(a[i]) * int64(b[i])
	}

	return float32(sum), nil
}

// EuclideanInt16Blocked calculates Euclidean distance for int16 vectors using blocked processing.
func EuclideanInt16Blocked(a, b []int16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return euclideanInt16Unrolled4x(a, b)
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// =============================================================================
// Int8 Blocked Implementations
// =============================================================================

// DotProductInt8Blocked calculates dot product for int8 vectors using blocked processing.
func DotProductInt8Blocked(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return dotInt8Unrolled4x(a, b)
	}

	var sum int32
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += int32(a[j]) * int32(b[j])
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		sum += int32(a[i]) * int32(b[i])
	}

	return float32(sum), nil
}

// EuclideanInt8Blocked calculates Euclidean distance for int8 vectors using blocked processing.
func EuclideanInt8Blocked(a, b []int8) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) < blockedSimdThreshold {
		return euclideanInt8Unrolled4x(a, b)
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	// Remainder
	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// =============================================================================
// Uint16 Blocked Implementations
// =============================================================================

// EuclideanUint16Blocked calculates Euclidean distance for uint16 vectors using blocked processing.
func EuclideanUint16Blocked(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// DotProductUint16Blocked calculates dot product for uint16 vectors using blocked processing.
func DotProductUint16Blocked(a, b []uint16) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += float64(a[j]) * float64(b[j])
		}
	}

	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}

	return float32(sum), nil
}

// =============================================================================
// Uint32 Blocked Implementations
// =============================================================================

// EuclideanUint32Blocked calculates Euclidean distance for uint32 vectors using blocked processing.
func EuclideanUint32Blocked(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// DotProductUint32Blocked calculates dot product for uint32 vectors using blocked processing.
func DotProductUint32Blocked(a, b []uint32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += float64(a[j]) * float64(b[j])
		}
	}

	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}

	return float32(sum), nil
}

// =============================================================================
// Int64 Blocked Implementations
// =============================================================================

// EuclideanInt64Blocked calculates Euclidean distance for int64 vectors using blocked processing.
func EuclideanInt64Blocked(a, b []int64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// DotProductInt64Blocked calculates dot product for int64 vectors using blocked processing.
func DotProductInt64Blocked(a, b []int64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += float64(a[j]) * float64(b[j])
		}
	}

	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}

	return float32(sum), nil
}

// =============================================================================
// Uint64 Blocked Implementations
// =============================================================================

// EuclideanUint64Blocked calculates Euclidean distance for uint64 vectors using blocked processing.
func EuclideanUint64Blocked(a, b []uint64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold
	if len(a) >= 2048 {
		blockSize = blockedSimdThreshold512
	}

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			d := float64(a[j]) - float64(b[j])
			sum += d * d
		}
	}

	for ; i < len(a); i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}

	return float32(math.Sqrt(sum)), nil
}

// DotProductUint64Blocked calculates dot product for uint64 vectors using blocked processing.
func DotProductUint64Blocked(a, b []uint64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum float64
	blockSize := blockedSimdThreshold

	i := 0
	for ; i <= len(a)-blockSize; i += blockSize {
		for j := i; j < i+blockSize; j++ {
			sum += float64(a[j]) * float64(b[j])
		}
	}

	for ; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}

	return float32(sum), nil
}
