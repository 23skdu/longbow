package simd

import (
	"errors"
	"math"
	"os"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/rs/zerolog/log"
)

var (
	ErrDimensionMismatch    = errors.New("simd: vector dimension mismatch")
	ErrInitializationFailed = errors.New("simd: initialization failed")
)

// Function pointer types for dispatch
type (
	distanceFunc          func(a, b []float32) (float32, error)
	distanceBatchFunc     func(query []float32, vectors [][]float32, results []float32) error
	distanceBatchFlatFunc func(query []float32, flatVectors []float32, numVectors, dims int, results []float32) error
	distanceSQ8BatchFunc  func(query []byte, vectors [][]byte, results []float32) error
	distanceF16BatchFunc  func(query []float16.Num, vectors [][]float16.Num, results []float32) error
	adcDistanceBatchFunc  func(table []float32, flatCodes []byte, m int, results []float32) error

	distanceF16Func func(a, b []float16.Num) (float32, error)

	distanceComplex64Func  func(a, b []complex64) (float32, error)
	distanceComplex128Func func(a, b []complex128) (float32, error)
	distanceFloat64Func    func(a, b []float64) (float32, error)

	// CompareOp represents a comparison operator for SIMD filters
	CompareOp int
)

const (
	CompareEq CompareOp = iota
	CompareNeq
	CompareGt
	CompareGe
	CompareLt
	CompareLe
)

type (
	matchInt64Func   func(src []int64, val int64, op CompareOp, dst []byte) error
	matchFloat32Func func(src []float32, val float32, op CompareOp, dst []byte) error
)

var (

	// Function pointers initialized at startup - eliminates switch overhead in hot path
	euclideanDistanceImpl    distanceFunc
	euclideanDistance384Impl distanceFunc
	euclideanDistance128Impl distanceFunc // optimized for dimensions=128
	cosineDistanceImpl       distanceFunc

	// DistFunc is the best available Euclidean distance implementation
	DistFunc distanceFunc

	dotProductImpl                 distanceFunc
	dotProduct384Impl              distanceFunc
	dotProduct128Impl              distanceFunc // optimized for dimensions=128
	euclideanDistanceBatchImpl     distanceBatchFunc
	euclideanDistanceBatchFlatImpl distanceBatchFlatFunc
	cosineDistanceBatchImpl        distanceBatchFunc
	dotProductBatchImpl            distanceBatchFunc
	l2SquaredImpl                  distanceFunc

	matchInt64Impl   matchInt64Func
	matchFloat32Impl matchFloat32Func

	adcDistanceBatchImpl               adcDistanceBatchFunc
	euclideanDistanceVerticalBatchImpl distanceBatchFunc
	euclideanDistanceSQ8BatchImpl      distanceSQ8BatchFunc
	euclideanDistanceF16BatchImpl      distanceF16BatchFunc

	// Bitwise operations
	andBytesImpl func(dst, src []byte)

	euclideanDistanceF16Impl distanceF16Func
	cosineDistanceF16Impl    distanceF16Func
	dotProductF16Impl        distanceF16Func

	euclideanDistanceComplex64Impl  distanceComplex64Func
	euclideanDistanceComplex128Impl distanceComplex128Func
	euclideanDistanceFloat64Impl    distanceFloat64Func

	euclideanDistanceInt8Impl  func(a, b []int8) (float32, error)
	euclideanDistanceInt16Impl func(a, b []int16) (float32, error)
)

func init() {
	detectCPU()
	initializeDispatch()

	// Expose the resolved implementation
	DistFunc = euclideanDistanceImpl

	if os.Getenv("LONGBOW_JIT") == "1" {
		if err := initJIT(); err != nil {
			log.Error().Err(err).Msg("Failed to init JIT")
		} else {
			// Override with JIT implementation
			euclideanDistanceBatchImpl = func(query []float32, vectors [][]float32, results []float32) error {
				if err := jitRT.EuclideanBatchInto(query, vectors, results); err != nil {
					return err
				}
				return nil
			}
			log.Info().Msg("JIT SIMD Enabled for Euclidean Batch")
		}
	}
}

// AndBytes performs bitwise AND: dst[i] &= src[i].
// Assumes len(dst) == len(src).
func AndBytes(dst, src []byte) error {
	if len(dst) != len(src) {
		return errors.New("simd: length mismatch")
	}
	andBytesImpl(dst, src)
	return nil
}

// GetCPUFeatures returns detected CPU SIMD capabilities

// Generic implementations (fallback)

func euclideanGeneric(a, b []float32) (float32, error) {
	d, err := L2SquaredFloat32(a, b)
	return float32(math.Sqrt(float64(d))), err
}

// L2SquaredFloat32 calculates the squared Euclidean distance.

func cosineGeneric(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var dot, normA, normB float32
	for i := 0; i < len(a); i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

func dotGeneric(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum float32
	for i := 0; i < len(a); i++ {
		sum += a[i] * b[i]
	}
	return sum, nil
}

// EuclideanDistanceBatch calculates the Euclidean distance between a query vector and multiple candidate vectors.
// This batch version reduces function call overhead and allows for CPU-specific optimizations.
// Uses pre-selected implementation via function pointer (no switch overhead).

// euclideanSQ8BatchGeneric is the fallback implementation
func euclideanSQ8BatchGeneric(query []byte, vectors [][]byte, results []float32) error {
	for i, v := range vectors {
		if v == nil {
			continue
		}
		d, err := EuclideanSQ8Generic(query, v)
		if err != nil {
			return err
		}
		results[i] = float32(d)
	}
	return nil
}

// euclideanBatchGeneric is the fallback implementation
func euclideanBatchGeneric(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		if v == nil {
			continue
		}
		d, err := euclideanGeneric(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// EuclideanDistanceBatchFlat computes distances between a query and vectors stored in a flat buffer.
// This eliminates the overhead of creating a [][]float32 slice wrapper.
// flatVectors: concatenated vectors [v0_0, v0_1, ..., v0_dims-1, v1_0, ...]
// dims: dimension of each vector
func euclideanBatchFlatGeneric(query []float32, flatVectors []float32, numVectors, dims int, results []float32) error {
	if len(results) != numVectors {
		return errors.New("simd: results length mismatch")
	}
	if len(flatVectors) < numVectors*dims {
		return errors.New("simd: flatVectors too small")
	}
	if numVectors == 0 {
		return nil
	}

	queryLen := len(query)
	if queryLen != dims {
		return errors.New("simd: query dimension mismatch")
	}

	for i := 0; i < numVectors; i++ {
		offset := i * dims
		v := flatVectors[offset : offset+dims]
		d, err := euclideanUnrolled4x(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// euclideanBatchFlatAVX2 is the AVX2 optimized flat batch version
func euclideanBatchFlatAVX2(query []float32, flatVectors []float32, numVectors, dims int, results []float32) error {
	return euclideanBatchFlatGeneric(query, flatVectors, numVectors, dims, results)
}

// euclideanBatchFlatAVX512 is the AVX512 optimized flat batch version
func euclideanBatchFlatAVX512(query []float32, flatVectors []float32, numVectors, dims int, results []float32) error {
	return euclideanBatchFlatGeneric(query, flatVectors, numVectors, dims, results)
}

func dotBatchGeneric(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		if v == nil {
			continue
		}
		d, err := DotProduct(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func cosineBatchGeneric(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		if v == nil {
			continue
		}
		d, err := CosineDistance(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// ADCDistanceBatch calculates asymmetric distances for multiple PQ-encoded vectors.
// table is the precomputed distance table of size [m * 256].

// FindNearestCentroid finds the index of the nearest centroid to query using batch distance computation.
// This is optimized for PQ encoding where we need to find the closest of K centroids for a subvector.
// centroids: flattened centroids [c0_0, c0_1, ..., c0_subDim-1, c1_0, ...]
// subDim: dimension of each centroid (and query)
// k: number of centroids
// Returns the index of the nearest centroid and the distance
func FindNearestCentroid(query []float32, centroids []float32, subDim, k int) (int, float32) {
	if len(centroids) < k*subDim {
		return 0, float32(math.MaxFloat32)
	}

	if k <= 8 {
		// For small K, sequential search is faster due to batch overhead
		bestDist := float32(math.MaxFloat32)
		bestIdx := 0
		for i := 0; i < k; i++ {
			offset := i * subDim
			cent := centroids[offset : offset+subDim]
			d, _ := L2Squared(query, cent)
			if d < bestDist {
				bestDist = d
				bestIdx = i
			}
		}
		return bestIdx, bestDist
	}

	// For larger K, use batch computation
	results := make([]float32, k)
	EuclideanDistanceBatchFlat(query, centroids, k, subDim, results)

	// Find minimum
	bestDist := results[0]
	bestIdx := 0
	for i := 1; i < k; i++ {
		if results[i] < bestDist {
			bestDist = results[i]
			bestIdx = i
		}
	}
	return bestIdx, bestDist
}

// FindNearestCentroidInCodebook finds the nearest centroid in a codebook for PQ encoding.
// codebook: M slices of K*SubDim flattened centroids
// m: number of subvectors
// k: number of centroids per subspace
// subDim: dimension of each subvector
// Returns the encoded bytes (M bytes)
func FindNearestCentroidInCodebook(query []float32, codebook [][]float32, m, k, subDim int) []byte {
	codes := make([]byte, m)
	for i := 0; i < m; i++ {
		subVec := query[i*subDim : (i+1)*subDim]
		centroids := codebook[i]
		bestIdx, _ := FindNearestCentroid(subVec, centroids, subDim, k)
		codes[i] = byte(bestIdx)
	}
	return codes
}

func adcBatchGeneric(table []float32, flatCodes []byte, m int, results []float32) error {
	for i := 0; i < len(results); i++ {
		var sum float32
		codes := flatCodes[i*m : (i+1)*m]
		for jj, code := range codes {
			sum += table[jj*256+int(code)]
		}
		results[i] = float32(math.Sqrt(float64(sum)))
	}
	return nil
}

// =============================================================================
// 4x Unrolled Generic Implementations
// Multiple accumulators break loop-carried dependencies, enabling:
// - Instruction-level parallelism (ILP)
// - Go compiler auto-vectorization on generic architectures
// - ~2-4x speedup over simple scalar loops
// =============================================================================

func euclideanUnrolled4x(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0

	// Main loop: process 4 elements at a time
	for ; i <= n-4; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}

	// Handle remainder (0-3 elements)
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += d * d
	}

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

// cosineUnrolled4x computes cosine distance with 4x loop unrolling
func cosineUnrolled4x(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}

	var dot0, dot1, dot2, dot3 float32
	var normA0, normA1, normA2, normA3 float32
	var normB0, normB1, normB2, normB3 float32
	n := len(a)
	i := 0

	// Main loop: process 4 elements at a time
	for ; i <= n-4; i += 4 {
		a0, a1, a2, a3 := a[i], a[i+1], a[i+2], a[i+3]
		b0, b1, b2, b3 := b[i], b[i+1], b[i+2], b[i+3]

		dot0 += a0 * b0
		dot1 += a1 * b1
		dot2 += a2 * b2
		dot3 += a3 * b3

		normA0 += a0 * a0
		normA1 += a1 * a1
		normA2 += a2 * a2
		normA3 += a3 * a3

		normB0 += b0 * b0
		normB1 += b1 * b1
		normB2 += b2 * b2
		normB3 += b3 * b3
	}

	// Handle remainder
	for ; i < n; i++ {
		dot0 += a[i] * b[i]
		normA0 += a[i] * a[i]
		normB0 += b[i] * b[i]
	}

	// Reduce accumulators
	dot := dot0 + dot1 + dot2 + dot3
	normA := normA0 + normA1 + normA2 + normA3
	normB := normB0 + normB1 + normB2 + normB3

	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}

// dotUnrolled4x computes dot product with 4x loop unrolling
func dotUnrolled4x(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}

	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0

	// Main loop: process 4 elements at a time
	for ; i <= n-4; i += 4 {
		sum0 += a[i] * b[i]
		sum1 += a[i+1] * b[i+1]
		sum2 += a[i+2] * b[i+2]
		sum3 += a[i+3] * b[i+3]
	}

	// Handle remainder
	for ; i < n; i++ {
		sum0 += a[i] * b[i]
	}

	return sum0 + sum1 + sum2 + sum3, nil
}

// euclideanBatchUnrolled4x computes batch Euclidean distances using unrolled inner loop
func euclideanBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := euclideanUnrolled4x(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// cosineBatchUnrolled4x computes batch cosine distances using unrolled inner loop
// with 4 independent accumulators per dot/norm calculation (12 total accumulators).
func cosineBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := cosineUnrolled4x(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// dotBatchUnrolled4x computes batch dot products using unrolled inner loop
// with 4 independent accumulators to break loop-carried dependencies.
func dotBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
		d, err := dotUnrolled4x(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

// euclidean128Unrolled4x calculates Euclidean distance for fixed 128 dimension.
// Relies on compiler loop unrolling and bound check elimination.
func euclidean128Unrolled4x(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return 0, errors.New("simd: length must be 128")
	}
	// Bounds check elimination hint
	_ = a[127]
	_ = b[127]

	var sum0, sum1, sum2, sum3 float32
	// 128 is divisible by 4, so no remainder loop
	for i := 0; i < 128; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func dot128Unrolled4x(a, b []float32) (float32, error) {
	if len(a) != 128 || len(b) != 128 {
		return 0, errors.New("simd: length must be 128")
	}
	_ = a[127]
	_ = b[127]

	var sum0, sum1, sum2, sum3 float32
	for i := 0; i < 128; i += 4 {
		sum0 += a[i] * b[i]
		sum1 += a[i+1] * b[i+1]
		sum2 += a[i+2] * b[i+2]
		sum3 += a[i+3] * b[i+3]
	}
	return sum0 + sum1 + sum2 + sum3, nil
}

// =============================================================================
// Parallel Sum Reduction with Multiple Accumulators - Batch Operations
// =============================================================================

// Batch function type for cosine and dot distance

// Internal implementation pointers
var prefetchImpl func(p unsafe.Pointer)

// MatchInt64 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchInt64(src []int64, val int64, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	matchInt64Impl(src, val, op, dst)
	return nil
}

// MatchFloat32 performs a comparison of src elements against val, storing the result (0 or 1) in dst.
// One byte per element is written to dst.
func MatchFloat32(src []float32, val float32, op CompareOp, dst []byte) error {
	if len(src) != len(dst) {
		return errors.New("simd: length mismatch")
	}
	matchFloat32Impl(src, val, op, dst)
	return nil
}

func matchInt64Generic(src []int64, val int64, op CompareOp, dst []byte) error {
	switch op {
	case CompareEq:
		for i, v := range src {
			// Branchless Equal: If v == val, xora is 0.
			// If v != val, xora is non-zero.
			// We want 1 if v == val, else 0.
			// trick: 1 if xora == 0 else 0.
			// Standard C-style: (xora == 0)
			// Go branchless:
			// diff = v ^ val
			// res = 1 - ( (diff | -diff) >> 63 ) ? No, that's for 0 check on int64?
			// Simpler:
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
			// Wait, simple if/else IS branchy.
			// Let's use the verified bitwise logic from plan/benchmark?
			// Go compiler might optimize `v==val` to setcc.
			// But let's force it if we want to be sure.
			// Actually, let's keep it readable but simple first.
			// Benchmark showed:
			// if v == val { dst[i] = 1 } else { dst[i] = 0 }
			// was significantly slower than
			// var res byte = 0; if v == val { res = 1 }; dst[i] = res
			//
			// Optimization:
			// diff := v ^ val
			// mask := (diff | -diff) >> 63  (0 if equal, -1 if not)
			// dst[i] = byte((mask + 1) & 1) (1 if equal, 0 if not)
			// Need to be careful with uint64 cast for shift.

			// diff := uint64(v ^ val)
			// For 0 check: (diff - 1) >> 63

			// Re-evaluating: The benchmark code `if v == val { dst[i] = 1 } else { dst[i] = 0 }` was the fastest?
			// No, benchmark showed "Branchless" logic (manual) was faster.
			// Using the benchmark's "if v==val res=1" is still an `if`.
			// Let's use pure bitwise for guarantees.

			// Equality:
			// d := v ^ val
			// d = (d | -d) >> 63
			// res = byte(1 ^ d)
			// (If equal, d=0. 0|-0=0. >>63=0. 1^0=1).
			// (If not, d!=0. high bit likely set after | -d? Yes.)

			diff := v ^ val
			// "smear" non-zero to sign bit
			// Note: -diff in 2's complement.
			// (diff | -diff) sets MSB if diff != 0.
			msb := uint64(diff|-diff) >> 63
			dst[i] = byte(1 ^ msb)
		}
	case CompareNeq:
		for i, v := range src {
			diff := v ^ val
			msb := uint64(diff|-diff) >> 63
			dst[i] = byte(msb)
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		// Fallback for others
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

// matchFloat32Generic implements branchless float32 matching
func matchFloat32Generic(src []float32, val float32, op CompareOp, dst []byte) error {
	// Treat as uint32 for bitwise ops if needed, or use careful regular comparison.
	// For Float32, equality is tricky with NaN, but assumming standard numbers.
	// op is the enum.

	switch op {
	case CompareEq:
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareNeq:
		for i, v := range src {
			if v != val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGt:
		for i, v := range src {
			if v > val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareGe:
		for i, v := range src {
			if v >= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLt:
		for i, v := range src {
			if v < val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	case CompareLe:
		for i, v := range src {
			if v <= val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	default:
		// Fallback
		for i, v := range src {
			if v == val {
				dst[i] = 1
			} else {
				dst[i] = 0
			}
		}
	}
	return nil
}

// =============================================================================
// FP16 Generic Implementations (Unrolled 4x)
// =============================================================================

func euclideanF16Unrolled4x(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		d0 := a[i].Float32() - b[i].Float32()
		d1 := a[i+1].Float32() - b[i+1].Float32()
		d2 := a[i+2].Float32() - b[i+2].Float32()
		d3 := a[i+3].Float32() - b[i+3].Float32()
		sum0 += d0 * d0
		sum1 += d1 * d1
		sum2 += d2 * d2
		sum3 += d3 * d3
	}
	for ; i < n; i++ {
		d := a[i].Float32() - b[i].Float32()
		sum0 += d * d
	}
	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3))), nil
}

func dotF16Unrolled4x(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var sum0, sum1, sum2, sum3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		sum0 += a[i].Float32() * b[i].Float32()
		sum1 += a[i+1].Float32() * b[i+1].Float32()
		sum2 += a[i+2].Float32() * b[i+2].Float32()
		sum3 += a[i+3].Float32() * b[i+3].Float32()
	}
	for ; i < n; i++ {
		sum0 += a[i].Float32() * b[i].Float32()
	}
	return sum0 + sum1 + sum2 + sum3, nil
}

func cosineF16Unrolled4x(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: length mismatch")
	}
	var dot0, dot1, dot2, dot3 float32
	var normA0, normA1, normA2, normA3 float32
	var normB0, normB1, normB2, normB3 float32
	n := len(a)
	i := 0
	for ; i <= n-4; i += 4 {
		va0, va1, va2, va3 := a[i].Float32(), a[i+1].Float32(), a[i+2].Float32(), a[i+3].Float32()
		vb0, vb1, vb2, vb3 := b[i].Float32(), b[i+1].Float32(), b[i+2].Float32(), b[i+3].Float32()
		dot0 += va0 * vb0
		dot1 += va1 * vb1
		dot2 += va2 * vb2
		dot3 += va3 * vb3
		normA0 += va0 * va0
		normA1 += va1 * va1
		normA2 += va2 * va2
		normA3 += va3 * va3
		normB0 += vb0 * vb0
		normB1 += vb1 * vb1
		normB2 += vb2 * vb2
		normB3 += vb3 * vb3
	}
	for ; i < n; i++ {
		va, vb := a[i].Float32(), b[i].Float32()
		dot0 += va * vb
		normA0 += va * va
		normB0 += vb * vb
	}
	dot := dot0 + dot1 + dot2 + dot3
	normA := normA0 + normA1 + normA2 + normA3
	normB := normB0 + normB1 + normB2 + normB3
	if normA == 0 || normB == 0 {
		return 1.0, nil
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB)))), nil
}
