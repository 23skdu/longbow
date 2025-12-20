package simd

import (
	"github.com/23skdu/longbow/internal/metrics"
	"math"

	"github.com/klauspost/cpuid/v2"
)

// CPUFeatures contains detected CPU SIMD capabilities
type CPUFeatures struct {
	Vendor    string
	HasAVX2   bool
	HasAVX512 bool
	HasNEON   bool
}

// Function pointer types for dispatch
type (
	distanceFunc      func(a, b []float32) float32
	distanceBatchFunc func(query []float32, vectors [][]float32, results []float32)
)

var (
	features       CPUFeatures
	implementation string

	// Function pointers initialized at startup - eliminates switch overhead in hot path
	euclideanDistanceImpl      distanceFunc
	cosineDistanceImpl         distanceFunc
	dotProductImpl             distanceFunc
	euclideanDistanceBatchImpl distanceBatchFunc
)

func init() {
	detectCPU()
	initializeDispatch()
}

func detectCPU() {
	features = CPUFeatures{
		Vendor:    cpuid.CPU.VendorString,
		HasAVX2:   cpuid.CPU.Supports(cpuid.AVX2),
		HasAVX512: cpuid.CPU.Supports(cpuid.AVX512F) && cpuid.CPU.Supports(cpuid.AVX512DQ),
		HasNEON:   cpuid.CPU.Supports(cpuid.ASIMD), // ARM NEON
	}

	// Select best implementation
	switch {
	case features.HasAVX512:
		implementation = "avx512"
	case features.HasAVX2:
		implementation = "avx2"
	case features.HasNEON:
		implementation = "neon"
	default:
		implementation = "generic"
	}
}

// initializeDispatch sets function pointers based on detected CPU features.
// This is called once at startup, removing branch overhead from hot paths.
func initializeDispatch() {
	switch implementation {
	case "avx512":
		euclideanDistanceImpl = euclideanAVX512
		metrics.SimdDispatchCount.WithLabelValues("avx512").Inc()
		cosineDistanceImpl = cosineAVX512
		dotProductImpl = dotAVX512
		euclideanDistanceBatchImpl = euclideanBatchAVX512
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
	case "avx2":
		euclideanDistanceImpl = euclideanAVX2
		metrics.SimdDispatchCount.WithLabelValues("avx2").Inc()
		cosineDistanceImpl = cosineAVX2
		dotProductImpl = dotAVX2
		euclideanDistanceBatchImpl = euclideanBatchAVX2
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
	case "neon":
		euclideanDistanceImpl = euclideanNEON
		metrics.SimdDispatchCount.WithLabelValues("neon").Inc()
		cosineDistanceImpl = cosineNEON
		dotProductImpl = dotNEON
		euclideanDistanceBatchImpl = euclideanBatchNEON
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
	default:
		euclideanDistanceImpl = euclideanUnrolled4x
		metrics.SimdDispatchCount.WithLabelValues("generic").Inc()
		cosineDistanceImpl = cosineUnrolled4x
		dotProductImpl = dotUnrolled4x
		euclideanDistanceBatchImpl = euclideanBatchUnrolled4x
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
	}
}

// GetCPUFeatures returns detected CPU SIMD capabilities
func GetCPUFeatures() CPUFeatures {
	return features
}

// GetImplementation returns the selected SIMD implementation name
func GetImplementation() string {
	return implementation
}

// EuclideanDistance calculates the Euclidean distance between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func EuclideanDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0
	}
	return euclideanDistanceImpl(a, b)
}

// CosineDistance calculates the cosine distance (1 - similarity) between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 1.0
	}
	return cosineDistanceImpl(a, b)
}

// DotProduct calculates the dot product of two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func DotProduct(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0
	}
	return dotProductImpl(a, b)
}

// Generic implementations (fallback)

func euclideanGeneric(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a); i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return float32(math.Sqrt(float64(sum)))
}

func cosineGeneric(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := 0; i < len(a); i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

func dotGeneric(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a); i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// EuclideanDistanceBatch calculates the Euclidean distance between a query vector and multiple candidate vectors.
// This batch version reduces function call overhead and allows for CPU-specific optimizations.
// Uses pre-selected implementation via function pointer (no switch overhead).
func EuclideanDistanceBatch(query []float32, vectors [][]float32, results []float32) {
	if len(vectors) != len(results) {
		panic("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return
	}
	euclideanDistanceBatchImpl(query, vectors, results)
}

// euclideanBatchGeneric is the fallback implementation
func euclideanBatchGeneric(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = euclideanGeneric(query, v)
	}
}

// =============================================================================
// 4x Unrolled Generic Implementations
// Multiple accumulators break loop-carried dependencies, enabling:
// - Instruction-level parallelism (ILP)
// - Go compiler auto-vectorization on generic architectures
// - ~2-4x speedup over simple scalar loops
// =============================================================================

// euclideanUnrolled4x computes Euclidean distance with 4x loop unrolling
func euclideanUnrolled4x(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
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

	return float32(math.Sqrt(float64(sum0 + sum1 + sum2 + sum3)))
}

// cosineUnrolled4x computes cosine distance with 4x loop unrolling
func cosineUnrolled4x(a, b []float32) float32 {
	if len(a) == 0 {
		return 1.0
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
		return 1.0
	}
	return 1.0 - (dot / float32(math.Sqrt(float64(normA)*float64(normB))))
}

// dotUnrolled4x computes dot product with 4x loop unrolling
func dotUnrolled4x(a, b []float32) float32 {
	if len(a) == 0 {
		return 0
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

	return sum0 + sum1 + sum2 + sum3
}

// euclideanBatchUnrolled4x computes batch Euclidean distances using unrolled inner loop
func euclideanBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = euclideanUnrolled4x(query, v)
	}
}

// =============================================================================
// Parallel Sum Reduction with Multiple Accumulators - Batch Operations
// =============================================================================

// Batch function type for cosine and dot distance
var (
	cosineDistanceBatchImpl distanceBatchFunc
	dotProductBatchImpl     distanceBatchFunc
)

// CosineDistanceBatch calculates cosine distance between query and multiple vectors.
// Uses parallel sum reduction with multiple accumulators for ILP optimization.
func CosineDistanceBatch(query []float32, vectors [][]float32, results []float32) {
	if len(vectors) == 0 {
		return
	}
	if len(results) < len(vectors) {
		panic("results slice too small")
	}
	metrics.CosineBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	cosineDistanceBatchImpl(query, vectors, results)
}

// DotProductBatch calculates dot product between query and multiple vectors.
// Uses parallel sum reduction with multiple accumulators for ILP optimization.
func DotProductBatch(query []float32, vectors [][]float32, results []float32) {
	if len(vectors) == 0 {
		return
	}
	if len(results) < len(vectors) {
		panic("results slice too small")
	}
	metrics.DotProductBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	dotProductBatchImpl(query, vectors, results)
}

// cosineBatchUnrolled4x computes batch cosine distances using unrolled inner loop
// with 4 independent accumulators per dot/norm calculation (12 total accumulators).
func cosineBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = cosineUnrolled4x(query, v)
	}
}

// dotBatchUnrolled4x computes batch dot products using unrolled inner loop
// with 4 independent accumulators to break loop-carried dependencies.
func dotBatchUnrolled4x(query []float32, vectors [][]float32, results []float32) {
	for i, v := range vectors {
		results[i] = dotUnrolled4x(query, v)
	}
}
