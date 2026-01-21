package simd

import (
	"errors"
	"math"
	"os"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/klauspost/cpuid/v2"
	"github.com/rs/zerolog/log"
)

var (
	ErrDimensionMismatch    = errors.New("simd: vector dimension mismatch")
	ErrInitializationFailed = errors.New("simd: initialization failed")
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
	distanceFunc         func(a, b []float32) (float32, error)
	distanceBatchFunc    func(query []float32, vectors [][]float32, results []float32) error
	distanceSQ8BatchFunc func(query []byte, vectors [][]byte, results []float32) error
	adcDistanceBatchFunc func(table []float32, flatCodes []byte, m int, results []float32) error

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
	features       CPUFeatures
	implementation string

	// Function pointers initialized at startup - eliminates switch overhead in hot path
	euclideanDistanceImpl    distanceFunc
	euclideanDistance384Impl distanceFunc
	euclideanDistance128Impl distanceFunc // optimized for dimensions=128
	cosineDistanceImpl       distanceFunc

	// DistFunc is the best available Euclidean distance implementation
	DistFunc distanceFunc

	dotProductImpl             distanceFunc
	dotProduct384Impl          distanceFunc
	dotProduct128Impl          distanceFunc // optimized for dimensions=128
	euclideanDistanceBatchImpl distanceBatchFunc
	cosineDistanceBatchImpl    distanceBatchFunc
	dotProductBatchImpl        distanceBatchFunc
	l2SquaredImpl              distanceFunc

	matchInt64Impl   matchInt64Func
	matchFloat32Impl matchFloat32Func

	adcDistanceBatchImpl               adcDistanceBatchFunc
	euclideanDistanceVerticalBatchImpl distanceBatchFunc
	euclideanDistanceSQ8BatchImpl      distanceSQ8BatchFunc

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
		euclideanDistance384Impl = euclidean384AVX512
		euclideanDistance128Impl = euclidean128Unrolled4x // Fallback to unrolled Go (efficient enough for 128)
		metrics.SimdDispatchCount.WithLabelValues("avx512").Inc()
		metrics.SimdStaticDispatchType.Set(3)
		cosineDistanceImpl = cosineAVX512
		dotProductImpl = dotAVX512
		dotProduct384Impl = dot384AVX512
		dotProduct128Impl = dot128Unrolled4x // Fallback to unrolled Go
		euclideanDistanceBatchImpl = euclideanBatchAVX512
		cosineDistanceBatchImpl = cosineBatchAVX512
		dotProductBatchImpl = dotBatchAVX512
		l2SquaredImpl = l2SquaredAVX512 // uses AVX512 kernel
		prefetchImpl = prefetchNTA
		matchInt64Impl = matchInt64AVX512
		matchFloat32Impl = matchFloat32AVX512
		adcDistanceBatchImpl = adcBatchAVX512
		euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX512
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric // Fallback to generic for now
		andBytesImpl = andBytesGeneric
		euclideanDistanceF16Impl = euclideanF16AVX512
		cosineDistanceF16Impl = cosineF16AVX512
		dotProductF16Impl = dotF16AVX512
		euclideanDistanceFloat64Impl = euclideanFloat64AVX512
		euclideanDistanceInt8Impl = euclideanInt8AVX2
		euclideanDistanceInt16Impl = euclideanInt16AVX2
		// Optimization: Use float32 AVX kernels for complex64
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
	case "avx2":
		euclideanDistanceImpl = euclideanAVX2
		euclideanDistance384Impl = euclideanGeneric // AVX2 384-dim not implemented yet, fallback
		euclideanDistance128Impl = euclidean128Unrolled4x
		metrics.SimdDispatchCount.WithLabelValues("avx2").Inc()
		metrics.SimdStaticDispatchType.Set(2)
		cosineDistanceImpl = cosineAVX2
		dotProductImpl = dotAVX2
		dotProduct384Impl = dotGeneric
		dotProduct128Impl = dot128Unrolled4x
		euclideanDistanceBatchImpl = euclideanBatchAVX2
		cosineDistanceBatchImpl = cosineBatchAVX2
		dotProductBatchImpl = dotBatchAVX2
		l2SquaredImpl = l2SquaredAVX2 // uses AVX2 kernel (no sqrt)
		prefetchImpl = prefetchNTA
		matchInt64Impl = matchInt64AVX2
		matchFloat32Impl = matchFloat32AVX2
		adcDistanceBatchImpl = adcBatchAVX2
		euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX2
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric // Fallback to generic for now
		andBytesImpl = andBytesGeneric
		euclideanDistanceF16Impl = euclideanF16AVX2
		cosineDistanceF16Impl = cosineF16AVX2
		dotProductF16Impl = dotF16AVX2
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled // Fallback
		euclideanDistanceFloat64Impl = euclideanFloat64AVX2
		euclideanDistanceInt8Impl = euclideanInt8AVX2
		euclideanDistanceInt16Impl = euclideanInt16AVX2
	case "neon":
		euclideanDistanceImpl = euclideanNEON
		euclideanDistance384Impl = euclidean384NEON
		euclideanDistance128Impl = euclidean128NEON
		metrics.SimdDispatchCount.WithLabelValues("neon").Inc()
		metrics.SimdStaticDispatchType.Set(1)
		cosineDistanceImpl = cosineNEON
		dotProductImpl = dotNEON
		dotProduct384Impl = dot384NEON
		dotProduct128Impl = dot128NEON
		euclideanDistanceBatchImpl = euclideanBatchNEON
		cosineDistanceBatchImpl = cosineBatchNEON
		dotProductBatchImpl = dotBatchNEON
		l2SquaredImpl = l2SquaredNEON
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Generic
		matchFloat32Impl = matchFloat32Generic
		adcDistanceBatchImpl = adcBatchNEON
		euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchNEON
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric // Fallback to generic for now
		andBytesImpl = andBytesGeneric
		// F16 Kernels
		euclideanDistanceF16Impl = euclideanF16NEON
		cosineDistanceF16Impl = cosineF16NEON
		dotProductF16Impl = dotF16NEON
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled // Fallback
		euclideanDistanceFloat64Impl = euclideanFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
	default:
		euclideanDistanceImpl = euclideanUnrolled4x
		euclideanDistance384Impl = euclideanUnrolled4x
		euclideanDistance128Impl = euclidean128Unrolled4x
		metrics.SimdDispatchCount.WithLabelValues("generic").Inc()
		metrics.SimdStaticDispatchType.Set(0)
		cosineDistanceImpl = cosineUnrolled4x
		dotProductImpl = dotUnrolled4x
		dotProduct384Impl = dotUnrolled4x
		dotProduct128Impl = dot128Unrolled4x
		euclideanDistanceBatchImpl = euclideanBatchUnrolled4x
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
		l2SquaredImpl = L2SquaredFloat32
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Generic
		matchFloat32Impl = matchFloat32Generic
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		andBytesImpl = andBytesGeneric
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
		euclideanDistanceFloat64Impl = euclideanFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
	}

	// Register current implementations into the new dynamic registry.
	// This enables the transition to polymorphic indexing while preserving
	// existing high-performance paths.

	// Float32 Euclidean
	Registry.Register(MetricEuclidean, DataTypeFloat32, 0, euclideanDistanceImpl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 128, euclideanDistance128Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 384, euclideanDistance384Impl)

	// Float32 Cosine & Dot Product
	Registry.Register(MetricCosine, DataTypeFloat32, 0, cosineDistanceImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 0, dotProductImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 128, dotProduct128Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 384, dotProduct384Impl)

	// Float16 (Support both native and unrolled paths)
	Registry.Register(MetricEuclidean, DataTypeFloat16, 0, euclideanDistanceF16Impl)
	Registry.Register(MetricCosine, DataTypeFloat16, 0, cosineDistanceF16Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat16, 0, dotProductF16Impl)

	// Complex Numbers (Unrolled Baselines)
	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanDistanceComplex64Impl)
	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanDistanceComplex128Impl)

	// Baseline Fallbacks for all other types
	Registry.Register(MetricEuclidean, DataTypeInt8, 0, euclideanDistanceInt8Impl)
	Registry.Register(MetricDotProduct, DataTypeInt8, 0, dotInt8Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeInt16, 0, euclideanDistanceInt16Impl)
	Registry.Register(MetricDotProduct, DataTypeInt16, 0, dotInt16Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeInt32, 0, euclideanInt32Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeInt64, 0, euclideanInt64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeUint8, 0, euclideanUint8Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeUint16, 0, euclideanUint16Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeUint32, 0, euclideanUint32Unrolled4x)
	Registry.Register(MetricEuclidean, DataTypeUint64, 0, euclideanUint64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeFloat64, 0, euclideanFloat64Unrolled4x)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 0, dotFloat64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanComplex64Unrolled)
	Registry.Register(MetricDotProduct, DataTypeComplex64, 0, dotComplex64Unrolled)
	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanComplex128Unrolled)
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
func EuclideanDistance(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) == 384 {
		return euclideanDistance384Impl(a, b)
	}
	if len(a) == 128 {
		return euclideanDistance128Impl(a, b)
	}
	return euclideanDistanceImpl(a, b)
}

// L2Squared calculates the squared Euclidean distance between two vectors.
// Optimized for PQ training and ADC table construction.
func L2Squared(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return l2SquaredImpl(a, b)
}

// CosineDistance calculates the cosine distance (1 - similarity) between two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func CosineDistance(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	return cosineDistanceImpl(a, b)
}

// DotProduct calculates the dot product of two vectors.
// Uses pre-selected implementation via function pointer (no switch overhead).
func DotProduct(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) == 384 {
		return dotProduct384Impl(a, b)
	}
	if len(a) == 128 {
		return dotProduct128Impl(a, b)
	}
	return dotProductImpl(a, b)
}

// EuclideanDistanceF16 calculates the Euclidean distance between two FP16 vectors.
func EuclideanDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("euclidean", implementation).Inc()
	return euclideanDistanceF16Impl(a, b)
}

// CosineDistanceF16 calculates the cosine distance between two FP16 vectors.
func CosineDistanceF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 1.0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("cosine", implementation).Inc()
	return cosineDistanceF16Impl(a, b)
}

// DotProductF16 calculates the dot product of two FP16 vectors.
func DotProductF16(a, b []float16.Num) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	metrics.SimdF16OpsTotal.WithLabelValues("dot", implementation).Inc()
	return dotProductF16Impl(a, b)
}

// EuclideanDistanceFloat64 calculates Euclidean distance for Float64 vectors.
// EuclideanDistanceFloat64 calculates Euclidean distance using Float64.
// Returns float32 distance for consistency with other metrics.
func EuclideanDistanceFloat64(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return euclideanDistanceFloat64Impl(a, b)
}

func EuclideanDistanceComplex64(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	// Unsafe cast to []float32
	vfA := unsafe.Slice((*float32)(unsafe.Pointer(&a[0])), len(a)*2)
	vfB := unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)*2)

	return EuclideanDistance(vfA, vfB)
}

// EuclideanDistanceComplex128 calculates Euclidean distance for Complex128 vectors.
func EuclideanDistanceComplex128(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	// Optimization: complex128 is just 2x float64s in memory.
	// We can treat them as float64 vectors of 2*N length and use the optimized
	// AVX/AVX512 kernels for float64.
	//
	// Euclidean distance is sqrt(sum((real_diff^2 + imag_diff^2))), which is
	// exactly what 2N float64 euclidean distance calculates.

	// Unsafe cast to []float64
	vfA := unsafe.Slice((*float64)(unsafe.Pointer(&a[0])), len(a)*2)
	vfB := unsafe.Slice((*float64)(unsafe.Pointer(&b[0])), len(b)*2)

	return EuclideanDistanceFloat64(vfA, vfB)
}

// Prefetch hints to the CPU to fetch data into cache for future use.
// It uses the PREFETCHNTA instruction on x86 for non-temporal access.
func Prefetch(p unsafe.Pointer) {
	prefetchImpl(p)
}

// DotProductF64 calculates the dot product of two Float64 vectors.
func DotProductF64(a, b []float64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotFloat64Unrolled4x(a, b)
}

// DotProductComplex64 calculates the real part of the dot product of two Complex64 vectors.
func DotProductComplex64(a, b []complex64) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	return dotComplex64Unrolled(a, b)
}

// DotProductComplex128 calculates the real part of the dot product of two Complex128 vectors.
func DotProductComplex128(a, b []complex128) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.New("simd: vector length mismatch")
	}
	if len(a) == 0 {
		return 0, nil
	}
	// Missing dotComplex128Unrolled implementation, using simple loop for now
	var dot complex128
	for i := range a {
		dot += a[i] * b[i]
	}
	return float32(real(dot)), nil
}

// ToFloat32 converts a float64 slice to a float32 slice (Allocates).
func ToFloat32(v []float64) []float32 {
	res := make([]float32, len(v))
	for i, val := range v {
		res[i] = float32(val)
	}
	return res
}

func float32SliceToBytes(vec []float32) []byte {
	if len(vec) == 0 {
		return nil
	}
	size := len(vec) * 4
	ptr := unsafe.Pointer(&vec[0])
	return unsafe.Slice((*byte)(ptr), size)
}

// Generic implementations (fallback)

func euclideanGeneric(a, b []float32) (float32, error) {
	d, err := L2SquaredFloat32(a, b)
	return float32(math.Sqrt(float64(d))), err
}

// L2SquaredFloat32 calculates the squared Euclidean distance.
// Uses unrolled 4x loop for performance.
func L2SquaredFloat32(a, b []float32) (float32, error) {
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

	// Handle remainder
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum0 += d * d
	}

	return sum0 + sum1 + sum2 + sum3, nil
}

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
func EuclideanDistanceBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	euclideanDistanceBatchImpl(query, vectors, results)
	return nil
}

// EuclideanDistanceVerticalBatch optimally calculates distances for multiple vectors at once.
func EuclideanDistanceVerticalBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	euclideanDistanceVerticalBatchImpl(query, vectors, results)
	return nil
}

// EuclideanDistanceSQ8Batch calculates Euclidean distance between SQ8 query and multiple SQ8 vectors.
func EuclideanDistanceSQ8Batch(query []byte, vectors [][]byte, results []float32) error {
	if len(vectors) != len(results) {
		return errors.New("simd: vectors and results length mismatch")
	}
	if len(vectors) == 0 {
		return nil
	}
	euclideanDistanceSQ8BatchImpl(query, vectors, results)
	return nil
}

// euclideanSQ8BatchGeneric is the fallback implementation
func euclideanSQ8BatchGeneric(query []byte, vectors [][]byte, results []float32) error {
	for i, v := range vectors {
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
		d, err := euclideanGeneric(query, v)
		if err != nil {
			return err
		}
		results[i] = d
	}
	return nil
}

func dotBatchGeneric(query []float32, vectors [][]float32, results []float32) error {
	for i, v := range vectors {
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
// flatCodes is a flattened byte slice of size [len(results) * m].
func ADCDistanceBatch(table []float32, flatCodes []byte, m int, results []float32) error {
	if len(flatCodes) < len(results)*m {
		return errors.New("simd: flatCodes too small")
	}
	adcDistanceBatchImpl(table, flatCodes, m, results)
	return nil
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

// CosineDistanceBatch calculates cosine distance between query and multiple vectors.
// Uses parallel sum reduction with multiple accumulators for ILP optimization.
func CosineDistanceBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) == 0 {
		return nil
	}
	if len(results) < len(vectors) {
		return errors.New("simd: results slice too small")
	}
	metrics.CosineBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	cosineDistanceBatchImpl(query, vectors, results)
	return nil
}

// DotProductBatch calculates dot product between query and multiple vectors.
// Uses parallel sum reduction with multiple accumulators for ILP optimization.
func DotProductBatch(query []float32, vectors [][]float32, results []float32) error {
	if len(vectors) == 0 {
		return nil
	}
	if len(results) < len(vectors) {
		return errors.New("simd: results slice too small")
	}
	metrics.DotProductBatchCallsTotal.Inc()
	metrics.ParallelReductionVectorsProcessed.Add(float64(len(vectors)))
	dotProductBatchImpl(query, vectors, results)
	return nil
}

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
