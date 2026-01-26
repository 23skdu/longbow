package simd

import (
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ImplementationDispatch holds all SIMD function pointers for a specific implementation
type ImplementationDispatch struct {
	// Core distance functions
	EuclideanDistance distanceFunc
	CosineDistance    distanceFunc
	DotProduct        distanceFunc

	// Batch functions
	EuclideanDistanceBatch     distanceBatchFunc
	CosineDistanceBatch        distanceBatchFunc
	DotProductBatch            distanceBatchFunc
	EuclideanDistanceBatchFlat distanceBatchFlatFunc

	// Specialized functions
	EuclideanDistance384 distanceFunc
	EuclideanDistance128 distanceFunc
}

// Global dispatch table - one per implementation
var dispatchTable = map[string]ImplementationDispatch{
	"avx512": {
		EuclideanDistance:          euclideanAVX512,
		CosineDistance:             cosineAVX512,
		DotProduct:                 dotAVX512,
		EuclideanDistanceBatch:     euclideanBatchAVX512,
		CosineDistanceBatch:        cosineBatchAVX512,
		DotProductBatch:            dotBatchAVX512,
		EuclideanDistanceBatchFlat: euclideanBatchFlatAVX512,
		EuclideanDistance384:       euclidean384AVX512,
		EuclideanDistance128:       euclidean128Unrolled4x,
	},
	"avx2": {
		EuclideanDistance:          euclideanAVX2,
		CosineDistance:             cosineAVX2,
		DotProduct:                 dotAVX2,
		EuclideanDistanceBatch:     euclideanBatchAVX2,
		CosineDistanceBatch:        cosineBatchAVX2,
		DotProductBatch:            dotBatchAVX2,
		EuclideanDistanceBatchFlat: euclideanBatchFlatAVX2,
		EuclideanDistance384:       euclideanGeneric,
		EuclideanDistance128:       euclidean128Unrolled4x,
	},
	"neon": {
		EuclideanDistance:          euclideanNEON,
		CosineDistance:             cosineNEON,
		DotProduct:                 dotNEON,
		EuclideanDistanceBatch:     euclideanBatchNEON,
		CosineDistanceBatch:        cosineBatchNEON,
		DotProductBatch:            dotBatchNEON,
		EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric, // Fallback for NEON
		EuclideanDistance384:       euclideanGeneric,
		EuclideanDistance128:       euclidean128Unrolled4x,
	},
	"generic": {
		EuclideanDistance:          euclideanGeneric,
		CosineDistance:             cosineGeneric,
		DotProduct:                 dotGeneric,
		EuclideanDistanceBatch:     euclideanBatchGeneric,
		CosineDistanceBatch:        cosineBatchGeneric,
		DotProductBatch:            dotBatchGeneric,
		EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,
		EuclideanDistance384:       euclideanGeneric,
		EuclideanDistance128:       euclideanGeneric,
	},
}

// Current dispatch - single pointer lookup instead of many
var currentDispatch *ImplementationDispatch

// initializeDispatch sets function pointers based on detected CPU features.
// This is called once at startup, removing branch overhead from hot paths.
func initializeDispatch() {
	dispatch, exists := dispatchTable[implementation]
	if !exists {
		// Fallback to generic if implementation not found
		dispatch = dispatchTable["generic"]
	}
	currentDispatch = &dispatch
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
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX512
		euclideanDistanceF16BatchImpl = euclideanF16BatchAVX512
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
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX2
		euclideanDistanceF16BatchImpl = euclideanF16BatchAVX2
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
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesGeneric
		// F16 Kernels
		// Use generic unrolled implementations for F16 to avoid flaky assembly bugs
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
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
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
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

// DispatchDistance computes the distance between two vectors using the best available kernel.
func DispatchDistance[T any](metric MetricType, a, b []T) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("simd: dimension mismatch: %d != %d", len(a), len(b))
	}
	if len(a) == 0 {
		return 0, nil
	}

	dt := GetSIMDDataType[T]()
	dims := len(a)

	kernel := Registry.Get(metric, dt, dims)
	if kernel == nil {
		return 0, fmt.Errorf("simd: no kernel found for %s/%s dims=%d", metric, dt, dims)
	}

	start := time.Now()
	defer func() {
		metrics.HNSWSimdDispatchLatency.WithLabelValues(dt.String()).Observe(time.Since(start).Seconds())
	}()

	switch k := kernel.(type) {
	case func([]T, []T) (float32, error):
		return k(a, b)
	case distanceFunc:
		if va, ok := any(a).([]float32); ok {
			vb := any(b).([]float32)
			return k(va, vb)
		}
	case distanceF16Func:
		if va, ok := any(a).([]float16.Num); ok {
			vb := any(b).([]float16.Num)
			return k(va, vb)
		}
	default:
		return 0, fmt.Errorf("simd: invalid kernel type for %s: %T", dt, kernel)
	}
	return 0, fmt.Errorf("simd: type mismatch between T and kernel for %s", dt)
}

// GetSIMDDataType returns the SIMDDataType for a given type T.
func GetSIMDDataType[T any]() SIMDDataType {
	var zero T
	switch any(zero).(type) {
	case float32:
		return DataTypeFloat32
	case float16.Num:
		return DataTypeFloat16
	case int8:
		return DataTypeInt8
	case uint8:
		return DataTypeUint8
	case int16:
		return DataTypeInt16
	case uint16:
		return DataTypeUint16
	case int32:
		return DataTypeInt32
	case uint32:
		return DataTypeUint32
	case int64:
		return DataTypeInt64
	case uint64:
		return DataTypeUint64
	case float64:
		return DataTypeFloat64
	case complex64:
		return DataTypeComplex64
	case complex128:
		return DataTypeComplex128
	}
	return DataTypeFloat32 // Default fallback
}
