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

	// Specialized functions for fixed dimensions
	EuclideanDistance128  distanceFunc
	EuclideanDistance384  distanceFunc
	EuclideanDistance768  distanceFunc
	EuclideanDistance1024 distanceFunc
	EuclideanDistance1536 distanceFunc
	EuclideanDistance3072 distanceFunc

	DotProduct128  distanceFunc
	DotProduct384  distanceFunc
	DotProduct768  distanceFunc
	DotProduct1024 distanceFunc
	DotProduct1536 distanceFunc
	DotProduct3072 distanceFunc
}

// Global dispatch table - one per implementation
var dispatchTable = map[string]*ImplementationDispatch{
	"avx512": {
		EuclideanDistance:          euclideanAVX512,
		CosineDistance:             cosineAVX512,
		DotProduct:                 dotAVX512,
		EuclideanDistanceBatch:     euclideanBatchAVX512,
		CosineDistanceBatch:        cosineBatchAVX512,
		DotProductBatch:            dotBatchAVX512,
		EuclideanDistanceBatchFlat: euclideanBatchFlatAVX512,

		EuclideanDistance128:       euclidean128Unrolled4x,
		EuclideanDistance384:       euclidean384AVX512,
		EuclideanDistance768:       euclidean768AVX512,
		EuclideanDistance1024:      euclidean1024Blocked,
		EuclideanDistance1536:      euclidean1536AVX512,
		EuclideanDistance3072:      euclidean3072Blocked,

		DotProduct128:  dot128Unrolled4x,
		DotProduct384:  dot384AVX512,
		DotProduct768:  dot768AVX512,
		DotProduct1024: dotAVX512,
		DotProduct1536: dot1536AVX512,
		DotProduct3072: DotProductFloat32Blocked,
	},
	"avx2": {
		EuclideanDistance:          euclideanAVX2,
		CosineDistance:             cosineAVX2,
		DotProduct:                 dotAVX2,
		EuclideanDistanceBatch:     euclideanBatchAVX2,
		CosineDistanceBatch:        cosineBatchAVX2,
		DotProductBatch:            dotBatchAVX2,
		EuclideanDistanceBatchFlat: euclideanBatchFlatAVX2,

		EuclideanDistance128:       euclidean128Unrolled4x,
		EuclideanDistance384:       euclidean384AVX2,
		EuclideanDistance768:       euclidean768AVX2,
		EuclideanDistance1024:      euclidean1024Blocked,
		EuclideanDistance1536:      euclidean1536AVX2,
		EuclideanDistance3072:      euclidean3072Blocked,

		DotProduct128:  dot128Unrolled4x,
		DotProduct384:  dotGeneric,
		DotProduct768:  dotGeneric,
		DotProduct1024: dotAVX2,
		DotProduct1536: dotAVX2,
		DotProduct3072: DotProductFloat32Blocked,
	},
	"neon": {
		EuclideanDistance:          euclideanNEON,
		CosineDistance:             cosineNEON,
		DotProduct:                 dotNEON,
		EuclideanDistanceBatch:     euclideanBatchNEON,
		CosineDistanceBatch:        cosineBatchNEON,
		DotProductBatch:            dotBatchNEON,
		EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,

		EuclideanDistance128:       euclidean128NEON,
		EuclideanDistance384:       euclidean384NEON,
		EuclideanDistance768:       euclidean768NEON,
		EuclideanDistance1024:      euclidean1024Blocked,
		EuclideanDistance1536:      euclidean1536NEON,
		EuclideanDistance3072:      euclidean3072Blocked,

		DotProduct128:  dot128NEON,
		DotProduct384:  dot384NEON,
		DotProduct768:  dot768NEON,
		DotProduct1024: dotNEON,
		DotProduct1536: dot1536NEON,
		DotProduct3072: DotProductFloat32Blocked,
	},
	"generic": {
		EuclideanDistance:          euclideanGeneric,
		CosineDistance:             cosineGeneric,
		DotProduct:                 dotGeneric,
		EuclideanDistanceBatch:     euclideanBatchGeneric,
		CosineDistanceBatch:        cosineBatchGeneric,
		DotProductBatch:            dotBatchGeneric,
		EuclideanDistanceBatchFlat: euclideanBatchFlatGeneric,

		EuclideanDistance128:  euclidean128Unrolled4x,
		EuclideanDistance384:  euclidean384Unrolled4x,
		EuclideanDistance768:  euclidean768Unrolled4x,
		EuclideanDistance1024: euclidean1024Blocked,
		EuclideanDistance1536: euclidean1536Unrolled4x,
		EuclideanDistance3072: euclidean3072Blocked,

		DotProduct128:  dot128Unrolled4x,
		DotProduct384:  dotUnrolled4x,
		DotProduct768:  dotUnrolled4x,
		DotProduct1024: dotUnrolled4x,
		DotProduct1536: dotUnrolled4x,
		DotProduct3072: DotProductFloat32Blocked,
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
	currentDispatch = dispatch
	switch implementation {
	case "avx512":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("avx512").Inc()
		metrics.SimdStaticDispatchType.Set(3)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = euclideanBatchAVX512

		cosineDistanceBatchImpl = cosineBatchAVX512
		dotProductBatchImpl = dotBatchAVX512
		l2SquaredImpl = l2SquaredAVX512 // uses AVX512 kernel
		prefetchImpl = prefetchNTA
		matchInt64Impl = matchInt64AVX512
		matchInt32Impl = matchInt32AVX512
		matchFloat32Impl = matchFloat32AVX512
		matchFloat64Impl = matchFloat64AVX512
		if features.HasVNNI {
			adcDistanceBatchImpl = adcBatchVNNI
		} else {
			adcDistanceBatchImpl = adcBatchAVX512
		}
		euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX512
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX512
		euclideanDistanceF16BatchImpl = euclideanF16BatchAVX512
		andBytesImpl = andBytesGeneric
		euclideanDistanceF16Impl = euclideanF16AVX512
		cosineDistanceF16Impl = cosineF16AVX512
		dotProductF16Impl = dotF16AVX512
		euclideanDistanceFloat64Impl = euclideanFloat64AVX512
		dotProductFloat64Impl = dotFloat64AVX512
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x // Fallback for now
		euclideanDistanceInt8Impl = euclideanInt8AVX2
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16AVX2
		euclideanDistanceUint16Impl = euclideanUint16AVX2
		dotProductInt16Impl = dotInt16AVX2
		dotProductUint16Impl = dotUint16AVX2
		dotProductInt4Impl = dotInt4AVX512
		dotProductInt2Impl = dotInt2AVX512
		// Optimization: Use float32 AVX kernels for complex64
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
	case "avx2":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("avx2").Inc()
		metrics.SimdStaticDispatchType.Set(2)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = euclideanBatchAVX2

		cosineDistanceBatchImpl = cosineBatchAVX2
		dotProductBatchImpl = dotBatchAVX2
		l2SquaredImpl = l2SquaredAVX2 // uses AVX2 kernel (no sqrt)
		prefetchImpl = prefetchNTA
		matchInt64Impl = matchInt64AVX2
		matchInt32Impl = matchInt32AVX2
		matchFloat32Impl = matchFloat32AVX2
		matchFloat64Impl = matchFloat64AVX2
		adcDistanceBatchImpl = adcBatchAVX2
		euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX2
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX2
		euclideanDistanceF16BatchImpl = euclideanF16BatchAVX2
		andBytesImpl = andBytesAVX2
		orBytesImpl = orBytesAVX2
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosAVX2
		euclideanDistanceF16Impl = euclideanF16AVX2
		cosineDistanceF16Impl = cosineF16AVX2
		dotProductF16Impl = dotF16AVX2
		euclideanDistanceFloat64Impl = euclideanFloat64AVX2
		dotProductFloat64Impl = dotFloat64AVX2
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x // Fallback for now
		euclideanDistanceInt8Impl = euclideanInt8AVX2
		dotProductInt8Impl = dotInt8Unrolled4x
		dotProductUint8Impl = dotUint8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16AVX2
		euclideanDistanceUint16Impl = euclideanUint16AVX2
		dotProductInt16Impl = dotInt16AVX2
		dotProductUint16Impl = dotUint16AVX2
		dotProductInt4Impl = dotInt4AVX2
		dotProductInt2Impl = dotInt2AVX2
	case "neon":
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		metrics.SimdDispatchCount.WithLabelValues("neon").Inc()
		metrics.SimdStaticDispatchType.Set(1)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		dotProduct128Impl = dispatch.DotProduct128
		euclideanDistanceBatchImpl = euclideanBatchNEON
		cosineDistanceBatchImpl = cosineBatchNEON
		dotProductBatchImpl = dotBatchNEON
		l2SquaredImpl = l2SquaredNEON
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Neon
		matchInt32Impl = matchInt32Neon
		matchFloat32Impl = matchFloat32Neon
		matchFloat64Impl = matchFloat64Neon
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchNEON
		cosineDistanceBatchImpl = cosineBatchNEON
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesGeneric
		orBytesImpl = orBytesGeneric
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosGeneric
		// F16 Kernels
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
		euclideanDistanceFloat64Impl = euclideanFloat64Unrolled4x
		dotProductFloat64Impl = dotFloat64Unrolled4x
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
		euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
		dotProductInt16Impl = dotInt16Unrolled4x
		dotProductUint16Impl = dotUint16Unrolled4x
		dotProductInt4Impl = dotInt4Neon
		dotProductInt2Impl = dotInt2Neon
	default:
		euclideanDistanceImpl = dispatch.EuclideanDistance
		euclideanDistance128Impl = dispatch.EuclideanDistance128
		euclideanDistance384Impl = dispatch.EuclideanDistance384
		euclideanDistance768Impl = dispatch.EuclideanDistance768
		euclideanDistance1024Impl = dispatch.EuclideanDistance1024
		euclideanDistance1536Impl = dispatch.EuclideanDistance1536
		euclideanDistance3072Impl = dispatch.EuclideanDistance3072
		metrics.SimdDispatchCount.WithLabelValues("generic").Inc()
		metrics.SimdStaticDispatchType.Set(0)
		cosineDistanceImpl = dispatch.CosineDistance
		dotProductImpl = dispatch.DotProduct
		dotProduct128Impl = dispatch.DotProduct128
		dotProduct384Impl = dispatch.DotProduct384
		dotProduct768Impl = dispatch.DotProduct768
		dotProduct1024Impl = dispatch.DotProduct1024
		dotProduct1536Impl = dispatch.DotProduct1536
		dotProduct3072Impl = dispatch.DotProduct3072
		euclideanDistanceBatchImpl = euclideanBatchUnrolled4x
		cosineDistanceBatchImpl = cosineBatchUnrolled4x
		dotProductBatchImpl = dotBatchUnrolled4x
		l2SquaredImpl = L2SquaredFloat32
		prefetchImpl = prefetchGeneric
		matchInt64Impl = matchInt64Generic
		matchInt32Impl = matchInt32Generic
		matchFloat32Impl = matchFloat32Generic
		matchFloat64Impl = matchFloat64Generic
		adcDistanceBatchImpl = adcBatchGeneric
		euclideanDistanceVerticalBatchImpl = euclideanBatchGeneric
		euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
		euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
		andBytesImpl = andBytesGeneric
		orBytesImpl = orBytesGeneric
		notBytesImpl = notBytesGeneric
		isAllZerosImpl = isAllZerosGeneric
		euclideanDistanceF16Impl = euclideanF16Unrolled4x
		cosineDistanceF16Impl = cosineF16Unrolled4x
		dotProductF16Impl = dotF16Unrolled4x
		euclideanDistanceComplex64Impl = euclideanComplex64Optimized
		euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
		euclideanDistanceFloat64Impl = euclideanFloat64Unrolled4x
		cosineDistanceFloat64Impl = cosineFloat64Unrolled4x
		euclideanDistanceInt8Impl = euclideanInt8Unrolled4x
		euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
		euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
		dotProductInt16Impl = dotInt16Unrolled4x
		dotProductUint16Impl = dotUint16Unrolled4x
		dotProductInt4Impl = dotInt4Generic
		dotProductInt2Impl = dotInt2Generic
	}

	// Register current implementations into the new dynamic registry.
	// This enables the transition to polymorphic indexing while preserving
	// existing high-performance paths.

	// Float32 Euclidean
	Registry.Register(MetricEuclidean, DataTypeFloat32, 0, euclideanDistanceImpl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 128, euclideanDistance128Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 384, euclideanDistance384Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 768, euclideanDistance768Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1024, euclideanDistance1024Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 1536, euclideanDistance1536Impl)
	Registry.Register(MetricEuclidean, DataTypeFloat32, 3072, euclideanDistance3072Impl)

	// Float32 Cosine & Dot Product
	Registry.Register(MetricCosine, DataTypeFloat32, 0, cosineDistanceImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 0, dotProductImpl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 128, dotProduct128Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 384, dotProduct384Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 768, dotProduct768Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1024, dotProduct1024Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 1536, dotProduct1536Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat32, 3072, dotProduct3072Impl)

	// Float16 (Support both native and unrolled paths)
	Registry.Register(MetricEuclidean, DataTypeFloat16, 0, euclideanDistanceF16Impl)
	Registry.Register(MetricCosine, DataTypeFloat16, 0, cosineDistanceF16Impl)
	Registry.Register(MetricDotProduct, DataTypeFloat16, 0, dotProductF16Impl)

	// Complex Numbers (Unrolled Baselines)
	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanDistanceComplex64Impl)
	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanDistanceComplex128Impl)

	// Baseline Fallbacks for all other types
	Registry.Register(MetricEuclidean, DataTypeInt8, 0, euclideanDistanceInt8Impl)
	Registry.Register(MetricCosine, DataTypeInt8, 0, CosineDistanceInt8)
	Registry.Register(MetricDotProduct, DataTypeInt8, 0, dotProductInt8Impl)

	Registry.Register(MetricEuclidean, DataTypeInt16, 0, euclideanDistanceInt16Impl)
	Registry.Register(MetricCosine, DataTypeInt16, 0, CosineDistanceInt16)
	Registry.Register(MetricDotProduct, DataTypeInt16, 0, dotProductInt16Impl)

	Registry.Register(MetricEuclidean, DataTypeInt32, 0, euclideanInt32Unrolled4x)
	Registry.Register(MetricCosine, DataTypeInt32, 0, CosineDistanceInt32)
	Registry.Register(MetricDotProduct, DataTypeInt32, 0, dotInt32Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeInt64, 0, euclideanInt64Unrolled4x)
	Registry.Register(MetricCosine, DataTypeInt64, 0, CosineDistanceInt64)
	Registry.Register(MetricDotProduct, DataTypeInt64, 0, dotInt64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeUint8, 0, euclideanDistanceUint8Impl)
	Registry.Register(MetricCosine, DataTypeUint8, 0, CosineDistanceUint8)
	Registry.Register(MetricDotProduct, DataTypeUint8, 0, dotProductUint8Impl)

	Registry.Register(MetricEuclidean, DataTypeUint16, 0, euclideanDistanceUint16Impl)
	Registry.Register(MetricCosine, DataTypeUint16, 0, CosineDistanceUint16)
	Registry.Register(MetricDotProduct, DataTypeUint16, 0, dotProductUint16Impl)

	Registry.Register(MetricEuclidean, DataTypeUint32, 0, euclideanUint32Unrolled4x)
	Registry.Register(MetricCosine, DataTypeUint32, 0, CosineDistanceUint32)
	Registry.Register(MetricDotProduct, DataTypeUint32, 0, dotUint32Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeUint64, 0, euclideanUint64Unrolled4x)
	Registry.Register(MetricCosine, DataTypeUint64, 0, CosineDistanceUint64)
	Registry.Register(MetricDotProduct, DataTypeUint64, 0, dotUint64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeFloat64, 0, euclideanFloat64Unrolled4x)
	Registry.Register(MetricCosine, DataTypeFloat64, 0, cosineFloat64Unrolled4x)
	Registry.Register(MetricDotProduct, DataTypeFloat64, 0, dotFloat64Unrolled4x)

	Registry.Register(MetricEuclidean, DataTypeComplex64, 0, euclideanComplex64Unrolled)
	Registry.Register(MetricCosine, DataTypeComplex64, 0, CosineDistanceComplex64)
	Registry.Register(MetricDotProduct, DataTypeComplex64, 0, dotComplex64Unrolled)

	Registry.Register(MetricEuclidean, DataTypeComplex128, 0, euclideanComplex128Unrolled)
	Registry.Register(MetricCosine, DataTypeComplex128, 0, CosineDistanceComplex128)
	Registry.Register(MetricDotProduct, DataTypeComplex128, 0, dotComplex128Unrolled)
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
