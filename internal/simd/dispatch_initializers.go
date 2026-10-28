package simd

import (
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// applyDispatchToGlobalPointers binds active dispatch methods to package-level function pointers.
func applyDispatchToGlobalPointers(dispatch *ImplementationDispatch) {
	euclideanDistanceImpl = dispatch.EuclideanDistance
	euclideanDistance128Impl = dispatch.EuclideanDistance128
	euclideanDistance384Impl = dispatch.EuclideanDistance384
	euclideanDistance768Impl = dispatch.EuclideanDistance768
	euclideanDistance1024Impl = dispatch.EuclideanDistance1024
	euclideanDistance1536Impl = dispatch.EuclideanDistance1536
	euclideanDistance3072Impl = dispatch.EuclideanDistance3072

	cosineDistanceImpl = dispatch.CosineDistance
	dotProductImpl = dispatch.DotProduct
	dotProduct128Impl = dispatch.DotProduct128
	dotProduct384Impl = dispatch.DotProduct384
	dotProduct768Impl = dispatch.DotProduct768
	dotProduct1024Impl = dispatch.DotProduct1024
	dotProduct1536Impl = dispatch.DotProduct1536
	dotProduct3072Impl = dispatch.DotProduct3072

	euclideanDistanceBatchImpl = dispatch.EuclideanDistanceBatch
	cosineDistanceBatchImpl = dispatch.CosineDistanceBatch
	dotProductBatchImpl = dispatch.DotProductBatch
	l2SquaredImpl = dispatch.L2SquaredDistance

	euclideanDistanceF16Impl = dispatch.EuclideanDistanceF16
	cosineDistanceF16Impl = dispatch.CosineDistanceF16
	dotProductF16Impl = dispatch.DotProductF16

	int8ToFloat32Impl = dispatch.Int8ToFloat32
	uint8ToFloat32Impl = dispatch.Uint8ToFloat32
	int16ToFloat32Impl = dispatch.Int16ToFloat32
	uint16ToFloat32Impl = dispatch.Uint16ToFloat32
	int32ToFloat32Impl = dispatch.Int32ToFloat32
	uint32ToFloat32Impl = dispatch.Uint32ToFloat32
	float16ToFloat32Impl = dispatch.Float16ToFloat32

	sigmoidFloat32Impl = dispatch.Sigmoid
	softmaxFloat32Impl = dispatch.Softmax
	expFloat32Impl = dispatch.Exp
	logFloat32Impl = dispatch.Log

	sumFloat32Impl = dispatch.Sum
	maxFloat32Impl = dispatch.Max
	minFloat32Impl = dispatch.Min
	matMulFloat32Impl = dispatch.MatMul
	sinFloat32Impl = dispatch.Sin
	cosFloat32Impl = dispatch.Cos
	sincosFloat32Impl = dispatch.Sincos
	sqrtFloat32Impl = dispatch.Sqrt
	atan2Float32Impl = dispatch.Atan2
	argMaxFloat32Impl = dispatch.ArgMax
	argMinFloat32Impl = dispatch.ArgMin
	manhattanDistanceImpl = dispatch.ManhattanDistance
	chebyshevDistanceImpl = dispatch.ChebyshevDistance
	brayCurtisDistanceImpl = dispatch.BrayCurtisDistance
	accumulateWeightedScatterFloat32Impl = dispatch.AccumulateWeightedScatter
	haversineBatchImpl = dispatch.HaversineBatch
	unpackTQ2Impl = dispatch.UnpackTQ2
	unpackTQ4Impl = dispatch.UnpackTQ4
	unpackTQ8Impl = dispatch.UnpackTQ8
	packTQ2Impl = dispatch.PackTQ2
	packTQ4Impl = dispatch.PackTQ4
	packTQ8Impl = dispatch.PackTQ8
	turboQuantDistanceBatchImpl = dispatch.TurboQuantDistanceBatch
}

// applyAVX512Auxiliary sets auxiliary kernels shared across AVX-512 and AMX tiers.
func applyAVX512Auxiliary() {
	prefetchImpl = func(p unsafe.Pointer) { prefetchNTA(uintptr(p)) }
	memcpyNTAImpl = memcpyGeneric
	matchInt64Impl = matchInt64AVX512
	matchInt32Impl = matchInt32AVX512
	matchFloat32Impl = matchFloat32AVX512
	matchFloat64Impl = matchFloat64AVX512
	adcDistanceBatchImpl = adcBatchAVX512
	euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX512
	euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX512
	euclideanDistanceF16BatchImpl = euclideanF16BatchAVX512
	andBytesImpl = andBytesAVX512
	orBytesImpl = orBytesAVX512
	notBytesImpl = notBytesGeneric
	isAllZerosImpl = isAllZerosAVX512
	euclideanDistanceFloat64Impl = euclideanFloat64AVX512
	dotProductFloat64Impl = dotFloat64AVX512
	l2SquaredFloat64Impl = l2SquaredFloat64AVX512
	cosineDistanceFloat64Impl = cosineFloat64AVX512
	euclideanDistanceInt8Impl = euclideanInt8AVX512
	l2SquaredInt8Impl = l2SquaredInt8AVX512
	dotProductInt8Impl = dotInt8AVX512
	dotProductUint8Impl = dotUint8AVX512
	euclideanDistanceUint8Impl = euclideanUint8AVX512
	l2SquaredUint8Impl = l2SquaredUint8AVX512
	euclideanDistanceInt16Impl = euclideanInt16AVX512
	euclideanDistanceUint16Impl = euclideanUint16AVX512
	dotProductInt16Impl = dotInt16AVX512
	dotProductUint16Impl = dotUint16AVX512
	cosineDistanceInt8Impl = cosineInt8AVX512
	cosineDistanceUint8Impl = cosineUint8AVX512
	cosineDistanceInt16Impl = cosineInt16AVX2
	cosineDistanceUint16Impl = cosineUint16AVX2
	euclideanDistanceInt32Impl = euclideanInt32AVX2
	dotProductInt32Impl = dotInt32AVX2
	cosineDistanceInt32Impl = cosineInt32AVX2
	euclideanDistanceUint32Impl = euclideanUint32AVX2
	dotProductUint32Impl = dotUint32AVX2
	cosineDistanceUint32Impl = cosineUint32AVX2
	euclideanDistanceInt64Impl = euclideanInt64Unrolled4x
	dotProductInt64Impl = dotInt64Unrolled4x
	cosineDistanceInt64Impl = cosineInt64AVX2
	euclideanDistanceUint64Impl = euclideanUint64Unrolled4x
	dotProductUint64Impl = dotUint64Unrolled4x
	cosineDistanceUint64Impl = cosineUint64AVX2
	dotProductInt4Impl = dotInt4AVX512
	dotProductInt2Impl = dotInt2AVX512
	euclideanDistanceComplex64Impl = euclideanComplex64Optimized
	euclideanDistanceComplex128Impl = euclideanComplex128Optimized
}

// applyAVX2Auxiliary sets auxiliary kernels for AVX2 tier.
func applyAVX2Auxiliary() {
	prefetchImpl = func(p unsafe.Pointer) { prefetchNTA(uintptr(p)) }
	memcpyNTAImpl = memcpyGeneric
	matchInt64Impl = matchInt64Generic
	matchInt32Impl = matchInt32Generic
	matchFloat32Impl = matchFloat32Generic
	matchFloat64Impl = matchFloat64Generic
	adcDistanceBatchImpl = adcBatchAVX2
	euclideanDistanceVerticalBatchImpl = euclideanVerticalBatchAVX2
	euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchAVX2
	euclideanDistanceF16BatchImpl = euclideanF16BatchAVX2
	andBytesImpl = andBytesAVX2
	orBytesImpl = orBytesAVX2
	notBytesImpl = notBytesGeneric
	isAllZerosImpl = isAllZerosAVX2
	euclideanDistanceFloat64Impl = euclideanFloat64AVX2
	dotProductFloat64Impl = dotFloat64AVX2
	l2SquaredFloat64Impl = l2SquaredFloat64AVX2
	cosineDistanceFloat64Impl = cosineFloat64AVX2
	euclideanDistanceInt8Impl = euclideanInt8AVX2
	l2SquaredInt8Impl = l2SquaredInt8Unrolled4x
	dotProductInt8Impl = dotInt8AVX2
	dotProductUint8Impl = dotUint8AVX2
	euclideanDistanceUint8Impl = euclideanUint8AVX2
	l2SquaredUint8Impl = l2SquaredUint8Unrolled4x
	euclideanDistanceInt16Impl = euclideanInt16AVX2
	euclideanDistanceUint16Impl = euclideanUint16AVX2
	dotProductInt16Impl = dotInt16AVX2
	dotProductUint16Impl = dotUint16AVX2
	cosineDistanceInt8Impl = cosineInt8AVX2
	cosineDistanceUint8Impl = cosineUint8AVX2
	cosineDistanceInt16Impl = cosineInt16AVX2
	cosineDistanceUint16Impl = cosineUint16AVX2
	euclideanDistanceInt32Impl = euclideanInt32AVX2
	dotProductInt32Impl = dotInt32AVX2
	cosineDistanceInt32Impl = cosineInt32AVX2
	euclideanDistanceUint32Impl = euclideanUint32AVX2
	dotProductUint32Impl = dotUint32AVX2
	cosineDistanceUint32Impl = cosineUint32AVX2
	euclideanDistanceInt64Impl = euclideanInt64Unrolled4x
	dotProductInt64Impl = dotInt64Unrolled4x
	cosineDistanceInt64Impl = cosineInt64AVX2
	euclideanDistanceUint64Impl = euclideanUint64Unrolled4x
	dotProductUint64Impl = dotUint64Unrolled4x
	cosineDistanceUint64Impl = cosineUint64AVX2
	dotProductInt4Impl = dotInt4Generic
	dotProductInt2Impl = dotInt2Generic
	euclideanDistanceComplex64Impl = euclideanComplex64Optimized
	euclideanDistanceComplex128Impl = euclideanComplex128Optimized
}

// applyNEONAuxiliary sets auxiliary kernels for ARM NEON tier.
func applyNEONAuxiliary(dispatch *ImplementationDispatch) {
	prefetchImpl = prefetchGeneric
	matchInt64Impl = matchInt64Generic
	matchInt32Impl = matchInt32Generic
	matchFloat32Impl = matchFloat32Generic
	matchFloat64Impl = matchFloat64Generic
	adcDistanceBatchImpl = adcBatchGeneric
	euclideanDistanceVerticalBatchImpl = dispatch.EuclideanDistanceBatch
	cosineDistanceBatchImpl = dispatch.CosineDistanceBatch
	euclideanDistanceSQ8BatchImpl = euclideanSQ8BatchGeneric
	euclideanDistanceF16BatchImpl = euclideanF16BatchGeneric
	andBytesImpl = andBytesGeneric
	orBytesImpl = orBytesGeneric
	notBytesImpl = notBytesGeneric
	isAllZerosImpl = isAllZerosGeneric
	euclideanDistanceComplex64Impl = euclideanComplex64Optimized
	euclideanDistanceComplex128Impl = euclideanComplex128Unrolled
	euclideanDistanceFloat64Impl = euclideanFloat64NEON
	dotProductFloat64Impl = dotFloat64NEON
	l2SquaredFloat64Impl = l2SquaredFloat64Unrolled4x
	cosineDistanceFloat64Impl = cosineFloat64NEON
	euclideanDistanceInt8Impl = euclideanInt8NEON
	l2SquaredInt8Impl = l2SquaredInt8Unrolled4x
	cosineDistanceInt8Impl = cosineInt8NEON
	cosineDistanceUint8Impl = cosineDistanceUint8Unrolled4x
	cosineDistanceInt16Impl = cosineDistanceInt16Unrolled4x
	cosineDistanceUint16Impl = cosineDistanceUint16Unrolled4x
	euclideanDistanceInt32Impl = euclideanInt32Unrolled4x
	dotProductInt32Impl = dotInt32Unrolled4x
	cosineDistanceInt32Impl = cosineDistanceInt32Unrolled4x
	euclideanDistanceUint32Impl = euclideanUint32Unrolled4x
	dotProductUint32Impl = dotUint32Unrolled4x
	cosineDistanceUint32Impl = cosineDistanceUint32Unrolled4x
	euclideanDistanceInt64Impl = euclideanInt64Unrolled4x
	dotProductInt64Impl = dotInt64Unrolled4x
	cosineDistanceInt64Impl = cosineDistanceInt64Unrolled4x
	euclideanDistanceUint64Impl = euclideanUint64Unrolled4x
	dotProductUint64Impl = dotUint64Unrolled4x
	cosineDistanceUint64Impl = cosineDistanceUint64Unrolled4x
	euclideanDistanceUint8Impl = euclideanUint8NEON
	l2SquaredUint8Impl = l2SquaredUint8Unrolled4x
	euclideanDistanceInt16Impl = euclideanInt16NEON
	euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
	dotProductInt8Impl = dotInt8NEON
	dotProductUint8Impl = dotUint8NEON
	dotProductInt16Impl = dotInt16NEON
	dotProductUint16Impl = dotUint16Unrolled4x
	dotProductInt4Impl = dotInt4Neon
	dotProductInt2Impl = dotInt2Neon
	memcpyNTAImpl = memcpyNEON
}

// applyGenericAuxiliary sets auxiliary kernels for generic fallback.
func applyGenericAuxiliary() {
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
	l2SquaredInt8Impl = l2SquaredInt8Unrolled4x
	euclideanDistanceUint8Impl = euclideanUint8Unrolled4x
	l2SquaredUint8Impl = l2SquaredUint8Unrolled4x
	euclideanDistanceInt16Impl = euclideanInt16Unrolled4x
	euclideanDistanceUint16Impl = euclideanUint16Unrolled4x
	dotProductInt8Impl = dotInt8Unrolled4x
	dotProductUint8Impl = dotUint8Unrolled4x
	dotProductInt16Impl = dotInt16Unrolled4x
	dotProductUint16Impl = dotUint16Unrolled4x
	cosineDistanceInt8Impl = cosineDistanceInt8Unrolled4x
	cosineDistanceUint8Impl = cosineDistanceUint8Unrolled4x
	cosineDistanceInt16Impl = cosineDistanceInt16Unrolled4x
	cosineDistanceUint16Impl = cosineDistanceUint16Unrolled4x
	euclideanDistanceInt32Impl = euclideanInt32Unrolled4x
	dotProductInt32Impl = dotInt32Unrolled4x
	cosineDistanceInt32Impl = cosineDistanceInt32Unrolled4x
	euclideanDistanceUint32Impl = euclideanUint32Unrolled4x
	dotProductUint32Impl = dotUint32Unrolled4x
	cosineDistanceUint32Impl = cosineDistanceUint32Unrolled4x
	euclideanDistanceInt64Impl = euclideanInt64Unrolled4x
	dotProductInt64Impl = dotInt64Unrolled4x
	cosineDistanceInt64Impl = cosineDistanceInt64Unrolled4x
	euclideanDistanceUint64Impl = euclideanUint64Unrolled4x
	dotProductUint64Impl = dotUint64Unrolled4x
	cosineDistanceUint64Impl = cosineDistanceUint64Unrolled4x
	dotProductInt4Impl = dotInt4Generic
	dotProductInt2Impl = dotInt2Generic
	memcpyNTAImpl = memcpyGeneric
}

// configureTierInitializers sets metrics and auxiliary pointers according to selected tier.
func configureTierInitializers(impl string, dispatch *ImplementationDispatch) {
	applyDispatchToGlobalPointers(dispatch)

	switch impl {
	case "emerald":
		metrics.SimdDispatchCount.WithLabelValues("emerald").Inc()
		metrics.SimdStaticDispatchType.Set(4)
		applyAVX512Auxiliary()
	case "granite":
		metrics.SimdDispatchCount.WithLabelValues("granite").Inc()
		metrics.SimdStaticDispatchType.Set(5)
		applyAVX512Auxiliary()
	case "avx512":
		metrics.SimdDispatchCount.WithLabelValues("avx512").Inc()
		metrics.SimdStaticDispatchType.Set(3)
		applyAVX512Auxiliary()
	case "avx2":
		metrics.SimdDispatchCount.WithLabelValues("avx2").Inc()
		metrics.SimdStaticDispatchType.Set(2)
		applyAVX2Auxiliary()
	case "neon":
		metrics.SimdDispatchCount.WithLabelValues("neon").Inc()
		metrics.SimdStaticDispatchType.Set(1)
		applyNEONAuxiliary(dispatch)
	default:
		metrics.SimdDispatchCount.WithLabelValues("generic").Inc()
		metrics.SimdStaticDispatchType.Set(0)
		applyGenericAuxiliary()
	}
}
