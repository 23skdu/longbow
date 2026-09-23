package index

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

func getSimdMetric(m basecore.DistanceMetric) simd.MetricType {
	switch m {
	case basecore.MetricCosine:
		return simd.MetricCosine
	case basecore.MetricDotProduct:
		return simd.MetricDotProduct
	case basecore.MetricL2Squared:
		return simd.MetricL2Squared
	default:
		return simd.MetricEuclidean
	}
}

type distanceFallbacks[T any] struct {
	cosine    simd.DistanceKernel[T]
	dot       simd.DistanceKernel[T]
	l2Squared simd.DistanceKernel[T]
	euclidean simd.DistanceKernel[T]
}

func resolveDistanceKernel[T any](sm simd.MetricType, dims int, fb distanceFallbacks[T]) simd.DistanceKernel[T] {
	k := simd.GetKernel[T](sm, dims)
	if k == nil {
		switch sm {
		case simd.MetricCosine:
			return fb.cosine
		case simd.MetricDotProduct:
			if fb.dot != nil {
				return func(a, b []T) (float32, error) {
					d, err := fb.dot(a, b)
					return -d, err
				}
			}
			return nil
		case simd.MetricL2Squared:
			if fb.l2Squared != nil {
				return fb.l2Squared
			}
			return fb.euclidean
		default:
			return fb.euclidean
		}
	}
	if sm == simd.MetricDotProduct {
		return func(a, b []T) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

func resolveL2SquaredKernel[T any](dims int, fallback simd.DistanceKernel[T]) simd.DistanceKernel[T] {
	if k := simd.GetKernel[T](simd.MetricL2Squared, dims); k != nil {
		return k
	}
	return fallback
}

func (h *ArrowHNSW) resolveAllDistanceFuncs() {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())

	h.distFunc = resolveDistanceKernel[float32](sm, dims, distanceFallbacks[float32]{
		cosine: simd.CosineDistance, dot: simd.DotProduct,
		l2Squared: simd.L2SquaredFloat32, euclidean: simd.EuclideanDistance,
	})
	h.distFuncSquared = resolveL2SquaredKernel[float32](dims, simd.L2SquaredFloat32)
	h.distFuncF16 = resolveDistanceKernel[float16.Num](sm, dims, distanceFallbacks[float16.Num]{
		cosine: simd.CosineDistanceF16, dot: simd.DotProductF16,
		euclidean: simd.EuclideanDistanceF16,
	})
	h.distFuncF64 = resolveDistanceKernel[float64](sm, dims, distanceFallbacks[float64]{
		cosine: simd.CosineDistanceFloat64, dot: simd.DotProductF64,
		l2Squared: simd.L2SquaredFloat64, euclidean: simd.EuclideanDistanceFloat64,
	})
	h.distFuncC64 = resolveDistanceKernel[complex64](sm, dims, distanceFallbacks[complex64]{
		cosine: simd.CosineDistanceComplex64, dot: simd.DotProductComplex64,
		euclidean: simd.EuclideanDistanceComplex64,
	})
	h.distFuncC128 = resolveDistanceKernel[complex128](sm, dims, distanceFallbacks[complex128]{
		cosine: simd.CosineDistanceComplex128, dot: simd.DotProductComplex128,
		euclidean: simd.EuclideanDistanceComplex128,
	})
	h.distFuncInt8 = resolveDistanceKernel[int8](sm, dims, distanceFallbacks[int8]{
		cosine: simd.CosineDistanceInt8, dot: simd.DotProductInt8,
		euclidean: simd.EuclideanDistanceInt8,
	})
	h.distFuncInt8Squared = resolveL2SquaredKernel[int8](dims, nil)
	h.distFuncUint8 = resolveDistanceKernel[uint8](sm, dims, distanceFallbacks[uint8]{
		cosine: simd.CosineDistanceUint8, dot: simd.DotProductUint8,
		euclidean: simd.EuclideanDistanceUint8,
	})
	h.distFuncUint8Squared = resolveL2SquaredKernel[uint8](dims, nil)
	h.distFuncInt16 = resolveDistanceKernel[int16](sm, dims, distanceFallbacks[int16]{
		cosine: simd.CosineDistanceInt16, dot: simd.DotProductInt16,
		euclidean: simd.EuclideanDistanceInt16,
	})
	h.distFuncUint16 = resolveDistanceKernel[uint16](sm, dims, distanceFallbacks[uint16]{
		cosine: simd.CosineDistanceUint16, dot: simd.DotProductUint16,
		euclidean: simd.EuclideanDistanceUint16,
	})
	h.distFuncInt32 = resolveDistanceKernel[int32](sm, dims, distanceFallbacks[int32]{
		cosine: simd.CosineDistanceInt32, dot: simd.DotProductInt32,
		euclidean: simd.EuclideanDistanceInt32,
	})
	h.distFuncUint32 = resolveDistanceKernel[uint32](sm, dims, distanceFallbacks[uint32]{
		cosine: simd.CosineDistanceUint32, dot: simd.DotProductUint32,
		euclidean: simd.EuclideanDistanceUint32,
	})
	h.distFuncInt64 = resolveDistanceKernel[int64](sm, dims, distanceFallbacks[int64]{
		cosine: simd.CosineDistanceInt64, dot: simd.DotProductInt64,
		euclidean: simd.EuclideanDistanceInt64,
	})
	h.distFuncUint64 = resolveDistanceKernel[uint64](sm, dims, distanceFallbacks[uint64]{
		cosine: simd.CosineDistanceUint64, dot: simd.DotProductUint64,
		euclidean: simd.EuclideanDistanceUint64,
	})

	if h.navigator != nil {
		h.navigator.SetDistanceKernel(h.distFunc)
	}
}
