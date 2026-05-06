package core

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Distance function resolvers for all supported vector types.
// These resolve to the best available SIMD kernels from the internal/simd package.

func getSimdMetric(m basecore.DistanceMetric) simd.MetricType {
	switch m {
	case basecore.MetricCosine:
		return simd.MetricCosine
	case basecore.MetricDotProduct:
		return simd.MetricDotProduct
	default:
		return simd.MetricEuclidean
	}
}

// resolveAllDistanceFuncs resolves and caches all typed distance functions.
func (h *ArrowHNSW) resolveAllDistanceFuncs() {
	h.distFunc = h.resolveDistanceFunc()
	h.distFuncF64 = h.resolveDistanceFuncF64()
	h.distFuncF16 = h.resolveDistanceFuncF16()
	h.distFuncC64 = h.resolveDistanceFuncC64()
	h.distFuncC128 = h.resolveDistanceFuncC128()
	h.distFuncInt8 = h.resolveDistanceFuncInt8()
	h.distFuncUint8 = h.resolveDistanceFuncUint8()
	h.distFuncInt16 = h.resolveDistanceFuncInt16()
	h.distFuncUint16 = h.resolveDistanceFuncUint16()
	h.distFuncInt32 = h.resolveDistanceFuncInt32()
	h.distFuncUint32 = h.resolveDistanceFuncUint32()
	h.distFuncInt64 = h.resolveDistanceFuncInt64()
	h.distFuncUint64 = h.resolveDistanceFuncUint64()
	
	// Sync with navigator
	if h.navigator != nil {
		h.navigator.SetDistanceKernel(h.distFunc)
	}
}

// resolveDistanceFunc returns the appropriate distance function for float32 vectors.
func (h *ArrowHNSW) resolveDistanceFunc() func(a, b []float32) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[float32](sm, dims)
	
	if k == nil {
		// Fallback to standard dispatch if no specialized kernel found
		return simd.EuclideanDistance
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []float32) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncF16 returns the FP16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF16() func(a, b []float16.Num) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[float16.Num](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceF16
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []float16.Num) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncF64 returns the Float64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF64() func(a, b []float64) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[float64](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceFloat64
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []float64) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncC64 returns the Complex64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC64() func(a, b []complex64) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[complex64](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceComplex64
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []complex64) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncC128 returns the Complex128 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC128() func(a, b []complex128) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[complex128](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceComplex128
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []complex128) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncInt8 returns the Int8 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt8() func(a, b []int8) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[int8](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceInt8
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []int8) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncUint8 returns the Uint8 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint8() func(a, b []uint8) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[uint8](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceUint8
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []uint8) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncInt16 returns the Int16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt16() func(a, b []int16) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[int16](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceInt16
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []int16) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncUint16 returns the Uint16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint16() func(a, b []uint16) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[uint16](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceUint16
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []uint16) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncInt32 returns the Int32 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt32() func(a, b []int32) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[int32](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceInt32
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []int32) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncUint32 returns the Uint32 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint32() func(a, b []uint32) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[uint32](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceUint32
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []uint32) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncInt64 returns the Int64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt64() func(a, b []int64) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[int64](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceInt64
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []int64) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

// resolveDistanceFuncUint64 returns the Uint64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint64() func(a, b []uint64) (float32, error) {
	sm := getSimdMetric(h.config.Metric)
	dims := int(h.dims.Load())
	k := simd.GetKernel[uint64](sm, dims)

	if k == nil {
		return simd.EuclideanDistanceUint64
	}

	if sm == simd.MetricDotProduct {
		return func(a, b []uint64) (float32, error) {
			d, err := k(a, b)
			return -d, err
		}
	}
	return k
}

