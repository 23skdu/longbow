package core

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// Distance function resolvers for all supported vector types.
// These resolve to the best available SIMD kernels from the internal/simd package.

// resolveDistanceFunc returns the appropriate distance function for float32 vectors.
func (h *ArrowHNSW) resolveDistanceFunc() func(a, b []float32) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistance
	case basecore.MetricDotProduct:
		return func(a, b []float32) (float32, error) {
			d, err := simd.DotProduct(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistance
	}
}

// resolveDistanceFuncF16 returns the FP16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF16() func(a, b []float16.Num) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceF16
	case basecore.MetricDotProduct:
		return func(a, b []float16.Num) (float32, error) {
			d, err := simd.DotProductF16(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceF16
	}
}

// resolveDistanceFuncF64 returns the Float64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncF64() func(a, b []float64) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceFloat64
	case basecore.MetricDotProduct:
		return func(a, b []float64) (float32, error) {
			d, err := simd.DotProductF64(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceFloat64
	}
}

// resolveDistanceFuncC64 returns the Complex64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC64() func(a, b []complex64) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceComplex64
	case basecore.MetricDotProduct:
		return func(a, b []complex64) (float32, error) {
			d, err := simd.DotProductComplex64(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceComplex64
	}
}

// resolveDistanceFuncC128 returns the Complex128 distance function.
func (h *ArrowHNSW) resolveDistanceFuncC128() func(a, b []complex128) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceComplex128
	case basecore.MetricDotProduct:
		return func(a, b []complex128) (float32, error) {
			d, err := simd.DotProductComplex128(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceComplex128
	}
}

// resolveDistanceFuncInt8 returns the Int8 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt8() func(a, b []int8) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceInt8
	case basecore.MetricDotProduct:
		return func(a, b []int8) (float32, error) {
			d, err := simd.DotProductInt8(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceInt8
	}
}

// resolveDistanceFuncUint8 returns the Uint8 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint8() func(a, b []uint8) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceUint8
	case basecore.MetricDotProduct:
		return func(a, b []uint8) (float32, error) {
			d, err := simd.DotProductUint8(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceUint8
	}
}

// resolveDistanceFuncInt16 returns the Int16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt16() func(a, b []int16) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceInt16
	case basecore.MetricDotProduct:
		return func(a, b []int16) (float32, error) {
			d, err := simd.DotProductInt16(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceInt16
	}
}

// resolveDistanceFuncUint16 returns the Uint16 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint16() func(a, b []uint16) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceUint16
	case basecore.MetricDotProduct:
		return func(a, b []uint16) (float32, error) {
			d, err := simd.DotProductUint16(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceUint16
	}
}

// resolveDistanceFuncInt32 returns the Int32 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt32() func(a, b []int32) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceInt32
	case basecore.MetricDotProduct:
		return func(a, b []int32) (float32, error) {
			d, err := simd.DotProductInt32(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceInt32
	}
}

// resolveDistanceFuncUint32 returns the Uint32 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint32() func(a, b []uint32) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceUint32
	case basecore.MetricDotProduct:
		return func(a, b []uint32) (float32, error) {
			d, err := simd.DotProductUint32(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceUint32
	}
}

// resolveDistanceFuncInt64 returns the Int64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncInt64() func(a, b []int64) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceInt64
	case basecore.MetricDotProduct:
		return func(a, b []int64) (float32, error) {
			d, err := simd.DotProductInt64(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceInt64
	}
}

// resolveDistanceFuncUint64 returns the Uint64 distance function.
func (h *ArrowHNSW) resolveDistanceFuncUint64() func(a, b []uint64) (float32, error) {
	switch h.config.Metric {
	case basecore.MetricCosine:
		return simd.CosineDistanceUint64
	case basecore.MetricDotProduct:
		return func(a, b []uint64) (float32, error) {
			d, err := simd.DotProductUint64(a, b)
			return -d, err
		}
	default: // Euclidean
		return simd.EuclideanDistanceUint64
	}
}
