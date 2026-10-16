package index

import (
	"fmt"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// ComputeDistanceAny computes the distance between two vectors of matching arbitrary type.
func (h *ArrowHNSW) ComputeDistanceAny(v1, v2 any) (float32, error) {
	if v1 == nil || v2 == nil {
		return 0, fmt.Errorf("nil vector")
	}
	switch a := v1.(type) {
	case []float32:
		if b, ok := v2.([]float32); ok {
			if h.distFunc != nil {
				return h.distFunc(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []float64:
		if b, ok := v2.([]float64); ok {
			if h.distFuncF64 != nil {
				return h.distFuncF64(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []int8:
		if b, ok := v2.([]int8); ok {
			if h.distFuncInt8 != nil {
				return h.distFuncInt8(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []uint8:
		if b, ok := v2.([]uint8); ok {
			if h.distFuncUint8 != nil {
				return h.distFuncUint8(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []float16.Num:
		if b, ok := v2.([]float16.Num); ok {
			if h.distFuncF16 != nil {
				return h.distFuncF16(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []int16:
		if b, ok := v2.([]int16); ok {
			if h.distFuncInt16 != nil {
				return h.distFuncInt16(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []uint16:
		if b, ok := v2.([]uint16); ok {
			if h.distFuncUint16 != nil {
				return h.distFuncUint16(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []int32:
		if b, ok := v2.([]int32); ok {
			if h.distFuncInt32 != nil {
				return h.distFuncInt32(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []uint32:
		if b, ok := v2.([]uint32); ok {
			if h.distFuncUint32 != nil {
				return h.distFuncUint32(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []int64:
		if b, ok := v2.([]int64); ok {
			if h.distFuncInt64 != nil {
				return h.distFuncInt64(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []uint64:
		if b, ok := v2.([]uint64); ok {
			if h.distFuncUint64 != nil {
				return h.distFuncUint64(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []complex64:
		if b, ok := v2.([]complex64); ok {
			if h.distFuncC64 != nil {
				return h.distFuncC64(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	case []complex128:
		if b, ok := v2.([]complex128); ok {
			if h.distFuncC128 != nil {
				return h.distFuncC128(a, b)
			}
			return simd.DispatchDistance(simd.MetricEuclidean, a, b)
		}
	}
	return 0, fmt.Errorf("unsupported or mismatched vector types: %T and %T", v1, v2)
}
