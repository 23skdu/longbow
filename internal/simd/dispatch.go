package simd

import (
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

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
	case func([]T, []T) float32:
		return k(a, b), nil
	case distanceFunc:
		if va, ok := any(a).([]float32); ok {
			vb := any(b).([]float32)
			return k(va, vb), nil
		}
	case distanceF16Func:
		if va, ok := any(a).([]float16.Num); ok {
			vb := any(b).([]float16.Num)
			return k(va, vb), nil
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
