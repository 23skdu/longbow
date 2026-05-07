package simd

import (
	"sync"
)

// MetricType defines the distance metric to use.
type MetricType int

const (
	// MetricEuclidean uses the L2 norm (square root of sum of squares).
	MetricEuclidean MetricType = iota
	// MetricCosine uses 1 - cosine similarity.
	MetricCosine
	// MetricDotProduct uses the sum of products of corresponding elements.
	MetricDotProduct
	// MetricManhattan uses the L1 norm (sum of absolute differences).
	MetricManhattan
	// MetricChebyshev uses the L-infinity norm (maximum absolute difference).
	MetricChebyshev
	// MetricBrayCurtis uses the Bray-Curtis dissimilarity.
	MetricBrayCurtis
	// MetricL2Squared uses the squared Euclidean distance.
	MetricL2Squared
)

func (m MetricType) String() string {
	switch m {
	case MetricEuclidean:
		return "euclidean"
	case MetricCosine:
		return "cosine"
	case MetricDotProduct:
		return "dot"
	case MetricManhattan:
		return "manhattan"
	case MetricChebyshev:
		return "chebyshev"
	case MetricBrayCurtis:
		return "braycurtis"
	case MetricL2Squared:
		return "l2_squared"
	default:
		return "unknown"
	}
}

// DataType is a local redeclaration of VectorDataType to avoid circular imports.
// It must stay in sync with the mapping in internal/store if used there.
type DataType int

const (
	// DataTypeFloat32 represents IEEE 754 single-precision floating point.
	DataTypeFloat32 DataType = iota
	// DataTypeFloat16 represents IEEE 754 half-precision floating point.
	DataTypeFloat16
	// DataTypeInt8 represents 8-bit signed integer.
	DataTypeInt8
	// DataTypeUint8 represents 8-bit unsigned integer.
	DataTypeUint8
	// DataTypeInt16 represents 16-bit signed integer.
	DataTypeInt16
	// DataTypeUint16 represents 16-bit unsigned integer.
	DataTypeUint16
	// DataTypeInt32 represents 32-bit signed integer.
	DataTypeInt32
	// DataTypeUint32 represents 32-bit unsigned integer.
	DataTypeUint32
	// DataTypeInt64 represents 64-bit signed integer.
	DataTypeInt64
	// DataTypeUint64 represents 64-bit unsigned integer.
	DataTypeUint64
	// DataTypeFloat64 represents IEEE 754 double-precision floating point.
	DataTypeFloat64
	// DataTypeComplex64 represents complex number with two float32s.
	DataTypeComplex64
	// DataTypeComplex128 represents complex number with two float64s.
	DataTypeComplex128
	// DataTypeInt4 represents 4-bit packed signed integer.
	DataTypeInt4
	// DataTypeInt2 represents 2-bit packed signed integer.
	DataTypeInt2
)

func (d DataType) String() string {
	switch d {
	case DataTypeInt8:
		return "int8"
	case DataTypeUint8:
		return "uint8"
	case DataTypeInt16:
		return "int16"
	case DataTypeUint16:
		return "uint16"
	case DataTypeInt32:
		return "int32"
	case DataTypeUint32:
		return "uint32"
	case DataTypeInt64:
		return "int64"
	case DataTypeUint64:
		return "uint64"
	case DataTypeFloat32:
		return "float32"
	case DataTypeFloat16:
		return "float16"
	case DataTypeFloat64:
		return "float64"
	case DataTypeComplex64:
		return "complex64"
	case DataTypeComplex128:
		return "complex128"
	case DataTypeInt4:
		return "int4"
	case DataTypeInt2:
		return "int2"
	default:
		return "unknown"
	}
}

// KernelKey identifies a specific kernel implementation.
type KernelKey struct {
	// Metric specifies the distance metric.
	Metric MetricType
	// DataType specifies the vector element data type.
	DataType DataType
	// Dims specifies the vector dimension (0 means any/generic).
	Dims int
}

// KernelRegistry manages the collection of available SIMD kernels.
type KernelRegistry struct {
	mu      sync.RWMutex
	kernels map[KernelKey]any
}

// Registry is the global KernelRegistry used for polymorphic kernel selection.
var Registry = &KernelRegistry{
	kernels: make(map[KernelKey]any),
}

// Register adds a kernel to the registry.
func (r *KernelRegistry) Register(metric MetricType, dt DataType, dims int, kernel any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := KernelKey{Metric: metric, DataType: dt, Dims: dims}
	r.kernels[key] = kernel
}

// Get retrieves a kernel from the registry.
// If a dimension-specific kernel isn't found, it falls back to the generic (Dims=0) one.
func (r *KernelRegistry) Get(metric MetricType, dt DataType, dims int) any {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Try exact match (dims + type)
	if k, ok := r.kernels[KernelKey{metric, dt, dims}]; ok {
		return k
	}

	// 2. Try generic match (type)
	if k, ok := r.kernels[KernelKey{metric, dt, 0}]; ok {
		return k
	}

	return nil
}

// GetKernel resolves the best available kernel for a given metric, type, and dimension.
// It returns a typed DistanceKernel that can be cached at the dataset level to avoid
// recurring registry lookups and interface assertions on hot paths.
func GetKernel[T any](metric MetricType, dims int) DistanceKernel[T] {
	dt := GetDataType[T]()
	kernel := Registry.Get(metric, dt, dims)
	if kernel == nil {
		return nil
	}

	// 1. Try direct match for generic kernel signature
	if k, ok := kernel.(func([]T, []T) (float32, error)); ok {
		return k
	}

	// 2. Try match for DistanceKernel[T]
	if k, ok := kernel.(DistanceKernel[T]); ok {
		return k
	}

	// 3. Handle standard specialized distance functions by casting through any
	switch k := kernel.(type) {
	case distanceFunc:
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case distanceF16Func:
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case distanceFloat64Func:
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case distanceComplex64Func:
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case distanceComplex128Func:
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]int8, []int8) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]uint8, []uint8) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]int16, []int16) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]uint16, []uint16) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]int32, []int32) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]uint32, []uint32) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]int64, []int64) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	case func([]uint64, []uint64) (float32, error):
		if f, ok := any(k).(func([]T, []T) (float32, error)); ok {
			return f
		}
	}

	return nil
}
