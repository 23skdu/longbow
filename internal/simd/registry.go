package simd

import (
	"sync"
)

// MetricType defines the distance metric to use.
type MetricType int

const (
	MetricEuclidean MetricType = iota
	MetricCosine
	MetricDotProduct
)

func (m MetricType) String() string {
	switch m {
	case MetricEuclidean:
		return "euclidean"
	case MetricCosine:
		return "cosine"
	case MetricDotProduct:
		return "dot"
	default:
		return "unknown"
	}
}

// SIMDDataType is a local redeclaration of VectorDataType to avoid circular imports.
// It must stay in sync with the mapping in internal/store if used there.
type SIMDDataType int

const (
	DataTypeFloat32 SIMDDataType = iota
	DataTypeFloat16
	DataTypeInt8
	DataTypeUint8
	DataTypeInt16
	DataTypeUint16
	DataTypeInt32
	DataTypeUint32
	DataTypeInt64
	DataTypeUint64
	DataTypeFloat64
	DataTypeComplex64
	DataTypeComplex128
)

func (d SIMDDataType) String() string {
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
	default:
		return "unknown"
	}
}

// KernelKey identifies a specific kernel implementation.
type KernelKey struct {
	Metric   MetricType
	DataType SIMDDataType
	Dims     int // 0 means any/generic
}

// KernelRegistry manages the collection of available SIMD kernels.
type KernelRegistry struct {
	mu      sync.RWMutex
	kernels map[KernelKey]any
}

var Registry = &KernelRegistry{
	kernels: make(map[KernelKey]any),
}

// Register adds a kernel to the registry.
func (r *KernelRegistry) Register(metric MetricType, dt SIMDDataType, dims int, kernel any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := KernelKey{Metric: metric, DataType: dt, Dims: dims}
	r.kernels[key] = kernel
}

// Get retrieves a kernel from the registry.
// If a dimension-specific kernel isn't found, it falls back to the generic (Dims=0) one.
func (r *KernelRegistry) Get(metric MetricType, dt SIMDDataType, dims int) any {
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
