package store

import "github.com/23skdu/longbow/internal/store/types"

// VectorDataType represents the underlying numeric type of vector elements.
type VectorDataType = types.VectorDataType

const (
	// VectorTypeFloat32 represents a 32-bit floating point vector.
	VectorTypeFloat32 = types.VectorTypeFloat32
	// VectorTypeFloat64 represents a 64-bit floating point vector.
	VectorTypeFloat64 = types.VectorTypeFloat64
	// VectorTypeComplex64 represents a 64-bit complex vector.
	VectorTypeComplex64 = types.VectorTypeComplex64
	// VectorTypeComplex128 represents a 128-bit complex vector.
	VectorTypeComplex128 = types.VectorTypeComplex128
	// VectorTypeInt8 represents an 8-bit signed integer vector.
	VectorTypeInt8 = types.VectorTypeInt8
	// VectorTypeUint8 represents an 8-bit unsigned integer vector.
	VectorTypeUint8 = types.VectorTypeUint8
	// VectorTypeFloat16 represents a 16-bit floating point vector.
	VectorTypeFloat16 = types.VectorTypeFloat16
	// VectorTypeInt32 represents a 32-bit signed integer vector.
	VectorTypeInt32 = types.VectorTypeInt32
	// VectorTypeInt64 represents a 64-bit signed integer vector.
	VectorTypeInt64 = types.VectorTypeInt64
	// VectorTypeTQ represents a Tensor Quantized vector.
	VectorTypeTQ = types.VectorTypeTQ
	// VectorTypeBQ represents a Binary Quantized vector.
	VectorTypeBQ = types.VectorTypeBQ
)

// SearchOptions defines tunable parameters for vector search operations.
type SearchOptions = types.SearchOptions
