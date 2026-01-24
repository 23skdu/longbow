package store

import "github.com/23skdu/longbow/internal/store/types"

type VectorDataType = types.VectorDataType

const (
	VectorTypeFloat32    = types.VectorTypeFloat32
	VectorTypeFloat64    = types.VectorTypeFloat64
	VectorTypeComplex64  = types.VectorTypeComplex64
	VectorTypeComplex128 = types.VectorTypeComplex128
	VectorTypeInt8       = types.VectorTypeInt8
	VectorTypeUint8      = types.VectorTypeUint8
	VectorTypeFloat16    = types.VectorTypeFloat16
	VectorTypeInt32      = types.VectorTypeInt32
	VectorTypeInt64      = types.VectorTypeInt64
)

type SearchOptions struct {
	IncludeVectors bool
	VectorFormat   VectorDataType
	Filter         any
	ExactK         bool
}
