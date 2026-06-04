package tensor

import (
	"fmt"
	"math"
)

// Dtype represents the element type of a tensor.
type Dtype uint8

const (
	DtypeInvalid Dtype = iota
	DtypeFloat32
	DtypeFloat64
	DtypeInt8
	DtypeUint8
	DtypeInt16
	DtypeUint16
	DtypeInt32
	DtypeUint32
	DtypeInt64
	DtypeUint64
	DtypeFloat16
	DtypeComplex64
	DtypeComplex128
)

func (d Dtype) Size() int {
	switch d {
	case DtypeFloat32, DtypeInt32, DtypeUint32:
		return 4
	case DtypeFloat64, DtypeInt64, DtypeUint64, DtypeComplex64:
		return 8
	case DtypeInt8, DtypeUint8:
		return 1
	case DtypeInt16, DtypeUint16, DtypeFloat16:
		return 2
	case DtypeComplex128:
		return 16
	default:
		return 0
	}
}

func (d Dtype) String() string {
	switch d {
	case DtypeFloat32:
		return "float32"
	case DtypeFloat64:
		return "float64"
	case DtypeInt8:
		return "int8"
	case DtypeUint8:
		return "uint8"
	case DtypeInt16:
		return "int16"
	case DtypeUint16:
		return "uint16"
	case DtypeInt32:
		return "int32"
	case DtypeUint32:
		return "uint32"
	case DtypeInt64:
		return "int64"
	case DtypeUint64:
		return "uint64"
	case DtypeFloat16:
		return "float16"
	case DtypeComplex64:
		return "complex64"
	case DtypeComplex128:
		return "complex128"
	default:
		return "invalid"
	}
}

func (d Dtype) IsFloat() bool {
	return d == DtypeFloat32 || d == DtypeFloat64 || d == DtypeFloat16
}

func (d Dtype) IsInt() bool {
	switch d {
	case DtypeInt8, DtypeInt16, DtypeInt32, DtypeInt64:
		return true
	}
	return false
}

func (d Dtype) IsUint() bool {
	switch d {
	case DtypeUint8, DtypeUint16, DtypeUint32, DtypeUint64:
		return true
	}
	return false
}

func (d Dtype) IsComplex() bool {
	return d == DtypeComplex64 || d == DtypeComplex128
}

// Promote returns the dtype for binary operations (e.g. float32+int32 -> float32).
func Promote(a, b Dtype) Dtype {
	if a == DtypeComplex128 || b == DtypeComplex128 {
		return DtypeComplex128
	}
	if a == DtypeComplex64 || b == DtypeComplex64 {
		return DtypeComplex64
	}
	if a == DtypeFloat64 || b == DtypeFloat64 {
		return DtypeFloat64
	}
	if a == DtypeFloat32 || b == DtypeFloat32 {
		return DtypeFloat32
	}
	if a == DtypeFloat16 || b == DtypeFloat16 {
		return DtypeFloat16
	}
	if a == DtypeInt64 || b == DtypeInt64 {
		return DtypeInt64
	}
	if a == DtypeUint64 || b == DtypeUint64 {
		return DtypeUint64
	}
	if a == DtypeInt32 || b == DtypeInt32 {
		return DtypeInt32
	}
	if a == DtypeUint32 || b == DtypeUint32 {
		return DtypeUint32
	}
	if a == DtypeInt16 || b == DtypeInt16 {
		return DtypeInt16
	}
	if a == DtypeUint16 || b == DtypeUint16 {
		return DtypeUint16
	}
	if a == DtypeInt8 || b == DtypeInt8 {
		return DtypeInt8
	}
	if a == DtypeUint8 || b == DtypeUint8 {
		return DtypeUint8
	}
	return DtypeFloat32
}

// Finite checks if a float32 value is finite (non-NaN, non-Inf).
func Finite32(v float32) bool {
	return !math.IsInf(float64(v), 0) && !math.IsNaN(float64(v))
}

// Finite64 checks if a float64 value is finite.
func Finite64(v float64) bool {
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func checkShape(name string, shape Shape, dims ...int) error {
	if len(shape) != len(dims) {
		return fmt.Errorf("%s: expected %d axes, got %d", name, len(dims), len(shape))
	}
	for i, d := range dims {
		if shape[i] != d {
			return fmt.Errorf("%s: axis %d: expected dim %d, got %d", name, i, d, shape[i])
		}
	}
	return nil
}
