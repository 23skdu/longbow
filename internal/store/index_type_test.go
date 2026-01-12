package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorDataType_String(t *testing.T) {
	tests := []struct {
		dt       VectorDataType
		expected string
	}{
		{VectorTypeInt8, "int8"},
		{VectorTypeUint8, "uint8"},
		{VectorTypeInt16, "int16"},
		{VectorTypeUint16, "uint16"},
		{VectorTypeInt32, "int32"},
		{VectorTypeUint32, "uint32"},
		{VectorTypeInt64, "int64"},
		{VectorTypeUint64, "uint64"},
		{VectorTypeFloat16, "float16"},
		{VectorTypeFloat32, "float32"},
		{VectorTypeFloat64, "float64"},
		{VectorTypeComplex64, "complex64"},
		{VectorTypeComplex128, "complex128"},
		{VectorDataType(-1), "unknown(-1)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dt.String())
		})
	}
}

func TestVectorDataType_ElementSize(t *testing.T) {
	tests := []struct {
		dt       VectorDataType
		expected int
	}{
		{VectorTypeInt8, 1},
		{VectorTypeUint8, 1},
		{VectorTypeInt16, 2},
		{VectorTypeUint16, 2},
		{VectorTypeFloat16, 2},
		{VectorTypeInt32, 4},
		{VectorTypeUint32, 4},
		{VectorTypeFloat32, 4},
		{VectorTypeInt64, 8},
		{VectorTypeUint64, 8},
		{VectorTypeFloat64, 8},
		{VectorTypeComplex64, 8},
		{VectorTypeComplex128, 16},
		{VectorDataType(99), 0},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.dt.String(), tt.expected), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dt.ElementSize())
		})
	}
}
