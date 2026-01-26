package store

import (
	"fmt"
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"

	"github.com/stretchr/testify/assert"
)

func TestVectorDataType_String(t *testing.T) {
	tests := []struct {
		dt       lbtypes.VectorDataType
		expected string
	}{
		{lbtypes.VectorTypeInt8, "int8"},
		{lbtypes.VectorTypeUint8, "uint8"},
		{lbtypes.VectorTypeInt16, "int16"},
		{lbtypes.VectorTypeUint16, "uint16"},
		{lbtypes.VectorTypeInt32, "int32"},
		{lbtypes.VectorTypeUint32, "uint32"},
		{lbtypes.VectorTypeInt64, "int64"},
		{lbtypes.VectorTypeUint64, "uint64"},
		{lbtypes.VectorTypeFloat16, "float16"},
		{lbtypes.VectorTypeFloat32, "float32"},
		{lbtypes.VectorTypeFloat64, "float64"},
		{lbtypes.VectorTypeComplex64, "complex64"},
		{lbtypes.VectorTypeComplex128, "complex128"},
		{lbtypes.VectorDataType(-1), "unknown(-1)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dt.String())
		})
	}
}

func TestVectorDataType_ElementSize(t *testing.T) {
	tests := []struct {
		dt       lbtypes.VectorDataType
		expected int
	}{
		{lbtypes.VectorTypeInt8, 1},
		{lbtypes.VectorTypeUint8, 1},
		{lbtypes.VectorTypeInt16, 2},
		{lbtypes.VectorTypeUint16, 2},
		{lbtypes.VectorTypeFloat16, 2},
		{lbtypes.VectorTypeInt32, 4},
		{lbtypes.VectorTypeUint32, 4},
		{lbtypes.VectorTypeFloat32, 4},
		{lbtypes.VectorTypeInt64, 8},
		{lbtypes.VectorTypeUint64, 8},
		{lbtypes.VectorTypeFloat64, 8},
		{lbtypes.VectorTypeComplex64, 8},
		{lbtypes.VectorTypeComplex128, 16},
		{VectorDataType(99), 0},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.dt.String(), tt.expected), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dt.ElementSize())
		})
	}
}
