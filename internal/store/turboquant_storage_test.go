package store

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseVectorType_Float32 verifies that "float32" maps to VectorTypeFloat32.
func TestParseVectorType_Float32(t *testing.T) {
	dt, err := ParseVectorType(VectorTypeAPIFloat32)
	require.NoError(t, err)
	assert.Equal(t, types.VectorTypeFloat32, dt)
}

// TestParseVectorType_Empty verifies that an empty string defaults to float32.
func TestParseVectorType_Empty(t *testing.T) {
	dt, err := ParseVectorType("")
	require.NoError(t, err)
	assert.Equal(t, types.VectorTypeFloat32, dt)
}

// TestParseVectorType_TurboQuant verifies that "turboquant" maps to VectorTypeTQ.
func TestParseVectorType_TurboQuant(t *testing.T) {
	dt, err := ParseVectorType(VectorTypeAPITurboQuant)
	require.NoError(t, err)
	assert.Equal(t, types.VectorTypeTQ, dt)
}

// TestParseVectorType_Int8 verifies that "int8" maps to VectorTypeInt8.
func TestParseVectorType_Int8(t *testing.T) {
	dt, err := ParseVectorType(VectorTypeAPIInt8)
	require.NoError(t, err)
	assert.Equal(t, types.VectorTypeInt8, dt)
}

// TestParseVectorType_Invalid verifies that an unknown type returns a descriptive
// error that lists all valid values.
func TestParseVectorType_Invalid(t *testing.T) {
	_, err := ParseVectorType("quaternion")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "quaternion", "error must quote the invalid value")
	assert.Contains(t, err.Error(), VectorTypeAPIFloat32, "error must list float32")
	assert.Contains(t, err.Error(), VectorTypeAPITurboQuant, "error must list turboquant")
}

// TestTurboQuantStorageRatio_Compression verifies that a TurboQuant dataset at
// 3-bit compression of 768-dim vectors achieves < 1.0 storage ratio vs float32.
func TestTurboQuantStorageRatio_Compression(t *testing.T) {
	const (
		vectorCount = 10000
		dims        = 768
		bitsPerDim  = 3
	)
	// TQ storage: roughly (dims * bitsPerDim / 8) bytes per vector.
	tqBytes := int64(vectorCount * dims * bitsPerDim / 8)
	ratio := TurboQuantStorageRatio(tqBytes, vectorCount, dims)
	assert.Less(t, ratio, 1.0, "TQ at 3 bits must use less storage than float32")
}
