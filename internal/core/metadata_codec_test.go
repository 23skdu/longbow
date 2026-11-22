package core

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncodeMetadata_NilInput runs the test encode metadata_nil input operation.
func TestEncodeMetadata_NilInput(t *testing.T) {
	result, err := EncodeMetadata(nil)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

// TestEncodeMetadata_StringValue runs the test encode metadata_string value operation.
func TestEncodeMetadata_StringValue(t *testing.T) {
	result, err := EncodeMetadata(map[string]interface{}{"key": "val"})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, "val", decoded["key"])
}

// TestEncodeMetadata_Int64Value runs the test encode metadata_int64 value operation.
func TestEncodeMetadata_Int64Value(t *testing.T) {
	result, err := EncodeMetadata(map[string]interface{}{"n": int64(42)})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), decoded["n"])
}

// TestEncodeMetadata_BoolFalse runs the test encode metadata_bool false operation.
func TestEncodeMetadata_BoolFalse(t *testing.T) {
	result, err := EncodeMetadata(map[string]interface{}{"flag": false})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, false, decoded["flag"])
}

// TestEncodeMetadata_BinaryData runs the test encode metadata_binary data operation.
func TestEncodeMetadata_BinaryData(t *testing.T) {
	data := []byte{0xCA, 0xFE}
	result, err := EncodeMetadata(map[string]interface{}{"raw": data})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, data, decoded["raw"])
}

// TestEncodeMetadata_UnknownTypeFallback runs the test encode metadata_unknown type fallback operation.
func TestEncodeMetadata_UnknownTypeFallback(t *testing.T) {
	type custom struct{ x int }
	result, err := EncodeMetadata(map[string]interface{}{"c": custom{1}})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	// Unknown type falls back to string representation
	assert.Equal(t, "{1}", decoded["c"])
}

// TestEncodeMetadata_AllTypes runs the test encode metadata_all types operation.
func TestEncodeMetadata_AllTypes(t *testing.T) {
	input := map[string]interface{}{
		"s":   "hello",
		"n":   int64(99),
		"f":   3.14,
		"b":   true,
		"nil": nil,
		"raw": []byte{1, 2, 3},
	}
	result, err := EncodeMetadata(input)
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, "hello", decoded["s"])
	assert.Equal(t, int64(99), decoded["n"])
	assert.InDelta(t, 3.14, decoded["f"].(float64), 0.001)
	assert.Equal(t, true, decoded["b"])
	assert.Nil(t, decoded["nil"])
	assert.Equal(t, []byte{1, 2, 3}, decoded["raw"])
}

// TestDecodeMetadata_TypeBinary_Roundtrip runs the test decode metadata_type binary_roundtrip operation.
func TestDecodeMetadata_TypeBinary_Roundtrip(t *testing.T) {
	original := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	result, err := EncodeMetadata(map[string]interface{}{"d": original})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Equal(t, original, decoded["d"])
}

// TestDecodeMetadata_TypeNil_Roundtrip runs the test decode metadata_type nil_roundtrip operation.
func TestDecodeMetadata_TypeNil_Roundtrip(t *testing.T) {
	result, err := EncodeMetadata(map[string]interface{}{"k": nil})
	require.NoError(t, err)
	decoded, err := DecodeMetadata(result)
	require.NoError(t, err)
	assert.Nil(t, decoded["k"])
}

// TestDecodeMetadata_UnknownType runs the test decode metadata_unknown type operation.
func TestDecodeMetadata_UnknownType(t *testing.T) {
	// Manually build binary with unknown typeID=99
	// Format: numFields(uint32) + keyLen(uint16) + key + typeID(uint8) + valLen(uint32) + value
	var buf []byte

	// numFields = 1
	nf := make([]byte, 4)
	binary.LittleEndian.PutUint32(nf, 1)
	buf = append(buf, nf...)

	// keyLen = 3
	kl := make([]byte, 2)
	binary.LittleEndian.PutUint16(kl, 3)
	buf = append(buf, kl...)

	// key = "xyz"
	buf = append(buf, "xyz"...)

	// typeID = 99 (unknown)
	buf = append(buf, 99)

	// valLen = 2
	vl := make([]byte, 4)
	binary.LittleEndian.PutUint32(vl, 2)
	buf = append(buf, vl...)

	// value = {0xAA, 0xBB}
	buf = append(buf, 0xAA, 0xBB)

	decoded, err := DecodeMetadata(buf)
	require.NoError(t, err)
	// Unknown type is skipped, so key should not be present
	_, exists := decoded["xyz"]
	assert.False(t, exists)
}

// TestDecodeMetadata_Empty runs the test decode metadata_empty operation.
func TestDecodeMetadata_Empty(t *testing.T) {
	decoded, err := DecodeMetadata([]byte{})
	assert.NoError(t, err)
	assert.Nil(t, decoded)
}
