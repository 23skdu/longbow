package core

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTestMetadata creates a simple binary metadata with one string field
func buildTestMetadata(key, value string) []byte {
	var buf []byte
	num := make([]byte, 2)
	binary.LittleEndian.PutUint16(num, 1)
	buf = append(buf, num...)

	kl := make([]byte, 2)
	binary.LittleEndian.PutUint16(kl, uint16(len(key)))
	buf = append(buf, kl...)
	buf = append(buf, []byte(key)...)

	// TypeString
	buf = append(buf, TypeString)

	vl := make([]byte, 4)
	binary.LittleEndian.PutUint32(vl, uint32(len(value)))
	buf = append(buf, vl...)
	buf = append(buf, []byte(value)...)

	return buf
}

func buildNilMetadata(key string) []byte {
	var buf []byte
	num := make([]byte, 2)
	binary.LittleEndian.PutUint16(num, 1)
	buf = append(buf, num...)

	kl := make([]byte, 2)
	binary.LittleEndian.PutUint16(kl, uint16(len(key)))
	buf = append(buf, kl...)
	buf = append(buf, []byte(key)...)

	buf = append(buf, TypeNil)

	vl := make([]byte, 4)
	binary.LittleEndian.PutUint32(vl, 0)
	buf = append(buf, vl...)

	return buf
}

// TestNewArrowMetadata runs the test new arrow metadata operation.
func TestNewArrowMetadata(t *testing.T) {
	data := buildTestMetadata("hello", "world")
	m := NewArrowMetadata(data)
	assert.NotNil(t, m)
}

// TestArrowMetadata_GetField_Found runs the test arrow metadata_get field_found operation.
func TestArrowMetadata_GetField_Found(t *testing.T) {
	data := buildTestMetadata("key", "value123")
	m := NewArrowMetadata(data)
	v, ok := m.GetField("key")
	require.True(t, ok)
	assert.Equal(t, "value123", v)
}

// TestArrowMetadata_GetField_NotFound runs the test arrow metadata_get field_not found operation.
func TestArrowMetadata_GetField_NotFound(t *testing.T) {
	data := buildTestMetadata("key", "value")
	m := NewArrowMetadata(data)
	_, ok := m.GetField("missing")
	assert.False(t, ok)
}

// TestArrowMetadata_GetField_Empty runs the test arrow metadata_get field_empty operation.
func TestArrowMetadata_GetField_Empty(t *testing.T) {
	m := NewArrowMetadata([]byte{})
	_, ok := m.GetField("key")
	assert.False(t, ok)
}

// TestArrowMetadata_GetField_NilValue runs the test arrow metadata_get field_nil value operation.
func TestArrowMetadata_GetField_NilValue(t *testing.T) {
	data := buildNilMetadata("nilkey")
	m := NewArrowMetadata(data)
	v, ok := m.GetField("nilkey")
	require.True(t, ok)
	assert.Nil(t, v)
}

// TestArrowMetadata_ToMap runs the test arrow metadata_to map operation.
func TestArrowMetadata_ToMap(t *testing.T) {
	// Use the codec to encode valid metadata
	data, err := EncodeMetadata(map[string]interface{}{"field1": "val1"})
	require.NoError(t, err)
	m := NewArrowMetadata(data)
	mp, err := m.ToMap()
	require.NoError(t, err)
	require.NotNil(t, mp)
	assert.Contains(t, mp, "field1")
	assert.Equal(t, "val1", mp["field1"])
}

// TestArrowMetadata_ToMap_Empty runs the test arrow metadata_to map_empty operation.
func TestArrowMetadata_ToMap_Empty(t *testing.T) {
	m := NewArrowMetadata([]byte{})
	mp, _ := m.ToMap()
	assert.Empty(t, mp)
}

// TestErrNotFound_Error error types
func TestErrNotFound_Error(t *testing.T) {
	err := NewNotFoundError("collection", "my_col")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "my_col")
	assert.Contains(t, err.Error(), "collection")
}

// TestErrInvalidArgument_Error_WithField runs the test err invalid argument_error_with field operation.
func TestErrInvalidArgument_Error_WithField(t *testing.T) {
	err := NewInvalidArgumentError("dims", "must be positive")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dims")
}

// TestErrInvalidArgument_Error_NoField runs the test err invalid argument_error_no field operation.
func TestErrInvalidArgument_Error_NoField(t *testing.T) {
	err := NewInvalidArgumentError("", "bad input")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad input")
}

// TestErrResourceExhausted_Error runs the test err resource exhausted_error operation.
func TestErrResourceExhausted_Error(t *testing.T) {
	err := NewResourceExhaustedError("memory", "out of heap")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "memory")
	assert.Contains(t, err.Error(), "out of heap")
}

// TestErrUnavailable_Error runs the test err unavailable_error operation.
func TestErrUnavailable_Error(t *testing.T) {
	err := NewUnavailableError("search", "index locked")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "search")
}

// TestNewShutdownError runs the test new shutdown error operation.
func TestNewShutdownError(t *testing.T) {
	inner := NewNotFoundError("store", "s1")
	err := NewShutdownError("flush", "store1", inner)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutdown")
}

// TestGeoPoint_MarshalUnmarshalJSON.
func TestGeoPoint_MarshalUnmarshalJSON(t *testing.T) {
	g := &GeoPoint{Lat: 37.7749, Lon: -122.4194}
	data, err := g.MarshalJSON()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	var g2 GeoPoint
	err = g2.UnmarshalJSON(data)
	require.NoError(t, err)
	assert.InDelta(t, 37.7749, g2.Lat, 0.001)
}

// TestTemporalSearchRequest_Validate_Valid Validate — "range" requires StartTime/EndTime > 0
func TestTemporalSearchRequest_Validate_Valid(t *testing.T) {
	req := &TemporalSearchRequest{
		Dataset:    "test",
		SearchType: "range",
		K:          10,
		StartTime:  1000,
		EndTime:    2000,
	}
	err := req.Validate()
	assert.NoError(t, err)
}

// TestTemporalSearchRequest_Validate_MissingTimes runs the test temporal search request_validate_missing times operation.
func TestTemporalSearchRequest_Validate_MissingTimes(t *testing.T) {
	req := &TemporalSearchRequest{
		Dataset:    "test",
		SearchType: "range",
		K:          10,
		StartTime:  0, // invalid
		EndTime:    0,
	}
	err := req.Validate()
	assert.Error(t, err)
}

// TestTemporalSearchRequest_Validate_AsOf_MissingTimestamp runs the test temporal search request_validate_as of_missing timestamp operation.
func TestTemporalSearchRequest_Validate_AsOf_MissingTimestamp(t *testing.T) {
	req := &TemporalSearchRequest{
		Dataset:    "test",
		SearchType: "as_of",
		Timestamp:  0, // invalid
	}
	err := req.Validate()
	assert.Error(t, err)
}

// TestTemporalAggregationRequest_Validate_Valid runs the test temporal aggregation request_validate_valid operation.
func TestTemporalAggregationRequest_Validate_Valid(t *testing.T) {
	req := &TemporalAggregationRequest{
		StartTime: 1000,
		EndTime:   2000,
		Interval:  100,
	}
	err := req.Validate()
	assert.NoError(t, err)
}

// TestTemporalAggregationRequest_Validate_InvalidTime runs the test temporal aggregation request_validate_invalid time operation.
func TestTemporalAggregationRequest_Validate_InvalidTime(t *testing.T) {
	req := &TemporalAggregationRequest{
		StartTime: 5000,
		EndTime:   1000,
		Interval:  100,
	}
	err := req.Validate()
	assert.Error(t, err)
}

// TestResultSliceIterator.
func TestResultSliceIterator(t *testing.T) {
	results := []SearchResult{
		{ID: VectorID(1), Distance: 0.1},
		{ID: VectorID(2), Distance: 0.2},
	}
	iter := NewResultSliceIterator(results)
	require.NotNil(t, iter)

	r, ok := iter.Next()
	require.True(t, ok)
	assert.Equal(t, VectorID(1), r.ID)

	r, ok = iter.Next()
	require.True(t, ok)
	assert.Equal(t, VectorID(2), r.ID)

	_, ok = iter.Next()
	assert.False(t, ok)

	err := iter.Close()
	assert.NoError(t, err)
}

// TestResultSliceIterator_Empty runs the test result slice iterator_empty operation.
func TestResultSliceIterator_Empty(t *testing.T) {
	iter := NewResultSliceIterator(nil)
	_, ok := iter.Next()
	assert.False(t, ok)
	assert.NoError(t, iter.Close())
}

// --- ArrowMetadata binary helpers (match GetField format: uint16 numFields) ---

func appendFieldRaw(buf []byte, key string, typeID uint8, payload []byte) []byte {
	kl := make([]byte, 2)
	binary.LittleEndian.PutUint16(kl, uint16(len(key)))
	buf = append(buf, kl...)
	buf = append(buf, []byte(key)...)
	buf = append(buf, typeID)
	vl := make([]byte, 4)
	binary.LittleEndian.PutUint32(vl, uint32(len(payload)))
	buf = append(buf, vl...)
	buf = append(buf, payload...)
	return buf
}

func beginMetadata(numFields uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, numFields)
	return buf
}

func appendFieldString(buf []byte, key, val string) []byte {
	return appendFieldRaw(buf, key, TypeString, []byte(val))
}

func appendFieldInt64(buf []byte, key string, val int64) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, uint64(val))
	return appendFieldRaw(buf, key, TypeInt64, payload)
}

func appendFieldFloat64(buf []byte, key string, val float64) []byte {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint64(payload, math.Float64bits(val))
	return appendFieldRaw(buf, key, TypeFloat64, payload)
}

func appendFieldBool(buf []byte, key string, val bool) []byte {
	var b byte
	if val {
		b = 1
	}
	return appendFieldRaw(buf, key, TypeBool, []byte{b})
}

func appendFieldBinary(buf []byte, key string, val []byte) []byte {
	return appendFieldRaw(buf, key, TypeBinary, val)
}

// TestArrowMetadata_GetField_NumFieldsZero runs the test arrow metadata_get field_num fields zero operation.
func TestArrowMetadata_GetField_NumFieldsZero(t *testing.T) {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, 0)
	m := NewArrowMetadata(buf)
	_, ok := m.GetField("anything")
	assert.False(t, ok)
}

// TestArrowMetadata_GetField_Int64 runs the test arrow metadata_get field_int64 operation.
func TestArrowMetadata_GetField_Int64(t *testing.T) {
	buf := beginMetadata(1)
	buf = appendFieldInt64(buf, "count", 42)
	m := NewArrowMetadata(buf)
	v, ok := m.GetField("count")
	require.True(t, ok)
	assert.Equal(t, int64(42), v)
}

// TestArrowMetadata_GetField_Float64 runs the test arrow metadata_get field_float64 operation.
func TestArrowMetadata_GetField_Float64(t *testing.T) {
	buf := beginMetadata(1)
	buf = appendFieldFloat64(buf, "score", 3.14)
	m := NewArrowMetadata(buf)
	v, ok := m.GetField("score")
	require.True(t, ok)
	assert.InDelta(t, 3.14, v.(float64), 0.001)
}

// TestArrowMetadata_GetField_Bool runs the test arrow metadata_get field_bool operation.
func TestArrowMetadata_GetField_Bool(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  bool
	}{
		{"true", true},
		{"false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := beginMetadata(1)
			buf = appendFieldBool(buf, "flag", tc.val)
			m := NewArrowMetadata(buf)
			v, ok := m.GetField("flag")
			require.True(t, ok)
			assert.Equal(t, tc.val, v)
		})
	}
}

// TestArrowMetadata_GetField_Binary runs the test arrow metadata_get field_binary operation.
func TestArrowMetadata_GetField_Binary(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	buf := beginMetadata(1)
	buf = appendFieldBinary(buf, "data", payload)
	m := NewArrowMetadata(buf)
	v, ok := m.GetField("data")
	require.True(t, ok)
	assert.Equal(t, payload, v)
}

// TestArrowMetadata_GetField_UnknownType runs the test arrow metadata_get field_unknown type operation.
func TestArrowMetadata_GetField_UnknownType(t *testing.T) {
	buf := beginMetadata(1)
	buf = appendFieldRaw(buf, "mystery", 99, []byte{1, 2, 3})
	m := NewArrowMetadata(buf)
	_, ok := m.GetField("mystery")
	assert.False(t, ok)
}

// TestArrowMetadata_GetField_MultiField runs the test arrow metadata_get field_multi field operation.
func TestArrowMetadata_GetField_MultiField(t *testing.T) {
	buf := beginMetadata(4)
	buf = appendFieldString(buf, "a", "alpha")
	buf = appendFieldInt64(buf, "b", 100)
	buf = appendFieldFloat64(buf, "c", 2.5)
	buf = appendFieldBool(buf, "d", true)
	m := NewArrowMetadata(buf)

	v, ok := m.GetField("d")
	require.True(t, ok)
	assert.Equal(t, true, v)

	v, ok = m.GetField("a")
	require.True(t, ok)
	assert.Equal(t, "alpha", v)

	_, ok = m.GetField("z")
	assert.False(t, ok)
}

// TestGeoPoint_UnmarshalJSON_Invalid runs the test geo point_unmarshal json_invalid operation.
func TestGeoPoint_UnmarshalJSON_Invalid(t *testing.T) {
	var g GeoPoint
	err := g.UnmarshalJSON([]byte("not json at all"))
	assert.Error(t, err)
}

// TestTemporalAggregationRequest_Validate_StartTimeZero runs the test temporal aggregation request_validate_start time zero operation.
func TestTemporalAggregationRequest_Validate_StartTimeZero(t *testing.T) {
	req := &TemporalAggregationRequest{
		StartTime: 0,
		EndTime:   2000,
		Interval:  100,
	}
	err := req.Validate()
	assert.Error(t, err)
}

// TestTemporalAggregationRequest_Validate_IntervalZero runs the test temporal aggregation request_validate_interval zero operation.
func TestTemporalAggregationRequest_Validate_IntervalZero(t *testing.T) {
	req := &TemporalAggregationRequest{
		StartTime: 1000,
		EndTime:   2000,
		Interval:  0,
	}
	err := req.Validate()
	assert.Error(t, err)
}
