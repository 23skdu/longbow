package core

import (
	"reflect"
	"testing"
)

func TestMetadataCodec(t *testing.T) {
	testCases := []struct {
		name string
		data map[string]interface{}
	}{
		{
			name: "Simple string",
			data: map[string]interface{}{"key": "value"},
		},
		{
			name: "Numeric types",
			data: map[string]interface{}{
				"int":   int64(42),
				"float": float64(3.14),
			},
		},
		{
			name: "Boolean",
			data: map[string]interface{}{"active": true},
		},
		{
			name: "Complex map",
			data: map[string]interface{}{
				"name":    "longbow",
				"version": int64(1),
				"score":   float64(0.95),
				"type":    "database",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeMetadata(tc.data)
			if err != nil {
				t.Fatalf("failed to encode: %v", err)
			}

			decoded, err := DecodeMetadata(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			if !reflect.DeepEqual(tc.data, decoded) {
				t.Errorf("expected %v, got %v", tc.data, decoded)
			}
		})
	}
}

func BenchmarkEncodeMetadata(b *testing.B) {
	data := map[string]interface{}{
		"name":    "longbow",
		"version": int64(1),
		"score":   float64(0.95),
		"tags":    []interface{}{"vector", "database"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeMetadata(data)
	}
}

func BenchmarkDecodeMetadata(b *testing.B) {
	data := map[string]interface{}{
		"name":    "longbow",
		"version": int64(1),
		"score":   float64(0.95),
		"tags":    []interface{}{"vector", "database"},
	}
	encoded, _ := EncodeMetadata(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeMetadata(encoded)
	}
}
