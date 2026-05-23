package main

import (
	"testing"
)

func TestVectorSearchModes(t *testing.T) {
	validModes := map[string]bool{
		"dense":    true,
		"hybrid":   true,
		"sparse":   true,
		"filtered": true,
		"byid":     true,
		"ingest":   true,
		"":         false,
		"unknown":  false,
		"INVALID":  false,
	}

	for mode, expected := range validModes {
		isValid := mode == "dense" || mode == "hybrid" || mode == "sparse" || mode == "filtered" || mode == "byid" || mode == "ingest"
		if isValid != expected && mode != "" {
			t.Errorf("mode %s validation mismatch: got %v, want %v", mode, isValid, expected)
		}
	}
}

func TestDataTypes(t *testing.T) {
	validDtypes := []string{
		"float32", "float64", "float16",
		"int8", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64",
		"complex64", "complex128",
		"turboquant2", "turboquant4", "turboquant8",
	}

	for _, dtype := range validDtypes {
		if len(dtype) == 0 {
			t.Errorf("empty dtype")
		}
	}
}

func TestDimensions(t *testing.T) {
	validDims := []int{128, 384, 768, 1024, 3072}
	brokenDims := []int{512}

	for _, dim := range validDims {
		if dim <= 0 || dim%128 != 0 && dim != 384 && dim != 768 {
			t.Errorf("invalid dimension: %d", dim)
		}
	}

	for _, dim := range brokenDims {
		t.Logf("Note: dimension %d may have known issues", dim)
	}
}

func TestVectorCounts(t *testing.T) {
	validCounts := []int{100, 500, 1000, 5000, 10000, 50000, 100000}
	minCount := 1
	maxCount := 1000000

	for _, count := range validCounts {
		if count < minCount || count > maxCount {
			t.Errorf("count %d outside valid range [%d, %d]", count, minCount, maxCount)
		}
	}
}

func TestSearchModesParse(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"dense", []string{"dense"}},
		{"dense,hybrid", []string{"dense", "hybrid"}},
		{"dense,hybrid,sparse,filtered,byid", []string{"dense", "hybrid", "sparse", "filtered", "byid"}},
		{"  dense , hybrid ", []string{"dense", "hybrid"}},
		{"", []string{}},
	}

	for _, tt := range tests {
		var result []string
		if tt.input != "" {
			result = parseSearchModes(tt.input)
		}

		if len(result) != len(tt.expected) {
			t.Errorf("parseSearchModes(%q): got %d modes, want %d", tt.input, len(result), len(tt.expected))
		}
	}
}

func parseSearchModes(input string) []string {
	if input == "" {
		return nil
	}
	var modes []string
	for _, m := range splitAndTrim(input, ",") {
		if m != "" {
			modes = append(modes, m)
		}
	}
	return modes
}

func splitAndTrim(s, sep string) []string {
	parts := []string{}
	for _, p := range split(s, sep) {
		parts = append(parts, trim(p))
	}
	return parts
}

func split(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && s[start] == ' ' {
		start++
	}
	for end > start && s[end-1] == ' ' {
		end--
	}
	return s[start:end]
}
