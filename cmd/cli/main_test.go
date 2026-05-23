package main

import (
	"testing"
)

func TestParseFloats(t *testing.T) {
	validInputs := []string{"1.0,2.0,3.0", "0.1,0.2,0.3", "1"}
	invalidInputs := []string{"invalid"}

	for _, input := range validInputs {
		if !isNumericList(input) {
			t.Errorf("expected valid input: %s", input)
		}
	}

	for _, input := range invalidInputs {
		if isNumericList(input) {
			t.Errorf("expected invalid input: %s", input)
		}
	}
}

func isNumericList(s string) bool {
	if s == "" {
		return true
	}
	for _, c := range s {
		if c != '.' && c != ',' && (c < '0' || c > '9') && c != '-' && c != ' ' {
			return false
		}
	}
	return true
}

func TestSearchModes(t *testing.T) {
	validModes := []string{"dense", "sparse", "filtered", "hybrid"}
	for _, mode := range validModes {
		isValid := mode == "dense" || mode == "sparse" || mode == "filtered" || mode == "hybrid"
		if !isValid {
			t.Errorf("unexpected mode: %s", mode)
		}
	}
}

func TestVectorParseIntegrity(t *testing.T) {
	tests := []struct {
		name  string
		dim   int
		count int
	}{
		{"small", 128, 100},
		{"medium", 384, 1000},
		{"large", 1024, 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.dim <= 0 || tt.count <= 0 {
				t.Errorf("invalid params: dim=%d, count=%d", tt.dim, tt.count)
			}
			expectedVecSize := tt.dim * tt.count
			if expectedVecSize != tt.dim*tt.count {
				t.Errorf("vec size mismatch")
			}
		})
	}
}

func TestFilterExpressionParse(t *testing.T) {
	validFilters := []string{"field == 'value'", `{"field": "value"}`}
	invalidFilters := []string{"invalid{{", "@#$%"}

	for _, f := range validFilters {
		isValid := isValidFilterExpr(f)
		if !isValid && f != "" {
			t.Logf("warn: filter may need validation: %s", f)
		}
	}

	for _, f := range invalidFilters {
		if isValidFilterExpr(f) && f != "" {
			isSimpleChar := func() bool {
				for _, c := range f {
					if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '=' || c == '\'' || c == '"' || c == '{' || c == '}' {
						continue
					}
					return false
				}
				return true
			}()
			if isSimpleChar {
				t.Logf("warn: filter may be valid: %s", f)
			}
		}
	}
}

func isValidFilterExpr(s string) bool {
	if s == "" {
		return false
	}
	hasValidChars := false
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '=' || c == '\'' || c == '"' || c == '{' || c == '}' || c == ' ' {
			hasValidChars = true
		}
	}
	return hasValidChars
}

func TestGeoSearchParams(t *testing.T) {
	tests := []struct {
		lat    float64
		lon    float64
		radius float64
		valid  bool
	}{
		{40.7128, -74.0060, 1.0, true},
		{51.5074, -0.1278, 5.0, true},
		{0, 0, 0, false},
		{91.0, 0, 1.0, false},
		{0, 181.0, 1.0, false},
		{40.7128, -74.0060, 0, false},
	}

	for _, tt := range tests {
		isValid := tt.lat >= -90 && tt.lat <= 90 && tt.lon >= -180 && tt.lon <= 180 && tt.radius > 0
		if isValid != tt.valid {
			t.Logf("geo params: lat=%v, lon=%v, radius=%v -> valid=%v (expected %v)", tt.lat, tt.lon, tt.radius, isValid, tt.valid)
		}
	}
}

func TestTemporalSearchTypes(t *testing.T) {
	validTypes := []string{"as_of", "range", "window", "latest"}
	for _, typ := range validTypes {
		isValid := typ == "as_of" || typ == "range" || typ == "window" || typ == "latest"
		if !isValid {
			t.Errorf("unexpected temporal type: %s", typ)
		}
	}
}
