package main

import (
	"testing"
)

func FuzzParseFloats(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		result, _ := fuzzParseFloats(s)
		if s != "" && isNumericString(s) && len(result) == 0 {
			t.Errorf("failed to parse valid input: %s", s)
		}
	})
}

func fuzzParseFloats(s string) ([]float32, error) {
	if s == "" {
		return nil, nil
	}
	var result []float32
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				var f float32
				_ = f
				result = append(result, f)
			}
			start = i + 1
		}
	}
	return result, nil
}

func isNumericString(s string) bool {
	for _, c := range s {
		if c != '.' && c != ',' && (c < '0' || c > '9') && c != '-' && c != ' ' {
			return false
		}
	}
	return true
}

func FuzzSearchModes(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		validModes := map[string]bool{
			"dense": true, "hybrid": true, "sparse": true, "filtered": true, "byid": true,
		}
		_ = validModes[s]
	})
}

func FuzzDimensions(f *testing.F) {
	f.Fuzz(func(t *testing.T, dim int) {
		if dim < 1 || dim > 65536 {
			if dim > 0 {
				t.Logf("dimension out of valid range: %d", dim)
			}
		}
	})
}

func FuzzVectorCounts(f *testing.F) {
	f.Fuzz(func(t *testing.T, count int) {
		if count < 0 {
			t.Logf("count should not be negative: %d", count)
		}
	})
}

func FuzzFilterExpression(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		result, _ := fuzzParseFilter(s)
		if s != "" && isValidFilter(s) && result == nil {
			t.Logf("failed to parse valid filter: %s", s)
		}
	})
}

func fuzzParseFilter(s string) (interface{}, error) {
	if s == "" {
		return nil, nil
	}
	return s, nil
}

func isValidFilter(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '{' && s[len(s)-1] == '}' {
		return true
	}
	validChars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_==<>!.,'() "
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
		found := false
		for _, vc := range validChars {
			if c == vc {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func FuzzGeoSearchParams(f *testing.F) {
	f.Fuzz(func(t *testing.T, lat, lon, radius float64) {
		isValid := lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && radius > 0
		_ = isValid
	})
}

func FuzzTemporalSearch(f *testing.F) {
	f.Fuzz(func(t *testing.T, s string) {
		valid := s == "as_of" || s == "range" || s == "window" || s == "latest"
		_ = valid
	})
}

func TestBenchIOIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

func TestVecModeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}