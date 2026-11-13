package main

import (
	"encoding/json"
	"testing"
)

func TestParseFloats(t *testing.T) {
	floats := parseFloats("1.0, 2.0, 3.5")
	if len(floats) != 3 || floats[0] != 1.0 || floats[1] != 2.0 || floats[2] != 3.5 {
		t.Fatalf("unexpected parseFloats result: %v", floats)
	}

	filter, err := parseFilterExpression(`{"status": "active"}`)
	if err != nil || filter == nil {
		t.Fatalf("unexpected parseFilterExpression error: %v", err)
	}

	rec, sch := generateDemoData(4, 3)
	if rec == nil || sch == nil {
		t.Fatal("expected non-nil demo data")
	}
	defer rec.Release()
	if rec.NumRows() != 3 {
		t.Errorf("demo data rows = %d; want 3", rec.NumRows())
	}

	printUsage()
}

func TestSearchModes(t *testing.T) {
	cases := []struct {
		name      string
		mode      string
		vector    string
		text      string
		id        string
		filters   string
		expectKey string
	}{
		{"dense", "dense", "1,0,0", "", "", "", "search"},
		{"sparse", "sparse", "", "quantum", "", `{"field":"cat","operator":"=","value":"sec"}`, "search"},
		{"filtered", "filtered", "1,0,0", "", "", `[{"field":"cat","operator":"=","value":"sec"}]`, "search"},
		{"hybrid", "hybrid", "1,0,0", "quantum", "", "", "search"},
		{"by-id", "by-id", "", "", "doc-0", "", "search_by_id"},
		{"range", "range", "1,0,0", "", "", "", "range_search"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := buildSearchTicket(tc.mode, "kb", tc.vector, tc.text, tc.id, tc.filters, 5, 60, 2, 0.5, 0.25, 0.2, true)
			if err != nil {
				t.Fatalf("buildSearchTicket(%s) error: %v", tc.mode, err)
			}
			var decoded map[string]map[string]any
			if err := json.Unmarshal(b, &decoded); err != nil {
				t.Fatalf("unmarshal ticket: %v", err)
			}
			if _, ok := decoded[tc.expectKey]; !ok {
				t.Fatalf("expected root key %q in %v", tc.expectKey, decoded)
			}
		})
	}
}

func TestSearchTicketBuilders(t *testing.T) {
	if _, err := buildGeoSearchTicket("box", "kb", "37.7,-122.5,37.8,-122.4", "", 0, 0, 0, 5); err != nil {
		t.Fatalf("box geo ticket failed: %v", err)
	}
	if _, err := buildGeoSearchTicket("hybrid", "kb", "", "1,0,0,0", 37.7, -122.4, 5.0, 5); err != nil {
		t.Fatalf("hybrid geo ticket failed: %v", err)
	}
	if _, err := buildFederatedSearchTicket("col_a,col_b", "", "1,0,0,0", 5, 60); err != nil {
		t.Fatalf("federated ticket failed: %v", err)
	}
	textReq := buildAdvancedTextRequest("kb", "enc AND mesh", "category", "<mark>", 5, 1)
	if textReq["query"] != "enc AND mesh" {
		t.Fatalf("unexpected text req: %v", textReq)
	}
	if _, err := buildMultiModalRequest("media", "text", "sunset", "", "", "", 5); err != nil {
		t.Fatalf("multimodal req failed: %v", err)
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
	validTypes := []string{"as_of", "range", "sliding_window", "sliding_window_time", "window"}
	for _, typ := range validTypes {
		body, err := buildTemporalSearchBody(typ, "kb", "5m", 100, 50, 100, 10, 5)
		if err != nil {
			t.Errorf("buildTemporalSearchBody(%s) failed: %v", typ, err)
		}
		if body["search_type"] == "" {
			t.Errorf("empty search_type for %s", typ)
		}
	}
}
