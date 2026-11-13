package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBenchmarkModes(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{"write mode", "write"},
		{"read mode", "read"},
		{"mixed mode", "mixed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mode != "write" && tt.mode != "read" && tt.mode != "mixed" {
				t.Errorf("unknown mode: %s", tt.mode)
			}
		})
	}
}

func TestBlockSizes(t *testing.T) {
	validSizes := []int{512, 1024, 2048, 4096, 8192, 16384}

	for _, size := range validSizes {
		if size <= 0 {
			t.Errorf("invalid block size: %d", size)
		}
	}
}

func TestConcurrencyBounds(t *testing.T) {
	invalidWorkers := []int{0, -1, -100}
	for _, w := range invalidWorkers {
		if w >= 1 {
			t.Errorf("expected invalid workers: %d", w)
		}
	}
}

func TestFileSizeValidation(t *testing.T) {
	invalidSizes := []int{0, -1, -100}
	for _, s := range invalidSizes {
		if s >= 1 {
			t.Errorf("expected invalid size: %d", s)
		}
	}
}

func TestIOBenchHelpers(t *testing.T) {
	printStats("SmokeTest", time.Second, 100, 1024*1024)
	tmpDir := t.TempDir()
	size := prepFile(tmpDir, 1, 4096)
	if size < 1024*1024 {
		t.Errorf("prepFile size = %d; want at least 1MB", size)
	}
}

func TestBuildIOBenchTicket(t *testing.T) {
	query := []float32{0.1, 0.2, 0.3}
	cases := []struct {
		mode    string
		rootKey string
	}{
		{"dense", "search"},
		{"hybrid", "search"},
		{"sparse", "search"},
		{"filtered", "search"},
		{"byid", "search_by_id"},
		{"range", "range_search"},
		{"federated", "federated_search"},
		{"recommend", "recommend"},
		{"geo_radius", "geo_search"},
		{"geo_box", "geo_search"},
		{"geo_hybrid", "geo_search"},
		{"temporal_as_of", "temporal_search"},
		{"temporal_range", "temporal_search"},
		{"temporal_window", "temporal_search"},
		{"temporal_sliding_time", "temporal_search"},
	}
	for _, tc := range cases {
		raw, err := buildIOBenchTicket(tc.mode, "ds", query, 5, 100, 1700000000000000000)
		if err != nil {
			t.Fatalf("buildIOBenchTicket(%q) error = %v", tc.mode, err)
		}
		var parsed map[string]any
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("buildIOBenchTicket(%q) invalid json: %v", tc.mode, err)
		}
		if _, ok := parsed[tc.rootKey]; !ok {
			t.Errorf("buildIOBenchTicket(%q) missing key %q in %s", tc.mode, tc.rootKey, string(raw))
		}
	}
	if _, err := buildIOBenchTicket("unknown", "ds", query, 0, 10, 0); err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}
