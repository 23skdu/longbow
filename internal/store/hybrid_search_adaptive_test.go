package store

import (
	"testing"
)

// =============================================================================
// Adaptive Alpha Tests
// =============================================================================

func TestEstimateAlpha(t *testing.T) {
	tests := []struct {
		query    string
		expected float32
	}{
		{"keyword", 0.3},
		{"two words", 0.3},
		{"three words query", 0.5},
		{"this is four words", 0.5},
		{"one two three four five", 0.5},    // Exactly 5 -> Medium
		{"this is a five words query", 0.8}, // 6 tokens -> Long
		{"this is a long natural language query", 0.8},
		{"", 0.3}, // Empty query treated as short
	}

	for _, tc := range tests {
		got := EstimateAlpha(tc.query)
		if got != tc.expected {
			t.Errorf("EstimateAlpha(%q): expected %v, got %v", tc.query, tc.expected, got)
		}
	}
}
