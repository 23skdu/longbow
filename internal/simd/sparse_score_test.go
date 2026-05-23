package simd

import (
	"math"
	"testing"
)

func TestBM25ScoreBatchCorrectness(t *testing.T) {
	// Sample data
	tfs := []int{1, 2, 0, 5}
	docLengths := []int{100, 200, 150, 50}
	avgDL := float32(125.0)
	idf := float32(2.5)
	k1 := float32(1.2)
	b := float32(0.75)

	// Expected results calculation
	// score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLen / avgDL))
	expected := make([]float32, len(tfs))
	for i, tf := range tfs {
		if tf == 0 {
			expected[i] = 0
			continue
		}
		numerator := idf * float32(tf) * (k1 + 1)
		denominator := float32(tf) + k1*(1-b+b*float32(docLengths[i])/avgDL)
		expected[i] = numerator / denominator
	}

	// Run BM25ScoreBatch
	// It should use the neon table on ARM64 which we rebound to generic
	actual := BM25ScoreBatch(tfs, docLengths, avgDL, idf, k1, b)

	if len(actual) != len(expected) {
		t.Fatalf("expected length %d, got %d", len(expected), len(actual))
	}

	for i := range expected {
		if math.Abs(float64(actual[i]-expected[i])) > 1e-6 {
			t.Errorf("at index %d: expected %f, got %f", i, expected[i], actual[i])
		}
	}
}

func TestBM25ScoreBatchEmpty(t *testing.T) {
	actual := BM25ScoreBatch(nil, nil, 100, 2.0, 1.2, 0.75)
	if len(actual) != 0 {
		t.Errorf("expected empty slice, got %v", actual)
	}
}
