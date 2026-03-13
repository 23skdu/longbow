package simd

import (
	"math"
	"testing"
)

func TestCosineDistanceFloat64(t *testing.T) {
	tests := []struct {
		name      string
		a         []float64
		b         []float64
		expected  float32
		tolerance float32
	}{
		{
			name:      "identical vectors",
			a:         []float64{1.0, 2.0, 3.0},
			b:         []float64{1.0, 2.0, 3.0},
			expected:  0.0,
			tolerance: 1e-6,
		},
		{
			name:      "orthogonal vectors",
			a:         []float64{1.0, 0.0},
			b:         []float64{0.0, 1.0},
			expected:  1.0,
			tolerance: 1e-6,
		},
		{
			name:      "opposite vectors",
			a:         []float64{1.0, 0.0},
			b:         []float64{-1.0, 0.0},
			expected:  2.0,
			tolerance: 1e-6,
		},
		{
			name:      "zero vector",
			a:         []float64{0.0, 0.0},
			b:         []float64{1.0, 2.0},
			expected:  1.0,
			tolerance: 1e-6,
		},
		{
			name:      "both zero vectors",
			a:         []float64{0.0, 0.0},
			b:         []float64{0.0, 0.0},
			expected:  1.0,
			tolerance: 1e-6,
		},
		{
			name:      "longer vectors",
			a:         []float64{1.0, 2.0, 3.0, 4.0, 5.0},
			b:         []float64{2.0, 4.0, 6.0, 8.0, 10.0},
			expected:  0.0,
			tolerance: 1e-6,
		},
		{
			name:      "partial similarity",
			a:         []float64{1.0, 0.0, 0.0},
			b:         []float64{1.0, 1.0, 0.0},
			expected:  0.292893, // 1 - (1 / sqrt(2))
			tolerance: 1e-4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := CosineDistanceFloat64(tt.a, tt.b)
			if err != nil {
				t.Fatalf("CosineDistanceFloat64 error: %v", err)
			}
			if math.Abs(float64(result-tt.expected)) > float64(tt.tolerance) {
				t.Errorf("CosineDistanceFloat64(%v, %v) = %v, expected %v (diff %v)",
					tt.a, tt.b, result, tt.expected, result-tt.expected)
			}
		})
	}
}

func TestCosineDistanceFloat64_LengthMismatch(t *testing.T) {
	a := []float64{1.0, 2.0}
	b := []float64{1.0, 2.0, 3.0}
	_, err := CosineDistanceFloat64(a, b)
	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestCosineDistanceFloat64_Empty(t *testing.T) {
	a := []float64{}
	b := []float64{}
	result, err := CosineDistanceFloat64(a, b)
	if err != nil {
		t.Fatalf("CosineDistanceFloat64 error: %v", err)
	}
	if result != 1.0 {
		t.Errorf("Empty vectors should have cosine distance 1.0, got %v", result)
	}
}
