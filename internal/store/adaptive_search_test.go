package store

import (
	"testing"
)

func TestCalculateAdaptiveLimit(t *testing.T) {
	tests := []struct {
		name    string
		k       int
		matches uint64
		total   int
		want    int
	}{
		{
			name:    "Small selectivity (high match rate)",
			k:       10,
			matches: 1000,
			total:   1000, // 100% match
			want:    20,   // k * 2.0 (min limit)
		},
		{
			name:    "Medium selectivity (50% match)",
			k:       10,
			matches: 500,
			total:   1000,
			want:    20, // Selectivity 0.5 -> Factor 2.0 -> k*2 = 20
		},
		{
			name:    "High selectivity (10% match)",
			k:       10,
			matches: 100,
			total:   1000,
			want:    100, // Selectivity 0.1 -> Factor 10 -> k*10 = 100
		},
		{
			name:    "Very high selectivity (2% match)",
			k:       10,
			matches: 20,
			total:   1000,
			want:    500, // Selectivity 0.02 -> Factor 50 -> k*50 = 500
		},
		{
			name:    "Extreme selectivity (1% match)",
			k:       10,
			matches: 10,
			total:   1000,
			want:    500, // Selectivity 0.01 -> Factor 100 -> Clamped to 50 -> k*50 = 500
		},
		{
			name:    "Empty matches",
			k:       10,
			matches: 0,
			total:   1000,
			want:    10, // Default k
		},
		{
			name:    "Empty total",
			k:       10,
			matches: 0,
			total:   0,
			want:    10, // Default k
		},
		{
			name:    "Limit exceeds total",
			k:       100,
			matches: 50,
			total:   200, // 25% match -> Factor 4 -> 400 > 200
			want:    200, // Clamped to total
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateAdaptiveLimit(tt.k, tt.matches, tt.total)
			if got != tt.want {
				t.Errorf("calculateAdaptiveLimit(%d, %d, %d) = %d, want %d", tt.k, tt.matches, tt.total, got, tt.want)
			}
		})
	}
}
