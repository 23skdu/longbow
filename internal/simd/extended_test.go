package simd

import (
	"testing"
)

func TestExtendedDistances(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 0.0, 6.0}

	// Manhattan: |1-4| + |2-0| + |3-6| = 3 + 2 + 3 = 8
	t.Run("Manhattan", func(t *testing.T) {
		got, err := ManhattanDistance(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if got != 8.0 {
			t.Errorf("got %f, want 8.0", got)
		}
	})

	// Chebyshev: max(|1-4|, |2-0|, |3-6|) = max(3, 2, 3) = 3
	t.Run("Chebyshev", func(t *testing.T) {
		got, err := ChebyshevDistance(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if got != 3.0 {
			t.Errorf("got %f, want 3.0", got)
		}
	})

	// Bray-Curtis: sum(|ai-bi|) / sum(|ai+bi|)
	// sum(|ai-bi|) = 8
	// sum(|ai+bi|) = |1+4| + |2+0| + |3+6| = 5 + 2 + 9 = 16
	// 8 / 16 = 0.5
	t.Run("BrayCurtis", func(t *testing.T) {
		got, err := BrayCurtisDistance(a, b)
		if err != nil {
			t.Fatal(err)
		}
		if got != 0.5 {
			t.Errorf("got %f, want 0.5", got)
		}
	})

	t.Run("BrayCurtis_Lengths", func(t *testing.T) {
		lengths := []int{1, 4, 7, 8, 9, 15, 16, 17, 33, 128, 255}
		for _, n := range lengths {
			v1 := make([]float32, n)
			v2 := make([]float32, n)
			for i := 0; i < n; i++ {
				v1[i] = float32(i+1) * 0.5
				v2[i] = float32(n-i) * 0.25
			}
			expected, err := BrayCurtisDistanceFloat32(v1, v2)
			if err != nil {
				t.Fatalf("reference failed at n=%d: %v", n, err)
			}
			actual, err := BrayCurtisDistance(v1, v2)
			if err != nil {
				t.Fatalf("BrayCurtisDistance failed at n=%d: %v", n, err)
			}
			diff := expected - actual
			if diff < 0 {
				diff = -diff
			}
			if diff > 1e-5 {
				t.Errorf("n=%d: got %f, want %f", n, actual, expected)
			}
		}
	})
}

func TestArgMaxMin(t *testing.T) {
	src := []float32{1.0, 5.0, 2.0, 10.0, -1.0}

	t.Run("ArgMax", func(t *testing.T) {
		got := ArgMax(src)
		if got != 3 {
			t.Errorf("got %d, want 3", got)
		}
	})

	t.Run("ArgMin", func(t *testing.T) {
		got := ArgMin(src)
		if got != 4 {
			t.Errorf("got %d, want 4", got)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		if ArgMax(nil) != -1 {
			t.Error("ArgMax(nil) should be -1")
		}
		if ArgMin(nil) != -1 {
			t.Error("ArgMin(nil) should be -1")
		}
	})
}
