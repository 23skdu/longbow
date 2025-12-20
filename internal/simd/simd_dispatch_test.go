package simd

import (
	"testing"
)

// TestDispatchInitialization verifies function pointers are initialized at startup
func TestDispatchInitialization(t *testing.T) {
	t.Run("euclidean_dispatch_initialized", func(t *testing.T) {
		if euclideanDistanceImpl == nil {
			t.Error("euclideanDistanceImpl should be initialized at startup")
		}
	})

	t.Run("cosine_dispatch_initialized", func(t *testing.T) {
		if cosineDistanceImpl == nil {
			t.Error("cosineDistanceImpl should be initialized at startup")
		}
	})

	t.Run("dot_product_dispatch_initialized", func(t *testing.T) {
		if dotProductImpl == nil {
			t.Error("dotProductImpl should be initialized at startup")
		}
	})

	t.Run("euclidean_batch_dispatch_initialized", func(t *testing.T) {
		if euclideanDistanceBatchImpl == nil {
			t.Error("euclideanDistanceBatchImpl should be initialized at startup")
		}
	})
}

// TestDispatchSelection verifies correct implementation is selected based on CPU
func TestDispatchSelection(t *testing.T) {
	impl := GetImplementation()

	// Verify implementation string matches one of the expected values
	validImpls := map[string]bool{
		"avx512":  true,
		"avx2":    true,
		"neon":    true,
		"generic": true,
	}

	if !validImpls[impl] {
		t.Errorf("unexpected implementation selected: %s", impl)
	}

	t.Logf("Selected SIMD implementation: %s", impl)
}

// TestDispatchCorrectness verifies dispatched functions produce correct results
func TestDispatchCorrectness(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0, 4.0}
	b := []float32{5.0, 6.0, 7.0, 8.0}

	t.Run("euclidean_via_dispatch", func(t *testing.T) {
		// Expected: sqrt((5-1)^2 + (6-2)^2 + (7-3)^2 + (8-4)^2) = sqrt(64) = 8
		result := EuclideanDistance(a, b)
		expected := float32(8.0)
		if abs(result-expected) > 0.0001 {
			t.Errorf("EuclideanDistance = %v, want %v", result, expected)
		}
	})

	t.Run("dot_product_via_dispatch", func(t *testing.T) {
		// Expected: 1*5 + 2*6 + 3*7 + 4*8 = 5 + 12 + 21 + 32 = 70
		result := DotProduct(a, b)
		expected := float32(70.0)
		if abs(result-expected) > 0.0001 {
			t.Errorf("DotProduct = %v, want %v", result, expected)
		}
	})

	t.Run("cosine_via_dispatch", func(t *testing.T) {
		result := CosineDistance(a, b)
		// Cosine distance should be between 0 and 2
		if result < 0 || result > 2 {
			t.Errorf("CosineDistance = %v, expected between 0 and 2", result)
		}
	})

	t.Run("euclidean_batch_via_dispatch", func(t *testing.T) {
		query := []float32{1.0, 2.0, 3.0, 4.0}
		vectors := [][]float32{
			{5.0, 6.0, 7.0, 8.0},
			{1.0, 2.0, 3.0, 4.0}, // same as query, distance = 0
		}
		results := make([]float32, 2)

		EuclideanDistanceBatch(query, vectors, results)

		if abs(results[0]-8.0) > 0.0001 {
			t.Errorf("batch result[0] = %v, want 8.0", results[0])
		}
		if abs(results[1]) > 0.0001 {
			t.Errorf("batch result[1] = %v, want 0.0", results[1])
		}
	})
}

// TestDispatchNoSwitch verifies we can call impl directly (for benchmarking)
func TestDispatchNoSwitch(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0, 4.0}
	b := []float32{5.0, 6.0, 7.0, 8.0}

	// Call implementation directly (no validation wrapper)
	result := euclideanDistanceImpl(a, b)
	expected := float32(8.0)
	if abs(result-expected) > 0.0001 {
		t.Errorf("direct impl call = %v, want %v", result, expected)
	}
}

// BenchmarkDispatchOverhead compares dispatched vs direct calls
func BenchmarkDispatchOverhead(b *testing.B) {
	a := make([]float32, 128)
	c := make([]float32, 128)
	for i := range a {
		a[i] = float32(i)
		c[i] = float32(i + 1)
	}

	b.Run("EuclideanDistance_Dispatched", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = EuclideanDistance(a, c)
		}
	})

	b.Run("EuclideanDistance_Direct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = euclideanDistanceImpl(a, c)
		}
	})

	b.Run("DotProduct_Dispatched", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = DotProduct(a, c)
		}
	})

	b.Run("DotProduct_Direct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dotProductImpl(a, c)
		}
	})
}

// BenchmarkHotPath simulates HNSW search hot path with many distance calls
func BenchmarkHotPath(b *testing.B) {
	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i) * 0.1
	}

	// Simulate 1000 candidate vectors
	candidates := make([][]float32, 1000)
	for i := range candidates {
		candidates[i] = make([]float32, 128)
		for j := range candidates[i] {
			candidates[i][j] = float32(i+j) * 0.1
		}
	}

	b.Run("Sequential_Dispatched", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, c := range candidates {
				_ = EuclideanDistance(query, c)
			}
		}
	})

	b.Run("Sequential_Direct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, c := range candidates {
				_ = euclideanDistanceImpl(query, c)
			}
		}
	})

	b.Run("Batch", func(b *testing.B) {
		results := make([]float32, len(candidates))
		for i := 0; i < b.N; i++ {
			EuclideanDistanceBatch(query, candidates, results)
		}
	})
}

func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
