package store

import (
	"testing"
)

func TestBatchDistanceCompute(t *testing.T) {
	queries := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	
	candidates := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
	}
	
	results := make([]float32, 2)
	
	BatchDistanceCompute(queries, candidates, results)
	
	// Identical vectors should have distance 0
	if results[0] != 0.0 {
		t.Errorf("distance[0] = %f, want 0.0", results[0])
	}
	if results[1] != 0.0 {
		t.Errorf("distance[1] = %f, want 0.0", results[1])
	}
}

func BenchmarkBatchDistanceCompute(b *testing.B) {
	numPairs := 100
	queries := make([][]float32, numPairs)
	candidates := make([][]float32, numPairs)
	results := make([]float32, numPairs)
	
	for i := 0; i < numPairs; i++ {
		queries[i] = make([]float32, 384)
		candidates[i] = make([]float32, 384)
		for j := 0; j < 384; j++ {
			queries[i][j] = float32(i + j)
			candidates[i][j] = float32(i + j + 1)
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BatchDistanceCompute(queries, candidates, results)
	}
}

func BenchmarkBatchVsSequential(b *testing.B) {
	numPairs := 100
	queries := make([][]float32, numPairs)
	candidates := make([][]float32, numPairs)
	results := make([]float32, numPairs)
	
	for i := 0; i < numPairs; i++ {
		queries[i] = make([]float32, 384)
		candidates[i] = make([]float32, 384)
		for j := 0; j < 384; j++ {
			queries[i][j] = float32(i + j)
			candidates[i][j] = float32(i + j + 1)
		}
	}
	
	b.Run("Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			BatchDistanceCompute(queries, candidates, results)
		}
	})
	
	b.Run("Sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < numPairs; j++ {
				results[j] = distanceSIMD(queries[j], candidates[j])
			}
		}
	})
}
