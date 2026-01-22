package pq

import (
	"math/rand"
	"testing"
)

func TestTrainKMeans_Basic(t *testing.T) {
	rand.Seed(42)

	// Create simple test data: 3 clusters of 100 vectors each
	n := 300
	dim := 16
	k := 3
	data := make([]float32, n*dim)

	// Generate 3 distinct clusters
	centroids := [][]float32{
		{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}

	for i := 0; i < n; i++ {
		cluster := i / 100
		baseVec := centroids[cluster]
		offset := i * dim
		for j := 0; j < dim; j++ {
			// Add small noise
			data[offset+j] = baseVec[j] + (rand.Float32()-0.5)*0.1
		}
	}

	result, err := TrainKMeans(data, n, dim, k, 20)
	if err != nil {
		t.Fatalf("TrainKMeans failed: %v", err)
	}

	if len(result) != k*dim {
		t.Errorf("Expected %d centroids, got %d", k, len(result)/dim)
	}

	// Verify centroids are reasonable (close to original cluster centers)
	matchCount := 0
	for i := 0; i < k; i++ {
		cent := result[i*dim : (i+1)*dim]
		for _, orig := range centroids {
			dist := float32(0)
			for j := 0; j < dim; j++ {
				d := cent[j] - orig[j]
				dist += d * d
			}
			if dist < 1.0 {
				matchCount++
				break
			}
		}
	}

	if matchCount < 2 {
		t.Errorf("Expected at least 2 centroids to match original clusters, got %d", matchCount)
	}
}

func TestTrainKMeans_EmptyCluster(t *testing.T) {
	rand.Seed(123)

	// Test handling of empty clusters (n < k case)
	n := 5
	dim := 8
	k := 10
	data := make([]float32, n*dim)

	for i := 0; i < n; i++ {
		for j := 0; j < dim; j++ {
			data[i*dim+j] = rand.Float32()
		}
	}

	_, err := TrainKMeans(data, n, dim, k, 5)
	if err == nil {
		t.Error("Expected error when n < k")
	}
	if err != nil && err.Error() != "insufficient data for k-means: n < k" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestTrainKMeans_SingleIteration(t *testing.T) {
	rand.Seed(456)

	n := 100
	dim := 8
	k := 4
	data := make([]float32, n*dim)

	for i := 0; i < n; i++ {
		for j := 0; j < dim; j++ {
			data[i*dim+j] = rand.Float32()
		}
	}

	result, err := TrainKMeans(data, n, dim, k, 1)
	if err != nil {
		t.Fatalf("TrainKMeans failed: %v", err)
	}

	if len(result) != k*dim {
		t.Errorf("Expected %d centroids, got %d", k, len(result)/dim)
	}
}

func TestTrainKMeans_CentroidCount(t *testing.T) {
	rand.Seed(999)

	n := 1000
	dim := 32
	k := 16
	data := make([]float32, n*dim)

	for i := 0; i < n; i++ {
		for j := 0; j < dim; j++ {
			data[i*dim+j] = rand.Float32()
		}
	}

	result, err := TrainKMeans(data, n, dim, k, 5)
	if err != nil {
		t.Fatalf("TrainKMeans failed: %v", err)
	}

	if len(result) != k*dim {
		t.Errorf("Expected %d centroids, got %d", k, len(result)/dim)
	}

	for i, v := range result {
		if v != v {
			t.Errorf("Centroid[%d] is NaN", i)
		}
		if v > 1e10 || v < -1e10 {
			t.Errorf("Centroid[%d] has unreasonable value: %f", i, v)
		}
	}
}

func floatEquals(a, b, tol float32) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < tol
}

func BenchmarkTrainKMeans(b *testing.B) {
	rand.Seed(42)

	testCases := []struct {
		name string
		n    int
		dim  int
		k    int
	}{
		{"Small", 1000, 64, 32},
		{"Medium", 10000, 128, 64},
		{"Large", 50000, 256, 256},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			data := make([]float32, tc.n*tc.dim)
			for i := range data {
				data[i] = rand.Float32()
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				TrainKMeans(data, tc.n, tc.dim, tc.k, 10)
			}
		})
	}
}

func BenchmarkTrainKMeans_AllocationReduction(b *testing.B) {
	rand.Seed(42)

	n := 10000
	dim := 128
	k := 64
	data := make([]float32, n*dim)
	for i := range data {
		data[i] = rand.Float32()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		TrainKMeans(data, n, dim, k, 10)
	}
}

// BenchmarkTrainKMeans_WarmPool measures performance after pool is warmed up
func BenchmarkTrainKMeans_WarmPool(b *testing.B) {
	rand.Seed(42)

	n := 10000
	dim := 128
	k := 64
	data := make([]float32, n*dim)
	for i := range data {
		data[i] = rand.Float32()
	}

	// Warm up the pool
	for i := 0; i < 5; i++ {
		TrainKMeans(data, n, dim, k, 5)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		TrainKMeans(data, n, dim, k, 10)
	}
}
