//go:build gpu && darwin && arm64

package gpu

import (
	"math"
	"testing"
)

func TestMetal3072Dimensions(t *testing.T) {
	dim := 3072
	cfg := GPUConfig{
		Dimension: dim,
		Enabled:   true,
		Backend:   BackendMetal,
	}

	index, err := NewMetalIndexOptimized(cfg)
	if err != nil {
		t.Fatalf("Failed to create MetalIndexOptimized: %v", err)
	}
	defer index.Close()

	// Create two 3072-dim vectors
	// v1: all 1s
	// v2: all 2s
	v1 := make([]float32, dim)
	v2 := make([]float32, dim)
	for i := 0; i < dim; i++ {
		v1[i] = 1.0
		v2[i] = 2.0
	}

	ids := []int64{1, 2}
	vectors := append(v1, v2...)

	err = index.Add(ids, vectors)
	if err != nil {
		t.Fatalf("Failed to add vectors: %v", err)
	}

	// Query with vector near v1
	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = 1.1
	}

	k := 1
	resultIDs, distances, err := index.Search(query, k)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(resultIDs) != k {
		t.Fatalf("Expected %d results, got %d", k, len(resultIDs))
	}

	if resultIDs[0] != 1 {
		t.Errorf("Expected ID 1, got %d", resultIDs[0])
	}

	// Calculate expected distance manually
	// dist = sqrt(sum((1.1 - 1.0)^2)) = sqrt(3072 * 0.01) = sqrt(30.72)
	expectedDist := float32(math.Sqrt(float64(dim) * 0.01))
	if math.Abs(float64(distances[0]-expectedDist)) > 1e-3 {
		t.Errorf("Expected distance %f, got %f", expectedDist, distances[0])
	}

	t.Logf("Successfully verified 3072-dimension support on Metal. Distance: %f", distances[0])
}
