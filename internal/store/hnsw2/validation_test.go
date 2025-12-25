package hnsw2

import (

	"testing"
	
	"github.com/23skdu/longbow/internal/store"
)

// DualIndexHarness will run the same operations on both coder/hnsw and hnsw2
// to validate correctness and measure recall.
// For now, it's a placeholder until Search is implemented.
type DualIndexHarness struct {
	candidate *ArrowHNSW            // Our implementation
	dataset   *store.Dataset
	vectors   map[uint32][]float32  // Test vectors
}

// NewDualIndexHarness creates a new validation harness.
func NewDualIndexHarness(dataset *store.Dataset) *DualIndexHarness {
	return &DualIndexHarness{
		candidate: NewArrowHNSW(dataset, DefaultConfig()),
		dataset:   dataset,
		vectors:   make(map[uint32][]float32),
	}
}

// AddVector adds a vector to the index.
func (h *DualIndexHarness) AddVector(id uint32, vec []float32) {
	// Store vector
	h.vectors[id] = vec
	
	// TODO: Add to candidate when Insert is implemented
	// h.candidate.Insert(id, vec)
}

// MeasureRecall will compare search results between reference and candidate.
// Returns recall@k (percentage of reference results found in candidate results).
// TODO: Implement when Search is ready
func (h *DualIndexHarness) MeasureRecall(query []float32, k int) float64 {
	// TODO: Implement when Search is ready
	return 0.0
}



// TestDualIndexHarness_Basic validates the harness setup.
func TestDualIndexHarness_Basic(t *testing.T) {
	// Create a simple dataset
	dataset := &store.Dataset{
		Name: "test",
	}
	
	harness := NewDualIndexHarness(dataset)
	
	// Add some vectors
	vec1 := []float32{1.0, 0.0, 0.0}
	vec2 := []float32{0.0, 1.0, 0.0}
	vec3 := []float32{0.0, 0.0, 1.0}
	
	harness.AddVector(0, vec1)
	harness.AddVector(1, vec2)
	harness.AddVector(2, vec3)
	
	// Verify vectors stored
	if len(harness.vectors) != 3 {
		t.Errorf("expected 3 vectors, got %d", len(harness.vectors))
	}
	
	// Search will be implemented in Phase 2
	query := []float32{1.0, 0.0, 0.0}
	recall := harness.MeasureRecall(query, 1)
	
	// For now, recall should be 0 (not implemented)
	if recall != 0.0 {
		t.Logf("Recall: %f (expected 0 until Search implemented)", recall)
	}
}
