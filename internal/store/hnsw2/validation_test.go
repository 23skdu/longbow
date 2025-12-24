package hnsw2

import (
	"math"
	"testing"
	
	"github.com/23skdu/longbow/internal/store"
	"github.com/coder/hnsw"
)

// DualIndexHarness runs the same operations on both coder/hnsw and hnsw2
// to validate correctness and measure recall.
type DualIndexHarness struct {
	reference *hnsw.Graph[uint32]  // coder/hnsw (ground truth)
	candidate *ArrowHNSW            // Our implementation
	dataset   *store.Dataset
	vectors   map[uint32][]float32  // Test vectors
	distFunc  func([]float32, []float32) float32
}

// NewDualIndexHarness creates a new validation harness.
func NewDualIndexHarness(dataset *store.Dataset) *DualIndexHarness {
	h := &DualIndexHarness{
		reference: hnsw.NewGraph[uint32](),
		candidate: NewArrowHNSW(dataset, DefaultConfig()),
		dataset:   dataset,
		vectors:   make(map[uint32][]float32),
		distFunc:  euclideanDistance,
	}
	
	// Set distance function for reference index
	h.reference.Distance = func(a, b uint32) float32 {
		vecA, okA := h.vectors[a]
		vecB, okB := h.vectors[b]
		if !okA || !okB {
			return float32(math.Inf(1))
		}
		return h.distFunc(vecA, vecB)
	}
	
	return h
}

// AddVector adds a vector to both indexes.
func (h *DualIndexHarness) AddVector(id uint32, vec []float32) {
	// Store vector for distance calculations
	h.vectors[id] = vec
	
	// Add to reference (coder/hnsw)
	h.reference.Add(id)
	
	// TODO: Add to candidate when Insert is implemented
	// h.candidate.Insert(id, vec)
}

// MeasureRecall compares search results between reference and candidate.
// Returns recall@k (percentage of reference results found in candidate results).
func (h *DualIndexHarness) MeasureRecall(query []float32, queryID uint32, k int) float64 {
	// Store query vector temporarily for distance calculations
	h.vectors[queryID] = query
	
	// Search reference
	refResults := h.reference.Search(queryID, k)
	
	// TODO: Search candidate when Search is implemented
	// candResults := h.candidate.Search(query, k)
	
	// For now, return 0 since candidate search not implemented
	_ = refResults
	return 0.0
}

// euclideanDistance computes L2 distance between two vectors.
func euclideanDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		return float32(math.Inf(1))
	}
	
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
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
	
	// Search (will return 0 recall until Search is implemented)
	query := []float32{1.0, 0.0, 0.0}
	recall := harness.MeasureRecall(query, 999, 1)
	
	// For now, recall should be 0 (not implemented)
	if recall != 0.0 {
		t.Logf("Recall: %f (expected 0 until Search implemented)", recall)
	}
}
