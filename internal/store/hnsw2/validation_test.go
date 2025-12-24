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
	vectors   [][]float32           // Test vectors
}

// NewDualIndexHarness creates a new validation harness.
func NewDualIndexHarness(dataset *store.Dataset) *DualIndexHarness {
	return &DualIndexHarness{
		reference: hnsw.NewGraph[uint32](),
		candidate: NewArrowHNSW(dataset, DefaultConfig()),
		dataset:   dataset,
		vectors:   make([][]float32, 0),
	}
}

// AddVector adds a vector to both indexes.
func (h *DualIndexHarness) AddVector(id uint32, vec []float32) {
	// Store vector for later recall testing
	if int(id) >= len(h.vectors) {
		// Expand vectors slice
		newVecs := make([][]float32, id+1)
		copy(newVecs, h.vectors)
		h.vectors = newVecs
	}
	h.vectors[id] = vec
	
	// Add to reference (coder/hnsw)
	h.reference.Add(id, vec)
	
	// TODO: Add to candidate when Insert is implemented
	// h.candidate.Insert(id, vec)
}

// MeasureRecall compares search results between reference and candidate.
// Returns recall@k (percentage of reference results found in candidate results).
func (h *DualIndexHarness) MeasureRecall(query []float32, k int) float64 {
	// Search reference
	refResults := h.reference.Search(query, k)
	
	// TODO: Search candidate when Search is implemented
	// candResults := h.candidate.Search(query, k)
	
	// For now, return 0 since candidate search not implemented
	_ = refResults
	return 0.0
}

// recall calculates recall@k between two result sets.
func recall(candidate, reference []hnsw.SearchResult[uint32]) float64 {
	if len(reference) == 0 {
		return 1.0
	}
	
	// Build set of reference IDs
	refSet := make(map[uint32]bool)
	for _, r := range reference {
		refSet[r.Key] = true
	}
	
	// Count matches in candidate
	matches := 0
	for _, c := range candidate {
		if refSet[c.Key] {
			matches++
		}
	}
	
	return float64(matches) / float64(len(reference))
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
	recall := harness.MeasureRecall(query, 1)
	
	// For now, recall should be 0 (not implemented)
	if recall != 0.0 {
		t.Logf("Recall: %f (expected 0 until Search implemented)", recall)
	}
}
