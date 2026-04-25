package store

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DualIndexHarness will run the same operations on both coder/hnsw and hnsw2
// to validate correctness and measure recall.
type DualIndexHarness struct {
	candidate *ArrowHNSW           // Our implementation
	dataset   *Dataset
	vectors   map[uint32][]float32 // Test vectors
}

// NewDualIndexHarness creates a new validation harness.
func NewDualIndexHarness(dataset *Dataset) *DualIndexHarness {
	cfg := DefaultArrowHNSWConfig()
	return &DualIndexHarness{
		candidate: NewArrowHNSW(dataset, &cfg, nil),
		dataset:   dataset,
		vectors:   make(map[uint32][]float32),
	}
}

// AddVector adds a vector to the index.
func (h *DualIndexHarness) AddVector(id uint32, vec []float32) {
	// Store vector
	h.vectors[id] = vec

	// Create a single-row batch for this vector
	// Note: This is inefficient but functional for validation harness usage
	mem := memory.NewGoAllocator()
	schema := h.dataset.Schema
	if schema == nil {
		schema = arrow.NewSchema([]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(len(vec)), arrow.PrimitiveTypes.Float32)},
		}, nil)
		h.dataset.Schema = schema
	}

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	listB := b.Field(0).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	listB.Append(true)
	valB.AppendValues(vec, nil)

	rec := b.NewRecordBatch()
	// Do not release rec immediately, it belongs to dataset now

	h.dataset.Records = append(h.dataset.Records, rec)
	batchIdx := len(h.dataset.Records) - 1

	// Insert into candidate
	if _, err := h.candidate.AddByLocation(context.Background(), batchIdx, 0); err != nil {
		fmt.Printf("PANIC ERROR: %v\n", err)
		panic(err.Error())
	}
}

// MeasureRecall will compare search results between reference and candidate.
// Returns recall@k (percentage of reference results found in candidate results).
func (h *DualIndexHarness) MeasureRecall(query []float32, k int) float64 {
	// 1. Brute force ground truth
	type res struct {
		id   uint32
		dist float32
	}
	var truth []res
	for id, vec := range h.vectors {
		var d float32
		for i := 0; i < len(vec); i++ {
			diff := vec[i] - query[i]
			d += diff * diff
		}
		truth = append(truth, res{id: id, dist: d})
	}
	sort.Slice(truth, func(i, j int) bool { return truth[i].dist < truth[j].dist })
	if len(truth) > k {
		truth = truth[:k]
	}

	truthIDs := make(map[uint32]bool)
	for _, r := range truth {
		truthIDs[r.id] = true
	}

	// 2. Candidate search
	cands, err := h.candidate.Search(context.Background(), query, k, nil)
	if err != nil {
		return 0.0
	}

	// 3. Calculate overlap
	matches := 0
	for _, c := range cands {
		if truthIDs[uint32(c.ID)] {
			matches++
		}
	}

	if len(truth) == 0 {
		return 1.0
	}
	return float64(matches) / float64(len(truth))
}

// TestDualIndexHarness_Basic validates the harness setup.
func TestDualIndexHarness_Basic(t *testing.T) {
	// Create a simple dataset
	dataset := &Dataset{
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

	// Search is implemented, measure recall
	query := []float32{1.0, 0.0, 0.0}
	recall := harness.MeasureRecall(query, 1)

	// Recall should be 1.0 since we have exact matches in the index
	if recall != 1.0 {
		t.Errorf("expected recall 1.0, got %f", recall)
	}
}
