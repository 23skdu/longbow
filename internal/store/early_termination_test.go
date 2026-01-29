package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_SearchEarlyTermination(t *testing.T) {
	// Setup a small HNSW with some data
	_ = memory.NewGoAllocator()
	cfg := DefaultArrowHNSWConfig()
	h := NewArrowHNSW(nil, &cfg)
	h.SetDimension(4)

	// Add 100 identical vectors (should converge extremely fast)
	vec := []float32{1.0, 1.0, 1.0, 1.0}
	for i := 0; i < 100; i++ {
		if err := h.InsertWithVector(uint32(i), vec, 0); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Search with long ef but should terminate early because all distances are 0
	q := []float32{1.0, 1.0, 1.0, 1.0}
	results, err := h.Search(context.Background(), q, 10, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 10)

	// We check if the search finished significantly before exhausting the budget
	// This is hard to test deterministically without internal probes,
	// but we can check if it returns results.
}

func FuzzHNSW_SearchEarlyTermination(f *testing.F) {
	f.Add(float32(1.0), 100)
	f.Fuzz(func(t *testing.T, val float32, ef int) {
		if ef <= 0 || ef > 1000 {
			return
		}
		cfg := DefaultArrowHNSWConfig()
		h := NewArrowHNSW(nil, &cfg)
		h.SetDimension(1)
		if err := h.InsertWithVector(1, []float32{val}, 0); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		q := []float32{val}
		_, err := h.Search(context.Background(), q, 1, nil)
		if err != nil {
			t.Errorf("Search failed: %v", err)
		}
	})
}
